/*-------------------------------------------------------------------------
 *
 * slot.c
 * 		Routines for orioledb tuple slot implementation
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "tableam/toast.h"
#include "tuple/toast.h"
#include "tuple/slot.h"

#include "access/detoast.h"
#include "access/toast_internals.h"
#include "catalog/heap.h"
#include "catalog/pg_type_d.h"
#include "storage/itemptr.h"
#include "utils/expandeddatum.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

#include "nodes/nodeFuncs.h"

static void tts_orioledb_init_reader(TupleTableSlot *slot);
static void tts_orioledb_get_index_values(TupleTableSlot *slot,
										  OIndexDescr *idx, Datum *values,
										  bool *isnull, bool leaf);

static void
tts_orioledb_init(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);
	oslot->descr = NULL;
	oslot->rowid = NULL;
	oslot->to_toast = NULL;
	oslot->version = 0;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;
}

static void
tts_orioledb_release(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	if (oslot->to_toast)
		pfree(oslot->to_toast);
}

/*
 * Optimized clear function.
 * Key optimizations:
 * 1. Combined checks to reduce branches
 * 2. Track whether we have any work to do before iterating
 */
static void
tts_orioledb_clear(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	/* Handle SHOULDFREE case - tuple data needs to be freed */
	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		if (!O_TUPLE_IS_NULL(oslot->tuple))
			pfree(oslot->tuple.data);
		if (oslot->data)
			pfree(oslot->data);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	/* Handle toast-related cleanup if we have toast arrays allocated */
	if (oslot->to_toast)
	{
		int			natts = slot->tts_tupleDescriptor->natts;
		bool	   *vfree = oslot->vfree;
		Datum	   *detoasted = oslot->detoasted;
		Datum	   *values = slot->tts_values;
		int			i;

		Assert(vfree);

		/*
		 * Optimize: only iterate if there's actually something to free.
		 * In many cases, vfree and detoasted arrays are all zeros.
		 */
		for (i = 0; i < natts; i++)
		{
			if (detoasted[i])
			{
				pfree(DatumGetPointer(detoasted[i]));
				detoasted[i] = (Datum) 0;
			}
			if (vfree[i])
			{
				pfree(DatumGetPointer(values[i]));
				vfree[i] = false;
			}
		}
		memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, natts * sizeof(char));
	}

	/* Reset slot state - these are cheap assignments */
	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);

	if (unlikely(oslot->rowid != NULL))
	{
		pfree(oslot->rowid);
		oslot->rowid = NULL;
	}

	oslot->descr = NULL;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static OTuple
tts_orioledb_make_key(TupleTableSlot *slot, OTableDescr *descr)
{
	OIndexDescr *id = GET_PRIMARY(descr);
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				ctid_off = id->primaryIsCtid ? 1 : 0;
	OTuple		result;

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = id->tableAttnums[i];

		if (attnum == 1 && ctid_off == 1)
		{
			key[i] = PointerGetDatum(&slot->tts_tid);
			isnull[i] = false;
		}
		else
		{
			int			attindex = attnum - 1 - ctid_off;
#ifdef USE_ASSERT_CHECKING
			/* PK attributes shouldn't be external or compressed */
			Form_pg_attribute att;

			att = TupleDescAttr(slot->tts_tupleDescriptor,
								attnum - 1 - ctid_off);
			if (!slot->tts_isnull[attindex] && att->attlen < 0)
			{
				Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
				Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
			}
#endif
			key[i] = slot->tts_values[attindex];
			isnull[i] = slot->tts_isnull[attindex];
		}
	}

	result = o_form_tuple(id->nonLeafTupdesc, &id->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull,
						  NULL);
	return result;
}

static OTuple
make_key_from_secondary_slot(TupleTableSlot *slot, OIndexDescr *idx, OTableDescr *descr)
{
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i;
	OTuple		result;

	for (i = 0; i < idx->nPrimaryFields; i++)
	{
		int			pk_attnum = idx->primaryFieldsAttnums[i];
		int			attindex = pk_attnum - 1;

#ifdef USE_ASSERT_CHECKING
		/* PK attributes shouldn't be external or compressed */
		Form_pg_attribute att;

		att = TupleDescAttr(slot->tts_tupleDescriptor, pk_attnum - 1);
		if (!slot->tts_isnull[attindex] && att->attlen < 0)
		{
			Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
			Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
		}
#endif
		key[i] = slot->tts_values[attindex];
		isnull[i] = slot->tts_isnull[attindex];
	}

	result = o_form_tuple(GET_PRIMARY(descr)->nonLeafTupdesc, &GET_PRIMARY(descr)->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull, NULL);
	return result;
}

static void
alloc_to_toast_vfree_detoasted(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	int			totalNatts = slot->tts_tupleDescriptor->natts;

	Assert(!oslot->to_toast && !oslot->vfree);
	oslot->to_toast = MemoryContextAllocZero(slot->tts_mcxt,
											 MAXALIGN(sizeof(bool) * totalNatts * 2) +
											 sizeof(Datum) * totalNatts);
	oslot->vfree = (bool *) (oslot->to_toast + totalNatts);
	oslot->detoasted = (Datum *) ((Pointer) oslot->to_toast + MAXALIGN(sizeof(char) * totalNatts + sizeof(bool) * totalNatts));
}

/*
 * Optimized helper: read and discard the next field.
 * Used for dropped/skipped attributes.
 */
static inline void
o_tuple_skip_next_field(OTupleReaderState *state)
{
	bool		dummy_null;

	(void) o_tuple_read_next_field(state, &dummy_null);
}

/*
 * Optimized getsomeattrs implementation.
 *
 * Key optimizations:
 * 1. Hoist loop-invariant conditions outside the loop
 * 2. Use specialized paths for common cases (primary index, no index_order)
 * 3. Reduce function call overhead with inlined helpers
 * 4. Cache frequently accessed values in local variables
 */
static void
tts_orioledb_getsomeattrs(TupleTableSlot *slot, int __natts)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	int			natts,
				attnum;
	OTableDescr *descr = oslot->descr;
	Datum	   *values = slot->tts_values;
	bool	   *isnull = slot->tts_isnull;
	bool		hastoast = false;
	OIndexDescr *idx;
	bool		index_order;
	int			ctid_off = 0;
	OIndexDescr *primary;
	bool		is_primary;
	bool		is_bridge;

	/* Fast path: already have enough attributes or tuple is null */
	if (likely(__natts <= slot->tts_nvalid) || O_TUPLE_IS_NULL(oslot->tuple))
		return;

	/* Cache commonly used values */
	Assert(descr);
	primary = GET_PRIMARY(descr);
	is_primary = (oslot->ixnum == PrimaryIndexNumber);
	is_bridge = (oslot->ixnum == BridgeIndexNumber);

	/* Get the appropriate index descriptor */
	if (is_bridge)
		idx = descr->bridge;
	else
		idx = descr->indices[oslot->ixnum];

	/* Determine if attributes should be fetched in index order */
	index_order = slot->tts_tupleDescriptor->tdtypeid == RECORDOID;
	if (is_primary)
		index_order = index_order &&
			slot->tts_tupleDescriptor->natts == idx->nFields;

	Assert(slot->tts_nvalid == 0 || is_primary);

	/* Calculate ctid offset once */
	if (is_primary)
	{
		if (primary->primaryIsCtid)
			ctid_off++;
		if (primary->bridging)
			ctid_off++;
	}

	/* Determine number of attributes to process */
	if (is_primary && oslot->leafTuple)
	{
		if (index_order)
			natts = descr->tupdesc->natts;
		else
			natts = Min(__natts, descr->tupdesc->natts);
	}
	else
	{
		natts = oslot->state.desc->natts;
	}

	/*
	 * FAST PATH: Primary index without index_order reordering.
	 * This is the most common case and we can optimize it significantly.
	 */
	if (is_primary && !index_order)
	{
		TupleDesc	tupdesc = slot->tts_tupleDescriptor;
		int			start_attnum = slot->tts_nvalid;

		for (attnum = start_attnum; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt = TupleDescAttr(tupdesc, attnum);

			/* Read the field directly to its destination */
			values[attnum] = o_tuple_read_next_field(&oslot->state,
													 &isnull[attnum]);

			/* Check for TOAST pointers in variable-length non-null attrs */
			if (!isnull[attnum] && thisatt->attlen < 0 && !thisatt->attbyval)
			{
				Pointer		p = DatumGetPointer(values[attnum]);

				Assert(p);
				if (IS_TOAST_POINTER(p) && !VARATT_IS_EXTERNAL_ORIOLEDB(p))
				{
					hastoast = true;
					natts = Max(natts, idx->maxTableAttnum - ctid_off);
				}
			}
		}
	}
	/*
	 * REGULAR PATH: Handles index_order, secondary indexes, bridge index
	 */
	else
	{
		int			cur_tbl_attnum = 0;
		TupleDesc	leaf_tupdesc = idx->leafTupdesc;

		for (attnum = slot->tts_nvalid; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt;
			int			res_attnum;

			/* Determine result attribute number */
			if (is_primary)
			{
				/* Primary index with index_order */
				if (cur_tbl_attnum >= idx->nFields ||
					attnum != idx->pk_tbl_field_map[cur_tbl_attnum].key)
				{
					res_attnum = -2;	/* dropped attribute */
				}
				else
				{
					res_attnum = idx->pk_tbl_field_map[cur_tbl_attnum].value;
					cur_tbl_attnum++;
				}
			}
			else if (index_order)
			{
				/* Secondary index */
				if (primary->primaryIsCtid && attnum == natts - 1)
					res_attnum = -1;	/* ctid attribute */
				else
					res_attnum = attnum;
			}
			else
			{
				Assert(false);
				res_attnum = attnum;	/* keep compiler happy */
			}

			/* Process based on attribute type */
			if (res_attnum >= 0)
			{
				/* Handle bridge_ctid special case */
				if (is_bridge && attnum == 0)
				{
					values[res_attnum] = PointerGetDatum(&oslot->bridge_ctid);
					isnull[res_attnum] = false;
					continue;
				}

				/* Read the field value */
				values[res_attnum] = o_tuple_read_next_field(&oslot->state,
															 &isnull[res_attnum]);

				/* Get attribute metadata */
				if (is_primary && !index_order)
					thisatt = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
				else
					thisatt = TupleDescAttr(leaf_tupdesc, attnum);

				/* Check for TOAST pointers */
				if (!isnull[res_attnum] && thisatt->attlen < 0 && !thisatt->attbyval)
				{
					Pointer		p = DatumGetPointer(values[res_attnum]);

					Assert(p);
					if (IS_TOAST_POINTER(p) && !VARATT_IS_EXTERNAL_ORIOLEDB(p))
					{
						hastoast = true;
						natts = Max(natts, idx->maxTableAttnum - ctid_off);
					}
				}
			}
			else if (res_attnum == -1)
			{
				/* ctid attribute - verify it matches */
				if (!idx->bridging)
				{
#ifdef USE_ASSERT_CHECKING
					Datum		iptr_value;
					bool		iptr_null;

					iptr_value = o_tuple_read_next_field(&oslot->state, &iptr_null);
					Assert(!iptr_null);
					Assert(memcmp(&slot->tts_tid,
								  (ItemPointer) iptr_value, sizeof(ItemPointerData)) == 0);
#else
					o_tuple_skip_next_field(&oslot->state);
#endif
				}
			}
			else	/* res_attnum == -2 */
			{
				/* Dropped attribute - skip it */
				o_tuple_skip_next_field(&oslot->state);
			}
		}
	}

	/* Process TOAST if needed */
	if (unlikely(hastoast))
	{
		OTuple		pkey;
		TupleDesc	tupdesc = slot->tts_tupleDescriptor;

		if (!oslot->to_toast)
			alloc_to_toast_vfree_detoasted(slot);

		/* Generate primary key for TOAST lookup */
		if (is_primary)
			pkey = tts_orioledb_make_key(slot, descr);
		else
			pkey = make_key_from_secondary_slot(slot, idx, descr);

		/* Process each potentially TOASTed attribute */
		for (attnum = 0; attnum < natts; attnum++)
		{
			Form_pg_attribute thisatt = TupleDescAttr(tupdesc, attnum);

			if (!isnull[attnum] && thisatt->attlen < 0 && !thisatt->attbyval)
			{
				Pointer		p = DatumGetPointer(values[attnum]);

				if (IS_TOAST_POINTER(p))
				{
					MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);
					OToastValue toastValue;

					memcpy(&toastValue, p, sizeof(toastValue));
					values[attnum] = create_o_toast_external(descr, pkey,
															 attnum + 1 + ctid_off,
															 &toastValue,
															 oslot->csn);
					oslot->vfree[attnum] = true;
					MemoryContextSwitchTo(mcxt);
				}
			}
		}

		/* Free primary key memory if not in bump context */
		if (!is_bump_memory_context(CurrentMemoryContext))
			pfree(pkey.data);
	}

	Assert(attnum == natts);
	slot->tts_nvalid = natts;
}
	}

	/* Ensure the number of processed attributes matches the expected count. */
	Assert(attnum == natts);

	/* Update the slot's valid attribute count. */
	slot->tts_nvalid = natts;
}

static Datum
tts_orioledb_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	const FormData_pg_attribute *att;

	ASAN_UNPOISON_MEMORY_REGION(isnull, sizeof(*isnull));

	if (attnum == RowIdAttributeNumber)
	{
		Datum		values[2 * INDEX_MAX_KEYS];
		bool		isnulls[2 * INDEX_MAX_KEYS];
		int			result_size,
					tuple_size;
		bytea	   *result;
		OTableDescr *descr = oslot->descr;
		OIndexDescr *primary;
		int			ctid_off;
		OTuple		tuple;
		ORowIdAddendumNonCtid addNonCtid;
		Pointer		ptr;

		if (oslot->rowid)
		{
			*isnull = false;
			return datumCopy(PointerGetDatum(oslot->rowid), false, -1);
		}

		if (!descr)
		{
			*isnull = true;
			return (Datum) 0;
		}

		primary = GET_PRIMARY(descr);
		ctid_off = primary->primaryIsCtid ? 1 : 0;

		if (primary->primaryIsCtid)
		{
			ORowIdAddendumCtid addCtid;

			addCtid.hint = oslot->hint;
			addCtid.csn = oslot->csn;
			addCtid.version = oslot->version;

			/* Ctid primary key: give hint + tid as rowid */
			result_size = MAXALIGN(VARHDRSZ) +
				MAXALIGN(sizeof(ORowIdAddendumCtid)) +
				MAXALIGN(sizeof(ItemPointerData));
			if (primary->bridging)
				result_size += MAXALIGN(sizeof(ItemPointerData));
			result = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
			SET_VARSIZE(result, result_size);
			ptr = (Pointer) result + MAXALIGN(VARHDRSZ);
			memcpy(ptr, &addCtid, sizeof(ORowIdAddendumCtid));
			ptr += MAXALIGN(sizeof(ORowIdAddendumCtid));
			memcpy(ptr, &slot->tts_tid, sizeof(ItemPointerData));
			if (primary->bridging)
			{
				ptr += MAXALIGN(sizeof(ItemPointerData));
				memcpy(ptr, &oslot->bridge_ctid, sizeof(ItemPointerData));
			}
			*isnull = false;
			oslot->rowid = result;
			return datumCopy(PointerGetDatum(result), false, -1);
		}

		/*
		 * General-case primary key: prepend tuple with maxaligned hint.
		 */
		result_size = MAXALIGN(VARHDRSZ) + MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		if (primary->bridging)
			result_size += MAXALIGN(sizeof(ItemPointerData));
		tts_orioledb_getsomeattrs(slot, primary->maxTableAttnum - ctid_off);
		tts_orioledb_get_index_values(slot, primary, values, isnulls, false);
		tuple_size = o_new_tuple_size(primary->nonLeafTupdesc,
									  &primary->nonLeafSpec,
									  NULL, NULL, oslot->version,
									  values, isnulls, NULL);
		result_size += MAXALIGN(tuple_size);
		result = (bytea *) MemoryContextAllocZero(slot->tts_mcxt, result_size);
		SET_VARSIZE(result, result_size);
		ptr = (Pointer) result + MAXALIGN(VARHDRSZ);
		if (primary->bridging)
			memcpy(ptr + MAXALIGN(sizeof(ORowIdAddendumNonCtid)), &oslot->bridge_ctid, sizeof(ItemPointerData));

		tuple.data = ptr + MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		if (primary->bridging)
			tuple.data += MAXALIGN(sizeof(ItemPointerData));
		o_tuple_fill(primary->nonLeafTupdesc, &primary->nonLeafSpec,
					 &tuple, tuple_size, NULL, NULL, oslot->version, values, isnulls, NULL);

		addNonCtid.csn = oslot->csn;
		addNonCtid.flags = tuple.formatFlags;
		addNonCtid.hint = oslot->hint;

		memcpy(ptr, &addNonCtid, sizeof(ORowIdAddendumNonCtid));

		*isnull = false;
		oslot->rowid = result;
		return datumCopy(PointerGetDatum(result), false, -1);
	}

	att = SystemAttributeDefinition(attnum);
	elog(ERROR, "orioledb tuples does not have system attribute: %s",
		 att->attname.data);

	return 0;					/* silence compiler warnings */
}

/*
 * Optimized materialize implementation.
 *
 * Key optimizations:
 * 1. Single loop that calculates size and identifies which attrs need copying
 * 2. Cache attribute pointers to avoid redundant TupleDescAttr calls
 * 3. Use local variables to reduce dereferencing
 */
static void
tts_orioledb_materialize(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	int			natts = desc->natts;
	Size		sz = 0;
	char	   *data;
	Datum	   *values = slot->tts_values;
	bool	   *isnull_arr = slot->tts_isnull;
	int			natt;
	bool		has_expanded = false;

	/* Fast path: already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	slot_getallattrs(slot);

	/*
	 * First pass: compute total size needed.
	 * Track if we have any expanded objects to handle.
	 */
	for (natt = 0; natt < natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);

		if (att->attbyval || isnull_arr[natt])
			continue;

		if (att->attlen == -1)
		{
			Datum		val = values[natt];

			if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
			{
				has_expanded = true;
				sz = att_align_nominal(sz, att->attalign);
				sz += EOH_get_flat_size(DatumGetEOHP(val));
			}
			else
			{
				sz = att_align_nominal(sz, att->attalign);
				sz = att_addlength_datum(sz, -1, val);
			}
		}
		else
		{
			/* Fixed-length pass-by-ref: -2 (cstring) or positive length */
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, values[natt]);
		}
	}

	/* All data is byval - nothing to materialize */
	if (sz == 0)
		return;

	/* Allocate memory for all non-byval attributes */
	oslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/*
	 * Second pass: copy all non-byval attributes.
	 * Use optimized path when no expanded objects exist.
	 */
	if (likely(!has_expanded))
	{
		/* Fast path: no expanded objects to flatten */
		for (natt = 0; natt < natts; natt++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, natt);
			Size		data_length;

			if (att->attbyval || isnull_arr[natt])
				continue;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(0, att->attlen, values[natt]);
			memcpy(data, DatumGetPointer(values[natt]), data_length);
			values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
	else
	{
		/* Slow path: handle expanded objects */
		for (natt = 0; natt < natts; natt++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, natt);
			Datum		val;

			if (att->attbyval || isnull_arr[natt])
				continue;

			val = values[natt];

			if (att->attlen == -1 &&
				VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
			{
				ExpandedObjectHeader *eoh = DatumGetEOHP(val);
				Size		data_length;

				data = (char *) att_align_nominal(data, att->attalign);
				data_length = EOH_get_flat_size(eoh);
				EOH_flatten_into(eoh, data, data_length);
				values[natt] = PointerGetDatum(data);
				data += data_length;
			}
			else
			{
				Size		data_length;

				data = (char *) att_align_nominal(data, att->attalign);
				data_length = att_addlength_datum(0, att->attlen, val);
				memcpy(data, DatumGetPointer(val), data_length);
				values[natt] = PointerGetDatum(data);
				data += data_length;
			}
		}
	}

	/* Clear toast tracking arrays if allocated */
	if (oslot->to_toast)
	{
		memset(oslot->vfree, 0, natts * sizeof(bool));
		memset(oslot->to_toast, 0, natts * sizeof(char));
	}
}

void
tts_orioledb_detoast(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	int			natts = tupleDesc->natts;
	int			i;

	slot_getallattrs(slot);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		Datum		tmp;

		if (!slot->tts_isnull[i] && att->attlen == -1 &&
			VARATT_IS_EXTENDED(slot->tts_values[i]))
		{
			MemoryContext mctx;

			if (!oslot->vfree)
				alloc_to_toast_vfree_detoasted(slot);

			mctx = MemoryContextSwitchTo(slot->tts_mcxt);
			tmp = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
			MemoryContextSwitchTo(mctx);
			Assert(slot->tts_values[i] != tmp);
			if (oslot->vfree[i])
				pfree(DatumGetPointer(slot->tts_values[i]));
			slot->tts_values[i] = tmp;
			oslot->vfree[i] = true;
		}
	}
}

static void
tts_orioledb_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;
	OTableSlot *dstoslot = (OTableSlot *) dstslot;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);
	tts_orioledb_clear(dstslot);

	if (srcslot->tts_ops == &TTSOpsOrioleDB &&
		((OTableSlot *) srcslot)->descr == dstoslot->descr)
	{
		OTableSlot *srcoslot = (OTableSlot *) srcslot;

		dstoslot->version = srcoslot->version;
		if (!O_TUPLE_IS_NULL(srcoslot->tuple))
		{
			MemoryContext mctx = MemoryContextSwitchTo(dstslot->tts_mcxt);
			OTuple		tup = srcoslot->tuple;
			uint32		tupLen = o_tuple_size(tup, &GET_PRIMARY(srcoslot->descr)->leafSpec);

			dstoslot->tuple.data = (Pointer) palloc(tupLen);
			memcpy(dstoslot->tuple.data, srcoslot->tuple.data, tupLen);
			dstoslot->tuple.formatFlags = srcoslot->tuple.formatFlags;
			dstoslot->descr = srcoslot->descr;
			if (srcoslot->rowid)
			{
				dstoslot->rowid = (bytea *) palloc(VARSIZE_ANY(srcoslot->rowid));
				memcpy(dstoslot->rowid, srcoslot->rowid,
					   VARSIZE_ANY(srcoslot->rowid));
			}
			MemoryContextSwitchTo(mctx);
			dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
			dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;
			dstslot->tts_nvalid = 0;
			dstoslot->csn = srcoslot->csn;
			dstoslot->ixnum = srcoslot->ixnum;
			dstoslot->leafTuple = srcoslot->leafTuple;
			tts_orioledb_init_reader(dstslot);
			return;
		}
	}

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_orioledb_materialize(dstslot);
}

static HeapTuple
tts_orioledb_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	result;

	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	result = heap_form_tuple(slot->tts_tupleDescriptor,
							 slot->tts_values,
							 slot->tts_isnull);

	ItemPointerCopy(&slot->tts_tid, &result->t_self);

	return result;
}

static MinimalTuple
tts_orioledb_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}

static void
tts_orioledb_init_reader(TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OIndexDescr *idx;

	if (oslot->ixnum == BridgeIndexNumber)
		idx = oslot->descr->bridge;
	else
		idx = oslot->descr->indices[oslot->ixnum];

	if (oslot->leafTuple)
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->leafTupdesc, &idx->leafSpec);
	else
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->nonLeafTupdesc, &idx->nonLeafSpec);

	if (idx->primaryIsCtid)
	{
		if (oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		{
			Datum		value;
			bool		isnull;

			value = o_tuple_read_next_field(&oslot->state, &isnull);
			slot->tts_tid = *((ItemPointer) value);
		}
		else if (!(idx->bridging &&
				   (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber)))
		{
			ItemPointer iptr;
			bool		isnull;

			if (oslot->leafTuple)
				iptr = o_tuple_get_last_iptr(idx->leafTupdesc, &idx->leafSpec,
											 oslot->tuple, &isnull);
			else
				iptr = o_tuple_get_last_iptr(idx->nonLeafTupdesc,
											 &idx->nonLeafSpec,
											 oslot->tuple, &isnull);
			Assert(!isnull && iptr);
			slot->tts_tid = *iptr;
		}
	}

	if (idx->bridging && (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber))
	{
		Datum		value;
		bool		isnull;

		value = o_tuple_read_next_field(&oslot->state, &isnull);
		oslot->bridge_ctid = *((ItemPointer) value);
	}

	slot->tts_tableOid = oslot->descr->oids.reloid;
}

static void
tts_orioledb_store_tuple_internal(TupleTableSlot *slot, OTuple tuple,
								  OTableDescr *descr, CommitSeqNo csn,
								  int ixnum, bool leafTuple, bool shouldfree,
								  BTreeLocationHint *hint)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	Assert(COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_INPROGRESS(csn));
	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	tts_orioledb_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;

	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->csn = csn;
	oslot->ixnum = ixnum;
	oslot->leafTuple = leafTuple;
	oslot->version = o_tuple_get_version(tuple);

	if (hint)
		oslot->hint = *hint;

	tts_orioledb_init_reader(slot);

	if (shouldfree)
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;
}

void
tts_orioledb_store_tuple(TupleTableSlot *slot, OTuple tuple,
						 OTableDescr *descr, CommitSeqNo csn,
						 int ixnum, bool shouldfree, BTreeLocationHint *hint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, true,
									  shouldfree, hint);
}

void
tts_orioledb_store_non_leaf_tuple(TupleTableSlot *slot, OTuple tuple,
								  OTableDescr *descr, CommitSeqNo csn,
								  int ixnum, bool shouldfree,
								  BTreeLocationHint *hint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, false,
									  shouldfree, hint);
}

/*
 * Optimized get_tbl_att function.
 * Key optimizations:
 * 1. Reordered branches to favor most common case
 * 2. Reduced redundant checks with combined conditions
 */
static inline Datum
get_tbl_att(TupleTableSlot *slot, int attnum, bool primaryIsCtid,
			bool *isnull, Oid *typid)
{
	OTableSlot *oSlot = (OTableSlot *) slot;
	int			i;
	Datum		value;
	Form_pg_attribute att;

	/* Handle special attribute numbers first */
	if (attnum == -1)
	{
		/* bridge_ctid - same handling regardless of primaryIsCtid */
		*isnull = false;
		if (typid)
			*typid = TIDOID;
		return PointerGetDatum(&oSlot->bridge_ctid);
	}

	if (primaryIsCtid)
	{
		if (attnum == 1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&slot->tts_tid);
		}
		i = attnum - 2;
	}
	else
	{
		i = attnum - 1;
	}

	/* Regular attribute access */
	att = TupleDescAttr(slot->tts_tupleDescriptor, i);
	*isnull = slot->tts_isnull[i];

	if (typid)
		*typid = att->atttypid;

	/* Fast path: null value or not extended */
	if (*isnull)
		return (Datum) 0;

	value = slot->tts_values[i];

	/* Check if detoasting is needed */
	if (att->attlen < 0 && VARATT_IS_EXTENDED(value))
	{
		if (!oSlot->to_toast)
			alloc_to_toast_vfree_detoasted(&oSlot->base);

		if (!oSlot->detoasted[i])
		{
			MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);

			oSlot->detoasted[i] = PointerGetDatum(PG_DETOAST_DATUM(value));
			MemoryContextSwitchTo(mcxt);
		}
		value = oSlot->detoasted[i];
	}
	return value;
}

static Datum
get_idx_expr_att(TupleTableSlot *slot, OIndexDescr *idx,
				 ExprState *exp_state, bool *isnull)
{
	Datum		result;

	idx->econtext->ecxt_scantuple = slot;

	result = ExecEvalExprSwitchContext(exp_state,
									   idx->econtext, isnull);
	return result;
}

/*
 * Prepares values for index tuple.  Works for leaf and non-leaf tuples of
 * secondary index and non-leaf tuple of primary index.
 *
 * Detoasts all the values and marks detoasted values in 'detoasted' array.
 * If 'detoasted' array isn't given, asserts not values are toasted.
 */
static void
tts_orioledb_get_index_values(TupleTableSlot *slot, OIndexDescr *idx,
							  Datum *values, bool *isnull, bool leaf)
{
	TupleDesc	tupleDesc = leaf ? idx->leafTupdesc : idx->nonLeafTupdesc;
	int			natts = tupleDesc->natts;
	int			i;
	ListCell   *indexpr_item = list_head(idx->expressions_state);

	Assert(natts <= 2 * INDEX_MAX_KEYS);

	for (i = 0; i < natts; i++)
	{
		int			attnum = idx->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			values[i] = get_tbl_att(slot, attnum, idx->primaryIsCtid,
									&isnull[i], NULL);
		else
		{
			values[i] = get_idx_expr_att(slot, idx,
										 (ExprState *) lfirst(indexpr_item),
										 &isnull[i]);
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}
	}
}

OTuple
tts_orioledb_make_secondary_tuple(TupleTableSlot *slot, OIndexDescr *idx, bool leaf)
{
	Datum		values[2 * INDEX_MAX_KEYS];
	bool		isnull[2 * INDEX_MAX_KEYS];
	TupleDesc	tupleDesc;
	OTupleFixedFormatSpec *spec;
	int			ctid_off = idx->primaryIsCtid ? 1 : 0;
	OTableSlot *oslot = (OTableSlot *) slot;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	tts_orioledb_get_index_values(slot, idx, values, isnull, leaf);

	if (leaf)
	{
		tupleDesc = idx->leafTupdesc;
		spec = &idx->leafSpec;
	}
	else
	{
		tupleDesc = idx->nonLeafTupdesc;
		spec = &idx->nonLeafSpec;
	}

	if (leaf && idx->bridging && idx->desc.type == oIndexBridge)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = false;
		bridge_data.attnum = 1;
		bridge_data_arg = &bridge_data;
	}

	return o_form_tuple(tupleDesc, spec, 0, values, isnull, bridge_data_arg);
}

/* fills key bound from tuple or index tuple that belongs to current BTree */
void
tts_orioledb_fill_key_bound(TupleTableSlot *slot, OIndexDescr *idx,
							OBTreeKeyBound *bound)
{
	int			i;
	int			ctid_off = idx->primaryIsCtid ? 1 : 0;
	ListCell   *indexpr_item = list_head(idx->expressions_state);

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	bound->nkeys = idx->nonLeafTupdesc->natts;
	for (i = 0; i < bound->nkeys; i++)
	{
		Datum		value;
		bool		isnull;
		int			attnum;
		Oid			typid;

		attnum = idx->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			value = get_tbl_att(slot, attnum, idx->primaryIsCtid,
								&isnull, &typid);
		else
		{
			value = get_idx_expr_att(slot, idx,
									 (ExprState *) lfirst(indexpr_item),
									 &isnull);
			typid = idx->nonLeafTupdesc->attrs[i].atttypid;
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}

		bound->keys[i].value = value;
		bound->keys[i].type = typid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = idx->fields[i].comparator;
	}
}

/*
 * Appends index key stored in the tuple slot to the given string.
 */
void
appendStringInfoIndexKey(StringInfo str, TupleTableSlot *slot, OIndexDescr *id)
{
	int			i;
	ListCell   *indexpr_item = list_head(id->expressions_state);

	slot_getallattrs(slot);

	appendStringInfo(str, "(");
	for (i = 0; i < id->nUniqueFields; i++)
	{
		Datum		value;
		bool		isnull;
		int			attnum = id->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			value = get_tbl_att(slot, attnum, id->primaryIsCtid,
								&isnull, NULL);
		else
		{
			value = get_idx_expr_att(slot, id,
									 (ExprState *) lfirst(indexpr_item),
									 &isnull);
			indexpr_item = lnext(id->expressions_state, indexpr_item);
		}

		if (i != 0)
			appendStringInfo(str, ", ");
		if (isnull)
			appendStringInfo(str, "null");
		else
		{
			Oid			typoutput;
			bool		typisvarlena;
			char	   *res;

			getTypeOutputInfo(id->nonLeafTupdesc->attrs[i].atttypid,
							  &typoutput, &typisvarlena);
			res = OidOutputFunctionCall(typoutput, value);
			appendStringInfo(str, "'%s'", res);
		}
	}
	appendStringInfo(str, ")");
}

/*
 * Returns a string representation of the index key that is stored in the
 * tuple slot.
 */
char *
tss_orioledb_print_idx_key(TupleTableSlot *slot, OIndexDescr *id)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoIndexKey(&buf, slot, id);

	return buf.data;
}

/*
 * Returns the expected length of the tuple that will be stored in the primary
 * key index.
 */
static inline int
expected_tuple_len(TupleTableSlot *slot, OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OIndexDescr *idx = GET_PRIMARY(descr);
	int			tup_size;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}
	tup_size = o_new_tuple_size(idx->leafTupdesc,
								&idx->leafSpec,
								idx->primaryIsCtid ? &slot->tts_tid : NULL,
								bridge_data_arg,
								oslot->version,
								slot->tts_values,
								slot->tts_isnull,
								oslot->to_toast);

	return tup_size;
}

/*
 * Returns true if the tuple stored in the slot fits the maximum size to be
 * stored in the index.
 */
static inline bool
can_be_stored_in_index(TupleTableSlot *slot, OTableDescr *descr)
{
	int			tup_size = expected_tuple_len(slot, descr);

	Assert(tup_size > 0);

	if (tup_size <= O_BTREE_MAX_TUPLE_SIZE)
		return true;
	return false;
}

/*
 * Apply TOAST including compression and out-of-line storage to the tuple
 * stored in the slot if necessary.
 */
void
tts_orioledb_toast(TupleTableSlot *slot, OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	Form_pg_attribute att;
	int			i,
				full_size = 0,
				to_toastn,
				natts;
	AttrNumber	toast_attn;
	bool		has_toasted = false;
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	bool		primaryIsCtid;
	int			ctid_off;

	primaryIsCtid = GET_PRIMARY(descr)->primaryIsCtid;
	ctid_off = primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	slot_getallattrs(slot);

	/* temporary, pointers to TupleDesc attributes */
	natts = tupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		att = TupleDescAttr(tupdesc, i);
		if (att->attlen <= 0 && !slot->tts_isnull[i]
			&& (VARATT_IS_EXTERNAL_ONDISK(slot->tts_values[i]) ||
				VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[i])))
			has_toasted = true;
	}

	if (!has_toasted)
		full_size = expected_tuple_len(slot, descr);

	/* we do not need use TOAST */
	if (full_size <= O_BTREE_MAX_TUPLE_SIZE && !has_toasted)
	{
		return;
	}

	/* if we there than tuple's values should be TOASTed or compressed */
	if (!oslot->to_toast)
		alloc_to_toast_vfree_detoasted(slot);

	full_size = 0;
	for (i = 0; i < descr->ntoastable; i++)
		oslot->to_toast[descr->toastable[i] - ctid_off] = ORIOLEDB_TO_TOAST_ON;

	full_size = expected_tuple_len(slot, descr);

	memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, sizeof(bool) * natts);

	/* if we can not compress tuple, we do not try do it */
	if (full_size > O_BTREE_MAX_TUPLE_SIZE)
	{
		return;
	}

	/*
	 * If we there than we must calculate which values should be compressed or
	 * TOASTed.
	 */
	to_toastn = 0;
	/* to make it easy now all values must be reTOASTed */
	for (i = 0; i < descr->ntoastable; i++)
	{
		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		if (VARATT_IS_EXTERNAL_ONDISK(slot->tts_values[toast_attn]) ||
			VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[toast_attn]))
		{
			oslot->to_toast[toast_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
	}

	while (to_toastn < descr->ntoastable &&
		   !can_be_stored_in_index(slot, descr))
	{
		Datum		tmp;
		int			max = 0,
					max_attn = -1,
					var_size;
		MemoryContext oldMctx;

		/* search max unprocessed value */
		for (i = 0; i < descr->ntoastable; i++)
		{
			toast_attn = descr->toastable[i] - ctid_off;
			if (!slot->tts_isnull[toast_attn] &&
				oslot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_OFF)
			{
				att = TupleDescAttr(tupdesc, toast_attn);

				Assert(att->attstorage != TYPSTORAGE_PLAIN);

				if (att->attstorage == TYPSTORAGE_MAIN &&
					VARATT_IS_COMPRESSED(slot->tts_values[toast_attn]))
					continue;

				var_size = VARSIZE_ANY(slot->tts_values[toast_attn]);
				if (var_size > max)
				{
					max = var_size;
					max_attn = toast_attn;
				}
			}
			/* else we already process it or it is NULL */
		}

		/* we have no values which can be toasted */
		if (max_attn == -1)
			break;

		att = TupleDescAttr(tupdesc, max_attn);

		/*
		 * If the value is already compressed or can not be compressed - it
		 * must be toasted
		 */
		if (VARATT_IS_COMPRESSED(slot->tts_values[max_attn])
			|| att->attstorage == TYPSTORAGE_EXTERNAL)
		{
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
			continue;
		}

		oldMctx = MemoryContextSwitchTo(slot->tts_mcxt);
		tmp = toast_compress_datum(slot->tts_values[max_attn],
								   TOAST_PGLZ_COMPRESSION);
		MemoryContextSwitchTo(oldMctx);

		if (DatumGetPointer(tmp) != NULL)
		{
			/* Suceessfully compressed, replace the value */

			/* free the old value */
			if (oslot->vfree[max_attn])
				pfree(DatumGetPointer(slot->tts_values[max_attn]));
			/* store the new value and mark to free it later */
			slot->tts_values[max_attn] = tmp;
			oslot->vfree[max_attn] = true;
		}
		else if (att->attstorage != TYPSTORAGE_MAIN)
		{
			/* Compression failed, try to TOAST it */
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
		else
		{
			/* Compression failed, but we can not TOAST it */
			Assert(att->attstorage == TYPSTORAGE_MAIN);
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_COMPRESSION_TRIED;
			to_toastn++;
		}
	}
}

OTuple
tts_orioledb_form_tuple(TupleTableSlot *slot,
						OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OTuple		tuple;			/* return tuple */
	Size		len;
	OIndexDescr *idx = GET_PRIMARY(descr);
	TupleDesc	tupleDescriptor = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	bool		primaryIsCtid = idx->primaryIsCtid;
	ItemPointer iptr;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (!O_TUPLE_IS_NULL(oslot->tuple) && oslot->descr == descr &&
		oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		return oslot->tuple;

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging && (idx->desc.type == oIndexPrimary || idx->desc.type == oIndexBridge))
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = idx->desc.type == oIndexPrimary;
		bridge_data.attnum = idx->desc.type == oIndexBridge ? 1 : idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec, iptr, bridge_data_arg,
						   0, slot->tts_values, slot->tts_isnull,
						   oslot->to_toast);

	tuple.data = (Pointer) MemoryContextAllocZero(slot->tts_mcxt, len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, 0,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->ixnum = PrimaryIndexNumber;
	oslot->leafTuple = true;
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	tts_orioledb_init_reader(slot);

	return tuple;
}

OTuple
tts_orioledb_form_orphan_tuple(TupleTableSlot *slot,
							   OTableDescr *descr)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	OTuple		tuple;
	Size		len;
	OIndexDescr *idx = GET_PRIMARY(descr);
	TupleDesc	tupleDescriptor = idx->leafTupdesc;
	OTupleFixedFormatSpec *spec = &idx->leafSpec;
	bool		primaryIsCtid = idx->primaryIsCtid;
	ItemPointer iptr;
	BrigeData	bridge_data;
	BrigeData  *bridge_data_arg = NULL;

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec,
						   iptr, bridge_data_arg, oslot->version,
						   slot->tts_values, slot->tts_isnull, oslot->to_toast);

	tuple.data = (Pointer) palloc0(len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, oslot->version,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	return tuple;
}

bool
tts_orioledb_insert_toast_values(TupleTableSlot *slot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	OTuple		idx_tup;
	int			i;
	bool		result = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	if (oslot->to_toast == NULL)
		return true;

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (oslot->to_toast[i])
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_insert(GET_PRIMARY(descr), descr->toast,
									idx_tup, i + 1 + ctid_off, p,
									toast_datum_size(value), oxid, csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}
	pfree(idx_tup.data);
	return result;
}

void
tts_orioledb_toast_sort_add(TupleTableSlot *slot,
							OTableDescr *descr,
							Tuplesortstate *sortstate)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	OTuple		idx_tup;
	int			i;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	if (oslot->to_toast == NULL)
		return;

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (oslot->to_toast[i])
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			o_toast_sort_add(GET_PRIMARY(descr), descr->toast,
							 idx_tup, i + 1 + ctid_off, p,
							 toast_datum_size(value), sortstate);
			if (free)
				pfree(p);
		}
	}
	pfree(idx_tup.data);
}

bool
tts_orioledb_remove_toast_values(TupleTableSlot *slot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	int			i;
	bool		result = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	slot_getallattrs(slot);

	for (i = 0; i < descr->ntoastable; i++)
	{
		int			toast_attn;
		Datum		value;

		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		value = slot->tts_values[toast_attn];
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			OToastExternal ote;
			OFixedKey	key;

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(value)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(value)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);
			o_btree_load_shmem(&descr->toast->desc);

			result = o_toast_delete(GET_PRIMARY(descr),
									descr->toast,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}
	}
	return result;
}

bool
tts_orioledb_update_toast_values(TupleTableSlot *oldSlot,
								 TupleTableSlot *newSlot,
								 OTableDescr *descr,
								 OXid oxid, CommitSeqNo csn)
{
	OTableSlot *newOSlot = (OTableSlot *) newSlot;
	OTuple		idx_tup;
	OTuple		old_idx_tup PG_USED_FOR_ASSERTS_ONLY;
	int			i;
	bool		result = true;
	OIndexDescr *primary = GET_PRIMARY(descr);
	int			ctid_off = primary->primaryIsCtid ? 1 : 0;

	if (descr->bridge)
		ctid_off++;

	slot_getallattrs(oldSlot);

	idx_tup = tts_orioledb_make_key(newSlot, descr);

#ifdef USE_ASSERT_CHECKING
	{
		int			natts;

		old_idx_tup = tts_orioledb_make_key(oldSlot, descr);
		o_tuple_set_version(&primary->nonLeafSpec, &old_idx_tup,
							o_tuple_get_version(idx_tup));
		/* old_idx_tup and idx_tup are equal */
		Assert(o_tuple_size(old_idx_tup, &primary->nonLeafSpec) ==
			   o_tuple_size(idx_tup, &primary->nonLeafSpec));
		Assert(old_idx_tup.formatFlags == idx_tup.formatFlags);

		/*
		 * Cannot use simple memcmp(old_idx_tup.data, idx_tup.data, ...)
		 * because of included fields and also equality of such special values
		 * as '0.0' and '-0.0' for float
		 */
		if (old_idx_tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
			natts = primary->nonLeafSpec.natts;
		else
			natts = primary->nonLeafTupdesc->natts;
		for (i = 0; i < natts; i++)
		{
			if (!OIgnoreColumn(primary, i))
			{
				Datum		old_value;
				Datum		new_value;
				bool		isnull;
				OIndexField *pkfield = &primary->fields[i];
				int			cmp;

				old_value = o_fastgetattr(old_idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);
				new_value = o_fastgetattr(idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);

				cmp = o_call_comparator(pkfield->comparator,
										old_value, new_value);
				Assert(cmp == 0);
			}
		}
		pfree(old_idx_tup.data);
	}
#endif

	for (i = 0; i < descr->ntoastable; i++)
	{
		int			toast_attn;
		Datum		oldValue = 0,
					newValue = 0;
		bool		newToast = false,
					oldToast = false;
		bool		insertNew = false;
		bool		deleteOld = false;

		toast_attn = descr->toastable[i] - ctid_off;
		if (!oldSlot->tts_isnull[toast_attn])
		{
			oldValue = oldSlot->tts_values[toast_attn];
			if (VARATT_IS_EXTERNAL_ORIOLEDB(oldValue))
				oldToast = true;
		}

		if (newOSlot->to_toast && newOSlot->to_toast[toast_attn])
		{
			newToast = true;
			newValue = newSlot->tts_values[toast_attn];
		}

		if (!newToast && !oldToast)
			continue;

		if (newToast && !oldToast)
		{
			insertNew = true;
		}
		else if (!newToast && oldToast)
		{
			deleteOld = true;
		}
		else if (o_toast_equal(&GET_PRIMARY(descr)->desc,
							   newValue,
							   oldValue))
		{
			/* if it is the same toast value than nothing to do */
			continue;
		}
		else
		{
			/* update value if it does not equal */
			bool		equal;
			int			rawSize;

			rawSize = o_get_raw_size(newValue);
			equal = (rawSize == o_get_raw_size(oldValue));
			if (equal)
			{
				Datum		newRawValue;
				Datum		oldRawValue;
				Pointer		newPtr;
				Pointer		oldPtr;
				bool		freeNew;
				bool		freeOld;

				newRawValue = o_get_raw_value(newValue, &freeNew);
				oldRawValue = o_get_raw_value(oldValue, &freeOld);
				newPtr = DatumGetPointer(newRawValue);
				oldPtr = DatumGetPointer(oldRawValue);

				Assert(VARSIZE_ANY_EXHDR(newPtr) == VARSIZE_ANY_EXHDR(oldPtr));
				Assert(VARSIZE_ANY_EXHDR(newPtr) == rawSize);
				equal = memcmp(VARDATA_ANY(oldPtr),
							   VARDATA_ANY(newPtr),
							   rawSize) == 0;
				if (freeNew)
					pfree(newPtr);
				if (freeOld)
					pfree(oldPtr);

				if (equal)
					continue;
			}

			insertNew = true;
			deleteOld = true;
		}

		if (deleteOld)
		{
			OToastExternal ote;
			OFixedKey	key;

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(oldValue)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(oldValue)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);
			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_delete(GET_PRIMARY(descr),
									descr->toast,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}

		if (insertNew)
		{
			Datum		value;
			Pointer		p;
			bool		free;

			value = o_get_src_value(newValue, &free);
			p = DatumGetPointer(value);

			o_btree_load_shmem(&descr->toast->desc);
			result = o_toast_insert(GET_PRIMARY(descr),
									descr->toast,
									idx_tup,
									toast_attn + 1 + ctid_off,
									p,
									toast_datum_size(value),
									oxid,
									csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}

	pfree(idx_tup.data);
	return result;
}

/*
 * tts_orioledb_modified - Check if specified attributes were modified between two tuples
 *
 * Optimized to:
 * 1. Cache slot values/isnull arrays
 * 2. Avoid redundant attribute lookups
 * 3. Use likely/unlikely hints for branch prediction
 */
bool
tts_orioledb_modified(TupleTableSlot *oldSlot,
					  TupleTableSlot *newSlot,
					  Bitmapset *attrs)
{
	TupleDesc	tupdesc = oldSlot->tts_tupleDescriptor;
	Datum	   *old_values = oldSlot->tts_values;
	Datum	   *new_values = newSlot->tts_values;
	bool	   *old_isnull = oldSlot->tts_isnull;
	bool	   *new_isnull = newSlot->tts_isnull;
	int			attnum,
				maxAttr;

	maxAttr = bms_prev_member(attrs, -1) + FirstLowInvalidHeapAttributeNumber - 1;

	if (maxAttr < 0)
		return false;

	slot_getsomeattrs(oldSlot, maxAttr + 1);
	slot_getsomeattrs(newSlot, maxAttr + 1);

	attnum = -1;
	while ((attnum = bms_next_member(attrs, attnum)) >= 0)
	{
		int			i = attnum + FirstLowInvalidHeapAttributeNumber - 1;

		if (unlikely(i < 0))
			elog(ERROR, "invalid attribute number %d", i);

		/* Fast null comparison */
		if (old_isnull[i] != new_isnull[i])
			return true;

		/* Both null - no modification */
		if (old_isnull[i])
			continue;

		/* Compare actual values */
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (!datumIsEqual(old_values[i], new_values[i],
							  att->attbyval, att->attlen))
				return true;
		}
	}
	return false;
}

void
tts_orioledb_set_ctid(TupleTableSlot *slot, ItemPointer iptr)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	slot->tts_tid = *iptr;
	if (!O_TUPLE_IS_NULL(oslot->tuple) &&
		oslot->ixnum == PrimaryIndexNumber &&
		oslot->leafTuple)
		o_tuple_set_ctid(oslot->tuple, iptr);
}

const TupleTableSlotOps TTSOpsOrioleDB = {
	.base_slot_size = sizeof(OTableSlot),
	.init = tts_orioledb_init,
	.release = tts_orioledb_release,
	.clear = tts_orioledb_clear,
	.getsomeattrs = tts_orioledb_getsomeattrs,
	.getsysattr = tts_orioledb_getsysattr,
	.materialize = tts_orioledb_materialize,
	.copyslot = tts_orioledb_copyslot,

	/*
	 * A virtual tuple table slot can not "own" a heap tuple or a minimal
	 * tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_orioledb_copy_heap_tuple,
	.copy_minimal_tuple = tts_orioledb_copy_minimal_tuple
};
