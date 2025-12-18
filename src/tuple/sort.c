/*-------------------------------------------------------------------------
 *
 * sort.c
 * 		Implementation of orioledb tuple sorting
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/sort.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "tableam/descr.h"
#include "tuple/format.h"
#include "tuple/sort.h"
#include "tuple/toast.h"

#include "catalog/pg_collation_d.h"
#include "catalog/pg_opclass_d.h"
#include "utils/tuplesort.h"

typedef struct
{
	TupleDesc	tupDesc;
	OIndexDescr *id;
	bool		enforceUnique;
} OIndexBuildSortArg;

typedef struct
{
	TupleDesc	newTupDesc;
	TupleDesc	oldTupDesc;
	OIndexDescr *newIdx;
	OIndexDescr *oldIdx;
	bool		enforceUnique;
} OIndexRebuildPkSortArg;

static void
write_o_tuple(void *ptr, OTuple tup, int tupsize)
{
	Pointer		p = (Pointer) ptr;

	*((uint8 *) p) = tup.formatFlags;
	p += MAXIMUM_ALIGNOF;
	memcpy(p, tup.data, tupsize);
}

static OTuple
read_o_tuple(void *ptr)
{
	OTuple		tup;
	Pointer		p = (Pointer) ptr;

	tup.formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	tup.data = p;

	return tup;
}

static int
comparetup_orioledb_index(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	SortSupport sortKey = base->sortKeys;
	OTuple		ltup;
	OTuple		rtup;
	TupleDesc	tupDesc;
	bool		equal_hasnull = false;
	int			nkey;
	int32		compare;
	AttrNumber	attno;
	Datum		datum1,
				datum2;
	bool		isnull1,
				isnull2;
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;

	/* Compare the leading sort key */
	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		return compare;

	/* Compare additional sort keys */
	ltup = read_o_tuple(a->tuple);
	rtup = read_o_tuple(b->tuple);
	tupDesc = arg->tupDesc;

	if (sortKey->abbrev_converter)
	{
	attno = sortKey->ssup_attno;

	datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
	datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

	compare = ApplySortAbbrevFullComparator(datum1, isnull1,
											datum2, isnull2,
											sortKey);
	if (compare != 0)
			return compare;
	}

	/* they are equal, so we only need to examine one null flag */
	if (a->isnull1)
		equal_hasnull = true;

	sortKey++;
	for (nkey = 1; nkey < base->nKeys; nkey++, sortKey++)
	{
		if (!OIgnoreColumn(arg->id, nkey))
		{
			attno = sortKey->ssup_attno;

			datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
			datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

			compare = ApplySortComparator(datum1, isnull1,
										  datum2, isnull2,
										  sortKey);
			if (compare != 0)
				return compare; /* done when we find unequal attributes */

			/* they are equal, so we only need to examine one null flag */
			if (isnull1)
				equal_hasnull = true;
		}
	}

	/* FIXME: all orioledb indexes should be unique */

	/*
	 * If btree has asked us to enforce uniqueness, complain if two equal
	 * tuples are detected (unless there was at least one NULL field).
	 *
	 * It is sufficient to make the test here, because if two tuples are equal
	 * they *must* get compared at some stage of the sort --- otherwise the
	 * sort algorithm wouldn't have checked whether one must appear before the
	 * other.
	 */
	if (arg->enforceUnique && !equal_hasnull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("could not create unique index \"%s\"",
						arg->id->name.data),
				 errdetail("Duplicate keys exist.")));
	}

	return 0;
}

static void
read_rebuild_entry(void *ptr, OIndexDescr *newIdx, OIndexDescr *oldIdx,
				   OTuple *key, OTuple *oldpk, BTreeLocationHint *hint)
{
	Pointer		p = (Pointer) ptr;
	int			oldlen;

	key->formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	key->data = p;
	p += o_tuple_size(*key, &newIdx->nonLeafSpec);
	oldlen = *((int *) p);
	p += sizeof(int);
	oldpk->formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	oldpk->data = p;
	p += oldlen;
	p += MAXALIGN(oldlen) - oldlen;
	memcpy(hint, p, sizeof(BTreeLocationHint));
}

static int
comparetup_orioledb_primary_rebuild(const SortTuple *a, const SortTuple *b,
									Tuplesortstate *state)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	SortSupport sortKey = base->sortKeys;
	OTuple		ltup;
	OTuple		rtup;
	OTuple		loldpk;
	OTuple		roldpk;
	BTreeLocationHint lhint,
				rhint;
	TupleDesc	tupDesc;
	TupleDesc	oldTupDesc;
	bool		equal_hasnull = false;
	int			nkey;
	int32		compare;
	AttrNumber	attno;
	Datum		datum1,
				datum2;
	bool		isnull1,
				isnull2;
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->newIdx->nonLeafSpec;
	OTupleFixedFormatSpec *oldspec = &arg->oldIdx->nonLeafSpec;

	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		return compare;

	read_rebuild_entry(a->tuple, arg->newIdx, arg->oldIdx, &ltup, &loldpk, &lhint);
	read_rebuild_entry(b->tuple, arg->newIdx, arg->oldIdx, &rtup, &roldpk, &rhint);
	tupDesc = arg->newTupDesc;
	oldTupDesc = arg->oldTupDesc;

	if (sortKey->abbrev_converter)
	{
		attno = sortKey->ssup_attno;

		datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
		datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

		compare = ApplySortAbbrevFullComparator(datum1, isnull1,
												datum2, isnull2,
												sortKey);
		if (compare != 0)
			return compare;
	}

	if (a->isnull1)
		equal_hasnull = true;

	sortKey++;
	for (nkey = 1; nkey < base->nKeys; nkey++, sortKey++)
	{
		if (!OIgnoreColumn(arg->newIdx, nkey))
		{
			attno = sortKey->ssup_attno;

			datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
			datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

			compare = ApplySortComparator(datum1, isnull1,
										  datum2, isnull2,
										  sortKey);
			if (compare != 0)
				return compare;

			if (isnull1)
				equal_hasnull = true;
		}
	}

	if (arg->enforceUnique && !equal_hasnull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("could not create unique index \"%s\"",
						arg->newIdx->name.data),
				 errdetail("Duplicate keys exist.")));
	}

	/* tie-breaker on old primary key */
	for (nkey = 0; nkey < arg->oldIdx->nPrimaryFields; nkey++)
	{
		Datum		lv;
		Datum		rv;
		bool		lnull;
		bool		rnull;

		if (OIgnoreColumn(arg->oldIdx, nkey))
			continue;

		lv = o_fastgetattr(loldpk, nkey + 1, oldTupDesc, oldspec, &lnull);
		rv = o_fastgetattr(roldpk, nkey + 1, oldTupDesc, oldspec, &rnull);

		compare = o_call_comparator(arg->oldIdx->fields[nkey].comparator,
									lv, rv);
		if (compare != 0)
			return compare;
	}

	return 0;
}

static void
writetup_orioledb_index(Tuplesortstate *state, LogicalTape *tape, SortTuple *stup)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	OTuple		tuple;
	int			tuplen;

	tuple = read_o_tuple(stup->tuple);
	tuplen = o_tuple_size(tuple, spec) + sizeof(int) + 1;

	LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
	LogicalTapeWrite(tape, (void *) tuple.data, o_tuple_size(tuple, spec));
	LogicalTapeWrite(tape, (void *) &tuple.formatFlags, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) /* need trailing length word? */
		LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
}

static void
readtup_orioledb_index(Tuplesortstate *state, SortTuple *stup,
					   LogicalTape *tape, unsigned int len)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	uint32		tuplen = len - sizeof(int) - 1;
	Pointer		tup = (Pointer) tuplesort_readtup_alloc(state, MAXIMUM_ALIGNOF + tuplen);
	OTuple		tuple;

	/* read in the tuple proper */
	LogicalTapeReadExact(tape, tup + MAXIMUM_ALIGNOF, tuplen);
	LogicalTapeReadExact(tape, tup, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) /* need trailing length word? */
		LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));
	stup->tuple = (void *) tup;
	tuple = read_o_tuple(tup);
	/* set up first-column key value */
	stup->datum1 = o_fastgetattr(tuple,
								 base->sortKeys[0].ssup_attno,
								 arg->tupDesc,
								 spec,
								 &stup->isnull1);
}

static void
removeabbrev_orioledb_index(Tuplesortstate *state, SortTuple *stups,
							int count)
{
	int			i;
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;

	for (i = 0; i < count; i++)
	{
		SortTuple  *stup = &stups[i];
		OTuple		tup;

		tup = read_o_tuple(stup->tuple);

		stup->datum1 = o_fastgetattr(tup,
									 base->sortKeys[0].ssup_attno,
									 arg->tupDesc,
									 spec,
									 &stup->isnull1);
	}
}

static void
removeabbrev_orioledb_primary_rebuild(Tuplesortstate *state, SortTuple *stups,
									  int count)
{
	int			i;
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->newIdx->nonLeafSpec;

	for (i = 0; i < count; i++)
	{
		SortTuple  *stup = &stups[i];
		OTuple		tup;
		OTuple		oldpk;
		BTreeLocationHint hint;

		read_rebuild_entry(stup->tuple, arg->newIdx, arg->oldIdx, &tup, &oldpk, &hint);

		stup->datum1 = o_fastgetattr(tup,
									 base->sortKeys[0].ssup_attno,
									 arg->newTupDesc,
									 spec,
									 &stup->isnull1);
	}
}

static int
rebuild_tuple_data_size(OIndexRebuildPkSortArg *arg, void *ptr)
{
	OTuple		tup;
	int			oldpk_len;
	Pointer		p = (Pointer) ptr;

	tup.formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	tup.data = p;
	p += o_tuple_size(tup, &arg->newIdx->nonLeafSpec);
	oldpk_len = *((int *) p);
	return o_tuple_size(tup, &arg->newIdx->nonLeafSpec) +
		sizeof(int) + MAXIMUM_ALIGNOF + MAXALIGN(oldpk_len) +
		sizeof(BTreeLocationHint);
}

static void
writetup_orioledb_primary_rebuild(Tuplesortstate *state, LogicalTape *tape,
								  SortTuple *stup)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	int			data_size;
	int			tuplen;

	data_size = rebuild_tuple_data_size(arg, stup->tuple);
	tuplen = data_size + sizeof(int) + 1;

	LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
	LogicalTapeWrite(tape, (void *) ((Pointer) stup->tuple + MAXIMUM_ALIGNOF), data_size);
	LogicalTapeWrite(tape, (void *) stup->tuple, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS)
		LogicalTapeWrite(tape, (void *) &tuplen, sizeof(tuplen));
}

static void
readtup_orioledb_primary_rebuild(Tuplesortstate *state, SortTuple *stup,
								 LogicalTape *tape, unsigned int len)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	uint32		tuplen = len - sizeof(int) - 1;
	Pointer		tup = (Pointer) tuplesort_readtup_alloc(state, MAXIMUM_ALIGNOF + tuplen);
	OTuple		key;
	OTuple		oldpk;

	LogicalTapeReadExact(tape, tup + MAXIMUM_ALIGNOF, tuplen);
	LogicalTapeReadExact(tape, tup, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS)
		LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));
	stup->tuple = (void *) tup;
	{
		BTreeLocationHint	h;

		read_rebuild_entry(tup, arg->newIdx, arg->oldIdx, &key, &oldpk, &h);
	}
	stup->datum1 = o_fastgetattr(key,
								 base->sortKeys[0].ssup_attno,
								 arg->newTupDesc,
								 &arg->newIdx->nonLeafSpec,
								 &stup->isnull1);
}

Tuplesortstate *
tuplesort_begin_orioledb_index(OIndexDescr *idx,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexBuildSortArg *arg;
	int			sort_fields;
	int			i;

	if (idx->unique)
		sort_fields = idx->nKeyFields;
	else
		sort_fields = idx->nFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = idx;
	arg->tupDesc = idx->leafTupdesc;
	arg->enforceUnique = idx->unique;

	base->sortKeys = (SortSupport) palloc0(sort_fields *
										   sizeof(SortSupportData));
	base->nKeys = sort_fields;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < sort_fields; i++)
	{
		if (!OIgnoreColumn(idx, i))
		{
			SortSupport sortKey = &base->sortKeys[i];

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = idx->fields[i].collation;
			sortKey->ssup_nulls_first = idx->fields[i].nullfirst;
			sortKey->ssup_attno =
				OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
			sortKey->abbreviate = (i == 0);
			sortKey->ssup_reverse = !idx->fields[i].ascending;
			/* FIXME: no abbrev converter yet */
			o_finish_sort_support_function(idx->fields[i].comparator, sortKey);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return state;
}

Tuplesortstate *
tuplesort_begin_orioledb_primary_rebuild(OIndexDescr *idx,
										 OIndexDescr *old_primary,
										 int workMem,
										 bool randomAccess,
										 SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexRebuildPkSortArg *arg;
	int			sort_fields;
	int			i;

	sort_fields = idx->nPrimaryFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexRebuildPkSortArg *) palloc0(sizeof(OIndexRebuildPkSortArg));
	arg->newIdx = idx;
	arg->oldIdx = old_primary;
	arg->newTupDesc = idx->nonLeafTupdesc;
	arg->oldTupDesc = old_primary->nonLeafTupdesc;
	arg->enforceUnique = idx->unique;

	base->sortKeys = (SortSupport) palloc0(sort_fields *
										   sizeof(SortSupportData));
	base->nKeys = sort_fields;

	base->removeabbrev = removeabbrev_orioledb_primary_rebuild;
	base->comparetup = comparetup_orioledb_primary_rebuild;
	base->writetup = writetup_orioledb_primary_rebuild;
	base->readtup = readtup_orioledb_primary_rebuild;
	base->arg = arg;

	for (i = 0; i < sort_fields; i++)
	{
		if (!OIgnoreColumn(idx, i))
		{
			SortSupport sortKey = &base->sortKeys[i];

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = idx->fields[i].collation;
			sortKey->ssup_nulls_first = idx->fields[i].nullfirst;
			sortKey->ssup_attno =
				OIndexKeyAttnumToTupleAttnum(BTreeKeyNonLeafTuple, idx, i + 1);
			sortKey->abbreviate = (i == 0);
			sortKey->ssup_reverse = !idx->fields[i].ascending;
			o_finish_sort_support_function(idx->fields[i].comparator, sortKey);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return state;
}

Tuplesortstate *
tuplesort_begin_orioledb_toast(OIndexDescr *toast,
							   OIndexDescr *primary,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	OIndexBuildSortArg *arg;
	SortSupport sortKey;
	OIndexField field;
	int			key_fields;
	int			i;

	key_fields = primary->nKeyFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = primary;
	arg->tupDesc = toast->leafTupdesc;
	arg->enforceUnique = true;

	base->sortKeys = (SortSupport)
		palloc0((key_fields + TOAST_NON_LEAF_FIELDS_NUM) *
				sizeof(SortSupportData));
	base->nKeys = key_fields + TOAST_NON_LEAF_FIELDS_NUM;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < key_fields; i++)
	{
		sortKey = &base->sortKeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = primary->fields[i].collation;
		sortKey->ssup_nulls_first = primary->fields[i].nullfirst;
		sortKey->ssup_attno = i + 1;
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !primary->fields[i].ascending;
		/* FIXME: no abbrev converter yet */
		o_finish_sort_support_function(primary->fields[i].comparator, sortKey);
	}

	field.collation = DEFAULT_COLLATION_OID;

	/* ATTN_POS */
	sortKey = &base->sortKeys[key_fields];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 1;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT2_BTREE_OPS_OID);
	o_finish_sort_support_function(field.comparator, sortKey);

	/* CHUNKN_POS */
	sortKey = &base->sortKeys[key_fields + 1];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 2;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT4_BTREE_OPS_OID);
	o_finish_sort_support_function(field.comparator, sortKey);

	MemoryContextSwitchTo(oldcontext);

	return state;
}

OTuple
tuplesort_getotuple(Tuplesortstate *state, bool forward)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(TuplesortstateGetPublic(state)->sortcontext);
	SortTuple	stup;
	OTuple		result;

	if (!tuplesort_gettuple_common(state, forward, &stup))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	if (stup.tuple)
	{
		result = read_o_tuple(stup.tuple);
	}
	else
	{
		result.data = NULL;
		result.formatFlags = 0;
	}

	return result;
}

void
tuplesort_putotuple(Tuplesortstate *state, OTuple tup)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexBuildSortArg *arg = (OIndexBuildSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->id->leafSpec;
	MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
	SortTuple	stup;
	int			tupsize;
	OTuple		written_tup;
#if PG_VERSION_NUM >= 170000
	Size		tuplen;
#endif

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	tupsize = o_tuple_size(tup, spec);
	stup.tuple = MemoryContextAlloc(base->tuplecontext, MAXIMUM_ALIGNOF + tupsize);
	write_o_tuple(stup.tuple, tup, tupsize);
	written_tup = read_o_tuple(stup.tuple);

	stup.datum1 = o_fastgetattr(written_tup,
								base->sortKeys[0].ssup_attno,
								arg->tupDesc,
								spec,
								&stup.isnull1);
#if PG_VERSION_NUM >= 170000
	/* GetMemoryChunkSpace is not supported for bump contexts */
	if (TupleSortUseBumpTupleCxt(base->sortopt))
		tuplen = MAXALIGN(tupsize);
	else
		tuplen = GetMemoryChunkSpace(stup.tuple);

	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1, tuplen);
#else
	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1);
#endif
	MemoryContextSwitchTo(oldcontext);
}

void
tuplesort_put_rebuild_primary(Tuplesortstate *state, OTuple key, OTuple oldpk,
							  BTreeLocationHint *hint)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	OTupleFixedFormatSpec *spec = &arg->newIdx->nonLeafSpec;
	OTupleFixedFormatSpec *oldspec = &arg->oldIdx->nonLeafSpec;
	MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
	SortTuple	stup;
	int			keysize;
	int			oldpksize;
	Pointer		ptr;
#if PG_VERSION_NUM >= 170000
	Size		tuplen;
#endif

	keysize = o_tuple_size(key, spec);
	oldpksize = o_tuple_size(oldpk, oldspec);
	stup.tuple = MemoryContextAlloc(base->tuplecontext,
									MAXIMUM_ALIGNOF + keysize + sizeof(int) + MAXIMUM_ALIGNOF + MAXALIGN(oldpksize) + sizeof(BTreeLocationHint));
	ptr = (Pointer) stup.tuple;

	*((uint8 *) ptr) = key.formatFlags;
	ptr += MAXIMUM_ALIGNOF;
	memcpy(ptr, key.data, keysize);
	ptr += keysize;
	memcpy(ptr, &oldpksize, sizeof(int));
	ptr += sizeof(int);
	*((uint8 *) ptr) = oldpk.formatFlags;
	ptr += MAXIMUM_ALIGNOF;
	memcpy(ptr, oldpk.data, oldpksize);
	ptr += oldpksize;
	if (oldpksize != MAXALIGN(oldpksize))
	{
		int			pad = MAXALIGN(oldpksize) - oldpksize;

		memset(ptr, 0, pad);
		ptr += pad;
	}
	memcpy(ptr, hint, sizeof(BTreeLocationHint));

	stup.datum1 = o_fastgetattr(key,
								base->sortKeys[0].ssup_attno,
								arg->newTupDesc,
								spec,
								&stup.isnull1);
#if PG_VERSION_NUM >= 170000
	if (TupleSortUseBumpTupleCxt(base->sortopt))
		tuplen = MAXALIGN(keysize + sizeof(int) + MAXIMUM_ALIGNOF + oldpksize);
	else
		tuplen = GetMemoryChunkSpace(stup.tuple);

	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1, tuplen);
#else
	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1);
#endif
	MemoryContextSwitchTo(oldcontext);
}

bool
tuplesort_get_rebuild_oldpk(Tuplesortstate *state, OTuple *oldpk,
							BTreeLocationHint *hint, bool forward)
{
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	MemoryContext oldcontext;
	SortTuple	stup;
	OIndexRebuildPkSortArg *arg = (OIndexRebuildPkSortArg *) base->arg;
	OTuple		key;

	oldcontext = MemoryContextSwitchTo(base->sortcontext);
	if (!tuplesort_gettuple_common(state, forward, &stup))
	{
		MemoryContextSwitchTo(oldcontext);
		return false;
	}

	read_rebuild_entry(stup.tuple, arg->newIdx, arg->oldIdx, &key, oldpk, hint);
	MemoryContextSwitchTo(oldcontext);
	return true;
}
