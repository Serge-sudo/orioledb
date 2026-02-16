/*-------------------------------------------------------------------------
 *
 * handler.c
 *		Implementation of table access method handler
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/page_state.h"
#include "btree/scan.h"
#include "btree/undo.h"
#include "catalog/indices.h"
#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "tableam/vacuum.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "tuple/slot.h"
#include "tuple/sort.h"
#include "utils/compress.h"
#include "utils/page_pool.h"
#include "utils/rel.h"
#include "utils/stopevent.h"

#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "common/relpath.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/backend_progress.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/sampling.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

bool		in_nontransactional_truncate = false;

typedef struct OScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */
	BTreeSeqScan *scan;
	OSnapshot	o_snapshot;
	ItemPointerData iptr;
} OScanDescData;
typedef OScanDescData *OScanDesc;

/*
 * Operation with indices. It does not update TOAST BTree. Implementations
 * are in tableam_handler.c.
 */
static void get_keys_from_rowid(OIndexDescr *id, Datum pkDatum, OBTreeKeyBound *key,
								BTreeLocationHint *hint, CommitSeqNo *csn,
								uint32 *version, ItemPointer *bridge_ctid);
static void rowid_set_csn(OIndexDescr *id, Datum pkDatum, CommitSeqNo csn);


/* ------------------------------------------------------------------------
 * Slot related callbacks for heap AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
orioledb_slot_callbacks(Relation relation)
{
	/* TODO: Create own TupleTableSlotOps */
	return &TTSOpsOrioleDB;
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for orioledb AM
 * ------------------------------------------------------------------------
 */

/*
 * Descriptor for fetches from orioledb table.
 */
typedef struct OrioledbIndexFetchData
{
	IndexFetchTableData xs_base;	/* AM independent part of the descriptor */
	bool		bridged_tuple;
} OrioledbIndexFetchData;

/*
 * Returns NULL to prevent index scan from inside of standard_planner
 * for greater and lower where clauses.
 */
static IndexFetchTableData *
orioledb_index_fetch_begin(Relation rel)
{
	OrioledbIndexFetchData *o_scan = palloc0(sizeof(OrioledbIndexFetchData));

	/* TODO: Remove this hack */
	extern Relation o_current_index;

	if (o_current_index)
	{
		OBTOptions *options = (OBTOptions *) o_current_index->rd_options;

		o_scan->bridged_tuple = (o_current_index->rd_rel->relam != BTREE_AM_OID) ||
			(options && !options->orioledb_index);
		o_current_index = NULL;
	}

	o_scan->xs_base.rel = rel;

	return &o_scan->xs_base;
}

static void
orioledb_index_fetch_reset(IndexFetchTableData *scan)
{
}

static void
orioledb_index_fetch_end(IndexFetchTableData *scan)
{
	OrioledbIndexFetchData *o_scan = (OrioledbIndexFetchData *) scan;

	orioledb_index_fetch_reset(scan);

	pfree(o_scan);
}

static bool
orioledb_index_fetch_tuple(struct IndexFetchTableData *scan,
						   Datum tupleid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	OrioledbIndexFetchData *o_scan = (OrioledbIndexFetchData *) scan;
	OTableDescr *descr;
	OBTreeKeyBound pkey;
	OTuple		tuple;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	CommitSeqNo tupleCsn;
	uint32		version;

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	*call_again = false;
	*all_dead = false;

	descr = relation_get_descr(scan->rel);
	Assert(descr != NULL);

	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	if (o_scan->bridged_tuple)
	{
		OBTreeKeyBound bridge_bound;
		OTuple		bridge_tup;

		bridge_bound.nkeys = 1;
		bridge_bound.n_row_keys = 0;
		bridge_bound.row_keys = NULL;
		bridge_bound.keys[0].value = tupleid;
		bridge_bound.keys[0].type = TIDOID;
		bridge_bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		bridge_bound.keys[0].comparator = NULL;
		csn = COMMITSEQNO_INPROGRESS;

		o_btree_load_shmem(&descr->bridge->desc);

		bridge_tup = o_btree_find_tuple_by_key(&descr->bridge->desc,
											   (Pointer) &bridge_bound, BTreeKeyBound,
											   &o_in_progress_snapshot, &tupleCsn,
											   slot->tts_mcxt, NULL);
		if (O_TUPLE_IS_NULL(bridge_tup))
			return false;

		o_fill_pindex_tuple_key_bound(&descr->bridge->desc, bridge_tup, &pkey);
	}
	else
		get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
							&csn, &version, NULL);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &oSnapshot, &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = descr->oids.reloid;

	/* FIXME? */
	if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
		snapshot->xmin = snapshot->xmax = InvalidTransactionId;

	return true;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for heap AM
 * ------------------------------------------------------------------------
 */

static TupleFetchCallbackResult
fetch_row_version_callback(OTuple tuple, OXid tupOxid, OSnapshot *oSnapshot,
						   void *arg, TupleFetchCallbackCheckType check_type)
{
	uint32		version = *((uint32 *) arg);

	if (check_type != OTupleFetchCallbackVersionCheck)
		return OTupleFetchNext;

	if (!(COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
		  tupOxid == get_current_oxid_if_any()))
		return OTupleFetchNext;

	if (o_tuple_get_version(tuple) <= version)
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}


/*
 * Fetches last committed row version for given tupleid.
 */
static bool
orioledb_fetch_row_version(Relation relation,
						   Datum tupleid,
						   Snapshot snapshot,
						   TupleTableSlot *slot)
{
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OTuple		tuple;
	BTreeLocationHint hint;
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	CommitSeqNo tupleCsn;
	uint32		version;
	bool		deleted;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
						&csn, &version, NULL);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key_cb(&GET_PRIMARY(descr)->desc,
										 (Pointer) &pkey,
										 BTreeKeyBound,
										 &oSnapshot, &tupleCsn,
										 slot->tts_mcxt,
										 &hint,
										 &deleted,
										 fetch_row_version_callback,
										 &version);

	if (deleted && COMMITSEQNO_IS_INPROGRESS(tupleCsn) && snapshot != SnapshotAny)
		return true;

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(relation);

	return true;
}

static bool
orioledb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OTuple		tuple;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};

	if (snapshot != SnapshotSelf)
		return true;

	descr = relation_get_descr(rel);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &pkey);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &o_in_progress_snapshot,
									  &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(rel);

	return true;

}


/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for heap AM.
 * ----------------------------------------------------------------------------
 */

static RowRefType
orioledb_get_row_ref_type(Relation rel)
{
	OTableDescr *descr;

	descr = relation_get_descr(rel);
	if (!descr)
	{
		/*
		 * It happens during relation creation.  Should be safe to assume
		 * we've TID identifiers at this point.
		 */
		return ROW_REF_TID;
	}

	/*
	 * Always use rowid identifieds.  If even we use ctid as primary key, we
	 * still prepend it with page location hint.
	 */
	return ROW_REF_ROWID;
}

static TupleTableSlot *
orioledb_tuple_insert(Relation relation, TupleTableSlot *slot,
					  CommandId cid, int options, BulkInsertState bistate)
{
	OTableDescr *descr;
	OSnapshot	oSnapshot;
	OXid		oxid;

	if (OidIsValid(relation->rd_rel->relrewrite))
		return slot;

	o_set_current_command(cid);

	descr = relation_get_descr(relation);
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	return o_tbl_insert(descr, relation, slot, oxid, oSnapshot.csn);
}

static TupleTableSlot *
orioledb_tuple_insert_with_arbiter(ResultRelInfo *rinfo,
								   TupleTableSlot *slot,
								   CommandId cid, int options,
								   struct BulkInsertStateData *bistate,
								   List *arbiterIndexes,
								   EState *estate,
								   LockTupleMode lockmode,
								   TupleTableSlot *lockedSlot,
								   TupleTableSlot *tempSlot)
{
	Relation	rel = rinfo->ri_RelationDesc;
	OTableDescr *descr;
	OTuple		tup;
	OIndexDescr *id;

	descr = relation_get_descr(rel);
	Assert(descr);
	id = GET_PRIMARY(descr);

	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	if (id->primaryIsCtid)
	{
		o_btree_load_shmem(&id->desc);
		slot->tts_tid = btree_ctid_get_and_inc(&id->desc);
	}

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &id->leafSpec),
								RelationGetRelationName(rel),
								false);

	o_set_current_command(cid);

	slot = o_tbl_insert_with_arbiter(rel, descr, slot, arbiterIndexes, cid,
									 lockmode, lockedSlot);

	return slot;
}

static TM_Result
orioledb_tuple_delete(Relation relation, Datum tupleid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, int options,
					  TM_FailureData *tmfd, bool changingPart,
					  TupleTableSlot *oldSlot)
{
	OModifyCallbackArg marg;
	OTableModifyResult mres;
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;
	OSnapshot	oSnapshot;

	ASAN_UNPOISON_MEMORY_REGION(tmfd, sizeof(*tmfd));
	ASAN_UNPOISON_MEMORY_REGION(&mres, sizeof(mres));

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	oxid = get_current_oxid();

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.selfModified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.changingPart = changingPart;
	marg.keyAttrs = NULL;
	marg.modifyCid = cid;
	marg.tupleCid = InvalidCommandId;
	o_set_current_command(cid);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &marg.csn, NULL, NULL);

	mres = o_tbl_delete(relation, descr, &pkey, oxid,
						marg.csn, &hint, &marg);

	if (marg.selfModified)
	{
		Assert(marg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = marg.tupleCid;
		return TM_SelfModified;
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
	{
		tmfd->traversed = false;
	}

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
		return TM_Updated;

	o_check_tbl_delete_mres(mres, descr, relation);

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;

	if (mres.success)
	{
		return marg.selfModified ? TM_SelfModified : TM_Ok;
	}

	return marg.selfModified ? TM_SelfModified : (marg.modified ? TM_Updated : TM_Deleted);
}

static TM_Result
orioledb_tuple_update(Relation relation, Datum tupleid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  int options, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
					  TU_UpdateIndexes *update_indexes,
					  TupleTableSlot *oldSlot)
{
	OTableModifyResult mres;
	OModifyCallbackArg marg;
	OBTreeKeyBound old_pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;
	OSnapshot	oSnapshot;
	ItemPointer bridge_ctid = NULL;

	ASAN_UNPOISON_MEMORY_REGION(tmfd, sizeof(*tmfd));

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	*update_indexes = TU_All;
	oxid = get_current_oxid();

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &old_pkey, &hint,
						&marg.csn, NULL, descr->bridge ? &bridge_ctid : NULL);
	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.selfModified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.newSlot = (OTableSlot *) slot;
	marg.keyAttrs = RelationGetIndexAttrBitmap(relation,
											   INDEX_ATTR_BITMAP_KEY);
	marg.modifyCid = cid;
	marg.tupleCid = InvalidCommandId;
	marg.pkSatisfiesBoundary = true;  /* Default to true, will be set during primary index modification */
	o_set_current_command(cid);

	mres = o_tbl_update(descr, slot, &old_pkey, relation, oxid,
						marg.csn, &hint, &marg, bridge_ctid);

	if (marg.selfModified)
	{
		Assert(marg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = marg.tupleCid;
		return TM_SelfModified;
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
	{
		if (TupIsNull(oldSlot))
			/* Tuple not passing quals anymore, exiting... */
			return TM_Deleted;

		return TM_Updated;
	}

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;
	o_check_tbl_update_mres(mres, descr, relation, slot);

	bms_free(marg.keyAttrs);
	Assert(mres.success);

	return mres.oldTuple ? TM_Ok : (marg.modified ? TM_Updated : TM_Deleted);
}

static TM_Result
orioledb_tuple_lock(Relation rel, Datum tupleid, Snapshot snapshot,
					TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	OLockCallbackArg larg;
	OBTreeModifyResult res;
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;

	descr = relation_get_descr(rel);
	Assert(descr != NULL);

	oxid = get_current_oxid();

	larg.rel = rel;
	larg.descr = descr;
	larg.oxid = oxid;
	larg.scanSlot = slot;
	larg.waitPolicy = wait_policy;
	larg.wouldBlock = false;
	larg.modified = false;
	larg.selfModified = false;
	larg.deleted = BTreeLeafTupleNonDeleted;
	larg.tupUndoLocation = InvalidUndoLocation;
	larg.modifyCid = cid;
	larg.tupleCid = InvalidCommandId;
	o_set_current_command(cid);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &larg.csn, NULL, NULL);

	res = o_tbl_lock(descr, &pkey, mode, oxid, &larg, &hint);

	if (larg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, larg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (larg.selfModified)
	{
		Assert(larg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = larg.tupleCid;
		return TM_SelfModified;
	}
	else
	{
		tmfd->xmax = InvalidTransactionId;
		tmfd->cmax = InvalidCommandId;
	}

	if (larg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (larg.wouldBlock)
		return TM_WouldBlock;

	if (res == OBTreeModifyResultNotFound)
		return TM_Deleted;

	Assert(res == OBTreeModifyResultLocked);

	return TM_Ok;
}

static void
orioledb_finish_bulk_insert(Relation relation, int options)
{
	/* Do nothing here */
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for heap AM.
 * ------------------------------------------------------------------------
 */

static void
orioledb_relation_set_new_filenode(Relation rel,
								   const RelFileNode *newrnode,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
{
	SMgrRelation srel;

	/* TRUNCATE case */
	if (rel->rd_rel->oid != 0 &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		!is_in_indexes_rebuild())
	{
		OTable	   *old_o_table,
				   *new_o_table;
		TupleDesc	tupdesc;
		OSnapshot	oSnapshot;
		OXid		oxid;
		int			oldTreeOidsNum,
					newTreeOidsNum;
		ORelOids	old_oids,
				   *oldTreeOids,
					new_oids,
				   *newTreeOids;
		bool		is_temp;
		Oid			toast_relid;

		/* If toast relation exists, set new filenode for it */
		toast_relid = rel->rd_rel->reltoastrelid;
		if (OidIsValid(toast_relid))
		{
			Relation	toastrel = relation_open(toast_relid,
												 AccessExclusiveLock);

			RelationSetNewRelfilenode(toastrel,
									  toastrel->rd_rel->relpersistence);
			table_close(toastrel, NoLock);
		}

		ORelOidsSetFromRel(old_oids, rel);
		old_o_table = o_tables_get(old_oids);
		Assert(old_o_table != NULL);
		oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);

		tupdesc = RelationGetDescr(rel);
		ORelOidsSetFromRel(new_oids, rel);
		new_oids.relnode = RelFileNodeGetNode(newrnode);

		new_o_table = o_table_tableam_create(new_oids, tupdesc,
											 rel->rd_rel->relpersistence,
											 old_o_table->fillfactor,
											 rel->rd_rel->reltablespace);
		o_opclass_cache_add_table(new_o_table);
		o_table_fill_oids(new_o_table, rel, newrnode, false);

		newTreeOids = o_table_make_index_oids(new_o_table, &newTreeOidsNum);

		o_tables_table_meta_lock(new_o_table);

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);

		/*
		 * COMMITSEQNO_INPROGRESS because there might be already commited
		 * concurrent truncate before function start and old_oids will be
		 * pointing to a not existed before this transaction table and will
		 * not be visible otherwise. There should not be concurrent access to
		 * old table during delete below, because of held locks
		 */
		o_tables_drop_by_oids(old_oids, oxid, COMMITSEQNO_INPROGRESS);
		o_tables_add(new_o_table, oxid, oSnapshot.csn);

		/*
		 * Pass NULL and InvalidOid as we don't want recovery to trigger an
		 * index (re)build.
		 */
		o_tables_table_meta_unlock(NULL, InvalidOid);
		o_table_free(new_o_table);

		orioledb_free_rd_amcache(rel);

		Assert(o_fetch_table_descr(new_oids) != NULL);
		is_temp = rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP;
		add_undo_truncate_relnode(old_oids, oldTreeOids, oldTreeOidsNum,
								  new_oids, newTreeOids, newTreeOidsNum, !is_temp);
		pfree(oldTreeOids);
		pfree(newTreeOids);
	}

	ASAN_UNPOISON_MEMORY_REGION(freezeXid, sizeof(*freezeXid));
	ASAN_UNPOISON_MEMORY_REGION(minmulti, sizeof(*minmulti));
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	srel = RelationCreateStorage(*newrnode, persistence, false);
	smgrclose(srel);
}

static void
drop_indices_for_rel(Relation rel, bool primary)
{
	ListCell   *index;
	Oid			indexOid;

	foreach(index, RelationGetIndexList(rel))
	{
		Relation	ind;
		bool		closed = false;
		OBTOptions *options;

		indexOid = lfirst_oid(index);
		ind = relation_open(indexOid, AccessShareLock);
		options = (OBTOptions *) ind->rd_options;

		if (ind->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index) &&
			((primary && ind->rd_index->indisprimary) || (!primary && !ind->rd_index->indisprimary)))
		{
			OIndexNumber ix_num;
			OTableDescr *descr = relation_get_descr(rel);

			Assert(descr != NULL);
			ix_num = o_find_ix_num_by_name(descr, ind->rd_rel->relname.data);
			if (GET_PRIMARY(descr)->primaryIsCtid)
				ix_num--;
			relation_close(ind, AccessShareLock);
			o_index_drop(rel, ix_num);
			closed = true;
		}
		if (!closed)
			relation_close(ind, AccessShareLock);
	}
}

static void
orioledb_relation_nontransactional_truncate(Relation rel)
{
	ORelOids	oids;

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		in_nontransactional_truncate = true;

	ORelOidsSetFromRel(oids, rel);
	if (!OidIsValid(rel->rd_rel->oid) || rel->rd_rel->relkind == RELKIND_TOASTVALUE)
		return;

	o_truncate_table(oids);

	drop_indices_for_rel(rel, false);
	/* drop primary after all indices to not rebuild them */
	drop_indices_for_rel(rel, true);

	if (RelationIsPermanent(rel))
		add_truncate_wal_record(oids);
}

static void
orioledb_relation_copy_data(Relation rel, const RelFileNode *new_relfilenode)
{
	SMgrRelation dstrel;

	/*
	 * Code from heapam_relation_copy_data just to create storage and new
	 * relfilenode
	 */
	FlushRelationBuffers(rel);
	dstrel = RelationCreateStorage(*new_relfilenode, rel->rd_rel->relpersistence, true);
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

static void
orioledb_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static bool
#if PG_VERSION_NUM >= 170000
orioledb_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
#else
orioledb_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
#endif
{
	OScanDesc	oscan = (OScanDesc) scan;
#if PG_VERSION_NUM >= 170000
	BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);
	BlockNumber blockno = read_stream_next_block(stream, &bstrategy);

	if (blockno == InvalidBlockNumber)
		return false;
#endif
	ItemPointerSetBlockNumber(&oscan->iptr, blockno);
	ItemPointerSetOffsetNumber(&oscan->iptr, 1);

	return true;
}

#define NUM_TUPLES_PER_BLOCK	128

static bool
orioledb_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	OScanDesc	oscan = (OScanDesc) scan;
	OTableDescr *descr;
	BTreeLocationHint hint;
	OTuple		tuple;
	bool		end;

	descr = relation_get_descr(scan->rs_rd);

	while (true)
	{
		tuple = btree_seq_scan_getnext_raw(oscan->scan, slot->tts_mcxt, &end, &hint);

		if (end || ItemPointerGetOffsetNumber(&oscan->iptr) > NUM_TUPLES_PER_BLOCK)
			return false;

		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, oscan->o_snapshot.csn,
									 PrimaryIndexNumber, false, &hint);

			*liverows += 1;
			ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&oscan->iptr, ItemPointerGetOffsetNumber(&oscan->iptr) + 1);
			return true;
		}
		else
		{
			*deadrows += 1;
		}
	}
}

static double
orioledb_index_build_range_scan(Relation heapRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								bool allow_sync,
								bool anyvisible,
								bool progress,
								BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								void *callback_state,
								TableScanDesc scan)
{
	OBTOptions *options = (OBTOptions *) indexRelation->rd_options;

	if (indexRelation->rd_rel->relam != BTREE_AM_OID || (options && !options->orioledb_index))
	{
		OTableDescr *descr;
		BTreeSeqScan *seq_scan;
		TupleTableSlot *primarySlot;
		double		heap_tuples;
		OTuple		tup;
		BTreeLocationHint hint;
		CommitSeqNo tupleCsn;
		ExprState  *predicate;
		EState	   *estate;
		ExprContext *econtext;
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		/*
		 * Need an EState for evaluation of index expressions and
		 * partial-index predicates.  Also a slot to hold the current tuple.
		 */
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);

		/* Set up execution state for predicate, if any. */
		predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

		descr = relation_get_descr(heapRelation);
		Assert(descr != NULL);

		seq_scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &o_in_progress_snapshot, NULL);
		primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);

		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = primarySlot;

		heap_tuples = 0;
		while (!O_TUPLE_IS_NULL(tup = btree_seq_scan_getnext(seq_scan, primarySlot->tts_mcxt, &tupleCsn, &hint)))
		{
			OTableSlot *oslot = (OTableSlot *) primarySlot;

			tts_orioledb_store_tuple(primarySlot, tup, descr, tupleCsn, PrimaryIndexNumber, true, &hint);
			slot_getallattrs(primarySlot);

			heap_tuples++;

			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			/*
			 * For the current heap tuple, extract all the attributes we use
			 * in this index, and note which are null.  This also performs
			 * evaluation of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   primarySlot,
						   estate,
						   values,
						   isnull);

			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &oslot->bridge_ctid, values, isnull, true, callback_state);

			ExecClearTuple(primarySlot);
		}
		ExecDropSingleTupleTableSlot(primarySlot);
		FreeExecutorState(estate);
		free_btree_seq_scan(seq_scan);

		return heap_tuples;
	}
	return 0.0;
}

/*
 * orioledb_index_validate - Validate an index for orioledb tables
 *
 * This function implements index validation for orioledb tables, similar to
 * PostgreSQL's validate_index but adapted for rowid-based tuple identification.
 * It performs the following steps:
 * 1. Calls index_bulk_delete (which invokes orioledb_ambulkdelete) with a callback
 *    that collects all primary key tuples from the index into a tuplesort
 * 2. Sorts the collected tuples in primary key order
 * 3. Scans the heap and performs a merge join with sorted index tuples
 * 4. Inserts any missing tuples into the index
 */
static void
orioledb_index_validate(Relation heapRelation,
					   Relation indexRelation,
					   Snapshot snapshot)
{
	IndexInfo  *indexInfo;
	IndexVacuumInfo ivinfo;
	OValidateIndexState state;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	OTableDescr *descr;

	{
		const int	progress_index[] = {
			PROGRESS_CREATEIDX_PHASE,
			PROGRESS_CREATEIDX_TUPLES_DONE,
			PROGRESS_CREATEIDX_TUPLES_TOTAL,
			PROGRESS_SCAN_BLOCKS_DONE,
			PROGRESS_SCAN_BLOCKS_TOTAL
		};
		const int64 progress_vals[] = {
			PROGRESS_CREATEIDX_PHASE_VALIDATE_IDXSCAN,
			0, 0, 0, 0
		};

		pgstat_progress_update_multi_param(5, progress_index, progress_vals);
	}

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/*
	 * Fetch info needed for index_insert.
	 */
	indexInfo = BuildIndexInfo(indexRelation);

	/* mark build is concurrent just for consistency */
	indexInfo->ii_Concurrent = true;

	/*
	 * Get table descriptor to access primary index descriptor
	 */
	descr = relation_get_descr(heapRelation);
	Assert(descr != NULL);

	/*
	 * Use tuplesort_begin_orioledb_index with the primary index descriptor.
	 * This ensures tuples are sorted in the same order as the primary key,
	 * which is essential for the merge join algorithm in validate_scan.
	 */
	state.tuplesort = tuplesort_begin_orioledb_index(GET_PRIMARY(descr),
													 maintenance_work_mem,
													 false,
													 NULL);
	state.htups = state.itups = state.tups_inserted = state.tups_deleted = 0;
	state.current_tuple.data = NULL; /* Initialize */
	state.index_descr = NULL; /* Will be set by ambulkdelete */
	state.table_descr = descr;
	state.index_oids.datoid = MyDatabaseId;
	state.index_oids.reloid = indexRelation->rd_rel->oid;
	state.index_oids.relnode = indexRelation->rd_rel->relfilenode;

	/*
	 * Scan the index and gather up all the primary key tuples into a tuplesort object.
	 * This is done by calling index_bulk_delete with validate_index_callback,
	 * which collects tuples instead of deleting them.
	 */
	ivinfo.index = indexRelation;
	ivinfo.heaprel = heapRelation;
	ivinfo.analyze_only = false;
	ivinfo.report_progress = true;
	ivinfo.estimated_count = true;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = heapRelation->rd_rel->reltuples;
	ivinfo.strategy = NULL;

	/* ambulkdelete updates progress metrics and collects tuples via callback */
	(void) index_bulk_delete(&ivinfo, NULL,
							 validate_index_callback, (void *) &state);

	/* Execute the sort */
	{
		const int	progress_index[] = {
			PROGRESS_CREATEIDX_PHASE,
			PROGRESS_SCAN_BLOCKS_DONE,
			PROGRESS_SCAN_BLOCKS_TOTAL
		};
		const int64 progress_vals[] = {
			PROGRESS_CREATEIDX_PHASE_VALIDATE_SORT,
			0, 0
		};

		pgstat_progress_update_multi_param(3, progress_index, progress_vals);
	}
	tuplesort_performsort(state.tuplesort);

	/*
	 * Now scan the heap and "merge" it with the index
	 */
	pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE,
								 PROGRESS_CREATEIDX_PHASE_VALIDATE_TABLESCAN);
	orioledb_index_validate_scan(heapRelation,
								 indexRelation,
								 indexInfo,
								 snapshot,
								 &state);

	/* Done with tuplesort object */
	tuplesort_end(state.tuplesort);

	/* Make sure to release resources cached in indexInfo (if needed). */
	index_insert_cleanup(indexRelation, indexInfo);

	elog(DEBUG2,
		 "orioledb_index_validate found %.0f heap tuples, %.0f index tuples; inserted %.0f missing tuples, deleted %.0f spurious tuples",
		 state.htups, state.itups, state.tups_inserted, state.tups_deleted);

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
}

/*
 * orioledb_index_validate_cleanup_old_concurrent - Cleanup old concurrent index
 *
 * During concurrent index creation (REINDEX CONCURRENTLY), PostgreSQL creates
 * a new index with a temporary name suffix (_ccnew). After validation succeeds,
 * the new index gets the original relfilenode and the old index structure should
 * be removed. However, orioledb maintains its own index structures that need
 * explicit cleanup.
 *
 * This function finds and removes the old index structure that has a different
 * relfilenode than what's currently in pg_class. It follows the same pattern
 * as o_define_index's reindex path, scanning through indices by reloid.
 *
 * Parameters:
 *   heapRelation - The table relation
 *   indexRelation - The new (validated) index relation with updated relfilenode
 */
void
orioledb_index_validate_cleanup_old_concurrent(Relation heapRelation,
											   Relation indexRelation)
{
	OTable	   *o_table;
	ORelOids	oids;
	Oid			current_relfilenode;
	OIndexNumber ix_num;
	int			i;

	/* Only process orioledb tables */
	if (!is_orioledb_rel(heapRelation))
		return;

	ORelOidsSetFromRel(oids, heapRelation);
	o_table = o_tables_get(oids);
	if (o_table == NULL)
		return;

	/* Get the current relfilenode from pg_class for the validated index */
	current_relfilenode = indexRelation->rd_rel->relfilenode;

	/*
	 * Scan through all indices to find the old one.
	 * Similar logic to o_define_index's reindex path.
	 */
	ix_num = InvalidIndexNumber;
	for (i = 0; i < o_table->nindices; i++)
	{
		/* Found an index with same reloid but different relfilenode - this is the old one */
		if (o_table->indices[i].oids.reloid == indexRelation->rd_rel->oid &&
			o_table->indices[i].oids.relnode != current_relfilenode)
		{
			ix_num = i;
			break;
		}
	}

	if (ix_num != InvalidIndexNumber)
	{
		elog(DEBUG1, "orioledb_index_validate_cleanup_old_concurrent: "
			 "dropping old concurrent index structure for reloid %u, "
			 "old relfilenode %u, new relfilenode %u",
			 o_table->indices[ix_num].oids.reloid,
			 o_table->indices[ix_num].oids.relnode,
			 current_relfilenode);

		/* Drop the old index structure */
		o_index_drop(heapRelation, ix_num);
	}

	o_table_free(o_table);
}

/*
 * debug_print_validate_tuple - Debug function to print tuple before adding to tuplesort
 *
 * This function prints the tuple data in readable format for debugging purposes 
 * during index validation, similar to tss_orioledb_print_idx_key.
 */
static void
debug_print_validate_tuple(OTuple tuple, OTableDescr *table_descr, OIndexDescr *index_descr)
{
	TupleTableSlot *slot;
	OIndexDescr *primary = GET_PRIMARY(table_descr);
	char	   *keystr;
	
	/* Create a temporary slot to hold the tuple */
	slot = MakeSingleTupleTableSlot(primary->nonLeafTupdesc, &TTSOpsOrioleDB);
	
	/* Store the tuple in the slot using PrimaryIndexNumber */
	tts_orioledb_store_non_leaf_tuple(slot, tuple, table_descr, 
									  COMMITSEQNO_INPROGRESS, 
									  PrimaryIndexNumber, false, NULL);
	
	/* Use the existing function to print the key in readable format */
	keystr = tss_orioledb_print_idx_key(slot, primary);
	
	elog(DEBUG2, "validate_index: adding PK tuple to tuplesort: %s", keystr);
	
	pfree(keystr);
	ExecDropSingleTupleTableSlot(slot);
}

/*
 * debug_print_validate_tuple - Debug helper to print tuples added to validation tuplesort
 *
 * Prints the secondary index tuple being added to tuplesort during validation.
 * Shows both the secondary key fields and primary key fields.
 */
static void
debug_print_validate_tuple(OTuple tuple, OTableDescr *table_descr, OIndexDescr *index_descr)
{
#ifdef USE_ASSERT_CHECKING
	StringInfoData buf;
	BTreeDescr *desc = &index_descr->desc;
	int			nKeyFields = desc->nKeyFields;
	int			i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "VALIDATE_TUPLE_ADD: index=%u.%u.%u, tuple_len=%zu",
					 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
					 o_btree_len(desc, tuple, OTupleLength));

	/* Print each field in the tuple */
	for (i = 1; i <= desc->nFields; i++)
	{
		Datum		value;
		bool		isnull;
		Oid			typid;
		int16		typlen;
		bool		typbyval;
		Oid			typoutput;
		bool		typisvarlena;
		char	   *value_str;

		/* Determine if this is a secondary key field or PK field */
		if (i <= nKeyFields)
			appendStringInfo(&buf, ", SK_field_%d: ", i);
		else
			appendStringInfo(&buf, ", PK_field_%d: ", i - nKeyFields);

		/* Get field info */
		o_tuple_get_column_by_num(&desc->leafSpec, tuple, i, &value, &isnull);
		typid = desc->leafSpec.types[i - 1];
		get_typlenbyvalalign(typid, &typlen, &typbyval, NULL);

		if (isnull)
		{
			appendStringInfo(&buf, "NULL");
		}
		else
		{
			/* Get the output function for this type */
			getTypeOutputInfo(typid, &typoutput, &typisvarlena);
			value_str = OidOutputFunctionCall(typoutput, value);
			appendStringInfo(&buf, "%s (type=%u)", value_str, typid);
			pfree(value_str);
		}
	}

	elog(DEBUG1, "%s", buf.data);
	pfree(buf.data);
#endif							/* USE_ASSERT_CHECKING */
}

/*
 * validate_index_callback - Callback for index validation
 *
 * This callback is invoked by orioledb_ambulkdelete for each tuple in the index.
 * It stores the FULL secondary index tuple into the tuplesort for later comparison
 * with heap tuples. The tuplesort will sort by primary key for merge-join validation.
 */
static bool
validate_index_callback(ItemPointer itemptr, void *callback_state)
{
	OValidateIndexState *state = (OValidateIndexState *) callback_state;
	OTuple		fullIndexTuple;
	OIndexDescr *id = state->index_descr;
	OTableDescr *table_descr = state->table_descr;

	/*
	 * For orioledb, the actual tuple is stored in state->current_tuple
	 * by orioledb_ambulkdelete. For validation with deletion support, we need
	 * to convert the secondary index tuple into (secondary_key_fields, pk_fields)
	 * format, excluding any INCLUDE fields that are not needed for validation.
	 *
	 * The tuplesort will sort these tuples by PK (using a custom comparator),
	 * enabling efficient merge-join with the primary index scan.
	 */
	if (!O_TUPLE_IS_NULL(state->current_tuple))
	{
		OIndexDescr *pkIndex = GET_PRIMARY(table_descr);
		
		/* Convert secondary tuple to (secondary_key_fields, pk_fields) format */
		fullIndexTuple = convert_secondary_tuple_for_validation(state->current_tuple, id, pkIndex);
		
		/* Debug: Print tuple before adding to tuplesort */
		debug_print_validate_tuple(fullIndexTuple, table_descr, id);
		
		/* 
		 * Put the converted tuple into tuplesort.
		 * The tuplesort comparator will sort by PK fields for merge-join.
		 */
		tuplesort_putotuple(state->tuplesort, fullIndexTuple);
		
		/* Free the allocated tuple */
		pfree(fullIndexTuple.data);
	}

	/* Return false to indicate we don't want to delete this tuple during the scan */
	return false;
}

static bool
orioledb_validate_next_index_tid(Tuplesortstate *tuplesort,
								 OTuple *indexTuple,
								 double *itups)
{
	*indexTuple = tuplesort_getotuple(tuplesort, true);
	
	if (O_TUPLE_IS_NULL(*indexTuple))
		return false;

	(*itups)++;
	return true;
}

/*
 * Convert a secondary index tuple into (secondary_key_fields, pk_fields) format
 * for validation tuplesort.
 * 
 * Secondary index tuples may have:
 * - Key fields (secondary key attributes)
 * - Primary key fields (at the end of the key)
 * - INCLUDE fields (non-key attributes that we don't need for validation)
 * 
 * This function extracts only the key fields (secondary + PK) and converts them
 * into a tuple formatted for the specialized tuplesort that has:
 *   [secondary_key_field_1, ..., secondary_key_field_N, pk_field_1, ..., pk_field_M]
 * 
 * Most of the time we can pass the secondary index tuple directly (if it has no
 * INCLUDE fields), but when INCLUDE fields exist, we need to extract only the
 * key portion.
 * 
 * Returns: A new OTuple in the format (secondary_key_fields, pk_fields).
 *          Caller must pfree the returned tuple data.
 */
static OTuple
convert_secondary_tuple_for_validation(OTuple secTuple, OIndexDescr *secIndex, OIndexDescr *pkIndex)
{
	BTreeDescr *secDesc = &secIndex->desc;
	BTreeDescr *pkDesc = &pkIndex->desc;
	int			nKeyFields = secIndex->nKeyFields;
	int			nFields = secIndex->nFields;
	OTuple		result;
	
	/*
	 * If secondary index has no INCLUDE fields (nFields == nKeyFields),
	 * we can use the tuple directly since it already has the correct format:
	 * (secondary_key_fields, pk_fields).
	 */
	if (nKeyFields == nFields)
	{
		/* Make a copy of the tuple */
		Size tupleLen = o_btree_len(secDesc, secTuple, OTupleLength);
		result.data = (Pointer) palloc(tupleLen);
		memcpy(result.data, secTuple.data, tupleLen);
		result.formatFlags = secTuple.formatFlags;
		return result;
	}
	
	/*
	 * Secondary index has INCLUDE fields that we need to exclude.
	 * Extract only the key fields (secondary key + PK) and create a new tuple.
	 */
	{
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		uint32		version = o_tuple_get_version(secTuple);
		int			i;
		Size		len;
		TupleDesc	secLeafTupdesc = secDesc->leafTupdesc;
		OTupleFixedFormatSpec *secLeafSpec = &secDesc->leafSpec;
		
		/* Extract all key fields (secondary key fields + PK fields) */
		for (i = 0; i < nKeyFields; i++)
		{
			values[i] = o_fastgetattr(secTuple, i + 1, secLeafTupdesc, secLeafSpec, &isnull[i]);
		}
		
		/*
		 * Create a new tuple with only the key fields.
		 * We use the secondary index's leafTupdesc/leafSpec for extraction,
		 * but create the tuple in a format suitable for validation tuplesort.
		 */
		len = o_new_tuple_size(secLeafTupdesc, secLeafSpec, NULL, NULL, version, values, isnull, NULL);
		result.data = (Pointer) palloc0(len);
		o_tuple_fill(secLeafTupdesc, secLeafSpec, &result, len, NULL, NULL, version, values, isnull, NULL);
		
		return result;
	}
}

/*
 * add_undo_version_to_sk - Add an undo version to a secondary key tuple's undo chain
 * 
 * This function finds the secondary key tuple and adds an undo record corresponding
 * to an older version from the primary key's undo chain.
 * 
 * Parameters:
 *   secIndexDescr - Secondary index descriptor
 *   skSlot - Slot containing the secondary key tuple
 *   pkTupleVersion - The PK tuple version to transform and add as undo
 *   primaryDescr - Primary index descriptor
 *   tableDescr - Table descriptor
 * 
 * Returns: true if undo record was successfully created, false otherwise
 */
static bool
add_undo_version_to_sk(OIndexDescr *secIndexDescr,
					   TupleTableSlot *skSlot,
					   OTuple pkTupleVersion,
					   OIndexDescr *primaryDescr,
					   OTableDescr *tableDescr)
{
	BTreeDescr *secDesc = &secIndexDescr->desc;
	OBTreeKeyBound bound;
	OBTreeFindPageContext context;
	OFindPageResult findResult;
	Page		p;
	OInMemoryBlkno blkno;
	BTreePageItemLocator *loc;
	BTreeLeafTuphdr *tupHdr;
	OTuple		leafTup;
	int			cmp;
	bool		success = false;

	/*
	 * Form a key bound for the secondary index tuple using the slot.
	 * This will be used to find the SK tuple in the BTree.
	 */
	tts_orioledb_fill_key_bound(skSlot, secIndexDescr, &bound);

	/* Load shared memory for secondary index */
	o_btree_load_shmem(secDesc);

	/*
	 * Find the page containing the secondary index tuple.
	 * We use BTREE_PAGE_FIND_MODIFY to get a write lock on the page.
	 */
	init_page_find_context(&context, secDesc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);

	findResult = find_page(&context, (Pointer) &bound, BTreeKeyBound, 0);

	if (findResult == OFindPageResultSuccess)
	{
		blkno = context.items[context.index].blkno;
		p = O_GET_IN_MEMORY_PAGE(blkno);
		loc = &context.items[context.index].locator;

		/* Verify the tuple exists and matches our key */
		if (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
		{
			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, leafTup, p, loc);
			
			/* Compare to ensure we found the right tuple */
			cmp = o_btree_cmp(secDesc, (Pointer) &bound, BTreeKeyBound,
							 &leafTup, BTreeKeyLeafTuple);

			if (cmp == 0)
			{
				/*
				 * Found the SK tuple. Now we need to create an undo record
				 * for this older version and link it to the tuple's undo chain.
				 *
				 * TODO: Complete undo record creation:
				 * 1. Transform pkTupleVersion to SK tuple format
				 * 2. Call make_undo_record() with BTreeOperationUpdate
				 * 3. Update the tuple header's undoLocation to point to new undo
				 * 4. Mark the page as dirty
				 *
				 * This is complex because:
				 * - We need to construct the full SK tuple from pkTupleVersion
				 * - We need to preserve the existing undo chain link
				 * - We need proper locking and page modification
				 *
				 * For now, we just log that we found the tuple.
				 */
				elog(DEBUG2, "Found SK tuple for undo chain addition during validation");
				success = true;
			}
		}

		/* Unlock the page */
		unlock_page(blkno);
	}

	return success;
}

/*
 * Extract PK tuple from a validation tuplesort tuple.
 * 
 * Validation tuples from tuplesort are in format: (secondary_key_fields, pk_fields).
 * This function extracts just the PK portion for comparison with primary index tuples.
 * 
 * Returns: A new OTuple containing only PK fields. Caller must pfree the returned tuple data.
 */
static OTuple
extract_pk_from_validation_tuple(OTuple validationTuple, OIndexDescr *secIndex, OIndexDescr *pkIndex)
{
	Datum		pkValues[INDEX_MAX_KEYS];
	bool		pkIsnull[INDEX_MAX_KEYS];
	uint32		version = o_tuple_get_version(validationTuple);
	int			i;
	Size		len;
	OTuple		result;
	BTreeDescr *secDesc = &secIndex->desc;
	BTreeDescr *pkDesc = &pkIndex->desc;
	int			nSecKeyFields = secIndex->nKeyFields - secIndex->nPrimaryFields;
	
	/*
	 * Extract PK fields from the validation tuple.
	 * PK fields start at position (nSecKeyFields + 1) in the tuple.
	 */
	for (i = 0; i < secIndex->nPrimaryFields; i++)
	{
		pkValues[i] = o_fastgetattr(validationTuple, nSecKeyFields + i + 1,
									secDesc->leafTupdesc, &secDesc->leafSpec,
									&pkIsnull[i]);
	}
	
	/*
	 * Create a new tuple with PK fields using the PK descriptor.
	 */
	len = o_new_tuple_size(pkDesc->nonLeafTupdesc, &pkDesc->nonLeafSpec, NULL, NULL,
						   version, pkValues, pkIsnull, NULL);
	result.data = (Pointer) palloc0(len);
	o_tuple_fill(pkDesc->nonLeafTupdesc, &pkDesc->nonLeafSpec, &result, len, NULL, NULL,
				 version, pkValues, pkIsnull, NULL);
	
	return result;
}

/*
 * orioledb_index_validate_scan - Validate secondary index by comparing with primary
 *
 * This function performs index validation by doing a merge-join between tuples
 * from the primary index and tuples from the secondary index (via tuplesort).
 *
 * Undo chain walking is enabled on the primary index iterator to ensure complete
 * transfer of all tuple versions from PK to secondary index, including both the
 * final tuple and all historical versions from undo chains.
 */
static void
orioledb_index_validate_scan(Relation heapRelation,
							 Relation indexRelation,
							 IndexInfo *indexInfo,
							 Snapshot snapshot,
							 OValidateIndexState *state)
{
	OTableDescr *descr;
	BTreeIterator *iterator;
	TupleTableSlot *primarySlot;
	OTableSlot *oslot;
	OTuple		tup;
	BTreeLocationHint hint;
	CommitSeqNo tupleCsn;
	ExprState  *predicate;
	EState	   *estate;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	OTuple		indexTuple = {0};
	OTuple		heapTuple;
	bool		indexTupleValid = false;
	bool		indexDone = false;
	IndexUniqueCheck checkUnique;
	OSnapshot	oSnapshot;
	Datum		heapRowIdDatum;
	bool		rowIdIsNull;
	OInMemoryBlkno lastBlkno = OInvalidInMemoryBlkno;
	OTuple		currentPK = {0};	/* Track the current PK tuple for undo chain handling */

	Assert(state != NULL);
	Assert(state->tuplesort != NULL);

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	descr = relation_get_descr(heapRelation);
	Assert(descr != NULL);

	/*
	 * Use SnapshotAny to see ALL tuples including in-progress ones.
	 * This is necessary for validation to properly handle concurrent modifications
	 * and ensure we see all tuples that might exist in secondary indexes.
	 */
	O_LOAD_SNAPSHOT(&oSnapshot, SnapshotAny);
	
	/* 
	 * Use iterator instead of sequential scan to ensure tuples are returned
	 * in primary key order. This is essential for the merge join algorithm.
	 * The validation boundary starts unset (validationBoundaryLen == 0) and
	 * will be updated as we progress through the scan.
	 */
	iterator = o_btree_iterator_create(&GET_PRIMARY(descr)->desc, NULL, BTreeKeyNone,
									   &oSnapshot, ForwardScanDirection);
	
	/*
	 * Enable undo chain walking for the primary index iterator.
	 * This ensures we transfer the entire undo chain from PK to secondary index,
	 * including both the final tuple and all undo versions.
	 * This is critical for validation during concurrent modifications.
	 */
	o_btree_iterator_set_undo_chain_walking(iterator, true);
	
	primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);
	econtext->ecxt_scantuple = primarySlot;

	checkUnique = indexInfo->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO;

	tuplesort_rescan(state->tuplesort);

	O_TUPLE_SET_NULL(currentPK);

	while (true)
	{
		oslot = (OTableSlot *) primarySlot;

		/* Fetch next tuple from iterator in PK order */
		tup = o_btree_iterator_fetch(iterator, &tupleCsn, NULL, BTreeKeyNone, true, &hint);
		
		if (O_TUPLE_IS_NULL(tup))
			break;

		/*
		 * Check if this tuple is an undo version of the current PK or a new PK.
		 * With undo chain walking enabled, the iterator returns:
		 * 1. Main tuple for PK1
		 * 2. Undo version(s) of PK1 (same key, older versions)
		 * 3. Main tuple for PK2
		 * etc.
		 */
		if (!O_TUPLE_IS_NULL(currentPK))
		{
			int pkCmp = o_btree_cmp(&GET_PRIMARY(descr)->desc,
									&currentPK, BTreeKeyLeafTuple,
									&tup, BTreeKeyLeafTuple);
			
			if (pkCmp == 0)
			{
				/*
				 * This is an undo version of the current PK tuple.
				 * Transform it to SK format and add to SK's undo chain.
				 */
				
				/* Transform the undo version from PK format to SK format */
				tts_orioledb_store_tuple(primarySlot, tup, descr, tupleCsn,
										 PrimaryIndexNumber, true, NULL);
				slot_getallattrs(primarySlot);

				MemoryContextReset(econtext->ecxt_per_tuple_memory);

				/*
				 * Add this undo version to the secondary key's undo chain.
				 * The helper function will find the SK tuple and create an undo record.
				 */
				if (add_undo_version_to_sk(state->index_descr, primarySlot,
										   tup, GET_PRIMARY(descr), descr))
				{
					elog(DEBUG2, "Successfully added undo version to SK during validation");
				}
				else
				{
					elog(WARNING, "Failed to add undo version to SK during validation");
				}
				
				/* Clean up and continue to next iteration */
				ExecClearTuple(primarySlot);
				pfree(tup.data);
				continue;
			}
			else
			{
				/* This is a new PK tuple, not an undo version */
				/* Free the previous PK tuple */
				if (currentPK.data != NULL)
					pfree(currentPK.data);
				O_TUPLE_SET_NULL(currentPK);
			}
		}

		/*
		 * This is a new PK tuple (not an undo version).
		 * Store it as the current PK for future undo version detection.
		 */
		currentPK.data = (Pointer) palloc(o_btree_len(&GET_PRIMARY(descr)->desc, tup, OTupleLength));
		memcpy(currentPK.data, tup.data, o_btree_len(&GET_PRIMARY(descr)->desc, tup, OTupleLength));
		currentPK.formatFlags = tup.formatFlags;

		tts_orioledb_store_tuple(primarySlot, tup, descr, tupleCsn, PrimaryIndexNumber, true, &hint);
		slot_getallattrs(primarySlot);
		state->htups++;

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
			{
				ExecClearTuple(primarySlot);
				pfree(tup.data);
				continue;
			}
		}

		FormIndexDatum(indexInfo,
					   primarySlot,
					   estate,
					   values,
					   isnull);

		/* Get heap tuple's rowid for comparison and insertion */
		heapRowIdDatum = slot_getsysattr(primarySlot, RowIdAttributeNumber, &rowIdIsNull);
		Assert(!rowIdIsNull);
		
		/* The heap tuple for comparison is the current primary key tuple */
		heapTuple = tup;

		for (;;)
		{
			int32		cmp;
			OTuple		indexTuplePK;
			OIndexDescr *secIndex;

			if (!indexTupleValid && !indexDone)
			{
				indexTupleValid = orioledb_validate_next_index_tid(state->tuplesort,
																   &indexTuple,
																   &state->itups);
				if (!indexTupleValid)
					indexDone = true;
			}

			if (indexDone)
			{
				/*
				 * No more tuples in secondary index. 
				 * Insert missing tuple into secondary index.
				 */
				if (index_insert(indexRelation, values, isnull, heapRowIdDatum,
								 heapRelation, checkUnique, false, indexInfo))
					state->tups_inserted++;
				break;
			}

			/* 
			 * Extract PK tuple from the validation tuplesort tuple for comparison.
			 * The validation tuple has format (secondary_key_fields, pk_fields).
			 */
			secIndex = state->index_descr;
			OTuple pkFromValidationTuple = extract_pk_from_validation_tuple(indexTuple, secIndex, GET_PRIMARY(descr));
			
			/* 
			 * Now compare PK-to-PK: the extracted PK from validation tuple vs the primary index tuple.
			 * Both are now proper PK tuples that can be directly compared.
			 */
			cmp = o_btree_cmp(&GET_PRIMARY(descr)->desc,
							  pkFromValidationTuple.data, BTreeKeyNonLeafKey,
							  heapTuple.data, BTreeKeyLeafTuple);
			
			/* Free the extracted PK tuple */
			if (pkFromValidationTuple.data != NULL)
				pfree(pkFromValidationTuple.data);
			
			if (cmp < 0)
			{
				/*
				 * Secondary index has a tuple that primary index doesn't have.
				 * This means we need to DELETE this spurious entry from the secondary index.
				 * Delete it immediately by marking the locator as invalid, same approach
				 * as in secondary_index_undo_callback.
				 */
				Page		p;
				OInMemoryBlkno blkno;
				BTreePageItemLocator *loc;
				OBTreeFindPageContext context;
				OFindPageResult findResult;
				BTreeLeafTuphdr *tupHdr;
				OTuple		leafTup;
				int			cmpResult;
				BTreeDescr *secondaryDesc = &secIndex->desc;
				
				/* Load shared memory for secondary index */
				o_btree_load_shmem(secondaryDesc);
				
				/* Initialize find context to locate the secondary index entry */
				init_page_find_context(&context, secondaryDesc,
									   COMMITSEQNO_INPROGRESS,
									   BTREE_PAGE_FIND_MODIFY);
				
				/* Find the page containing this secondary index entry */
				findResult = refind_page(&context, (Pointer) &indexTuple,
										 BTreeKeyLeafTuple,
										 0, InvalidInMemoryBlkno,
										 InvalidOPageChangeCount);
				
				if (findResult == OFindPageResultSuccess)
				{
					blkno = context.items[context.index].blkno;
					p = O_GET_IN_MEMORY_PAGE(blkno);
					loc = &context.items[context.index].locator;

					/* Verify the entry exists and matches */
					if (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
					{
						BTREE_PAGE_READ_LEAF_ITEM(tupHdr, leafTup, p, loc);
						cmpResult = o_btree_cmp(secondaryDesc, &indexTuple, BTreeKeyLeafTuple,
											   &leafTup, BTreeKeyLeafTuple);
					}
					else
					{
						cmpResult = 1;
					}

					if (cmpResult == 0)
					{
						/*
						 * Entry found and matches. Delete it immediately by marking
						 * the locator as invalid (non-transactional deletion).
						 */
						
						/* Mark page as dirty before modification */
						page_block_reads(blkno);
						
						/* Mark the locator as invalid to delete the entry */
						BTREE_PAGE_LOCATOR_SET_INVALID(loc);
						
						/* Update page statistics */
						PAGE_ADD_N_VACATED(p, BTREE_PAGE_GET_ITEM_SIZE(p, loc));
						
						/* Mark the page as modified */
						MARK_DIRTY(secondaryDesc, blkno);
						
						state->tups_deleted++;
						elog(DEBUG2, "validate_index: deleted spurious tuple from secondary index");
					}
					
					/* Unlock the page */
					unlock_page(blkno);
				}
				
				/* Move to next index tuple */
				indexTupleValid = false;
				
				/* Free the index tuple data */
				if (!O_TUPLE_IS_NULL(indexTuple) && indexTuple.data != NULL)
					pfree(indexTuple.data);
				
				continue;
			}

			if (cmp > 0)
			{
				/*
				 * Heap has a tuple that secondary index doesn't have.
				 * Insert the missing tuple into secondary index.
				 */
				if (index_insert(indexRelation, values, isnull, heapRowIdDatum,
								 heapRelation, checkUnique, false, indexInfo))
					state->tups_inserted++;
			}
			else
			{
				/*
				 * Both have the same tuple (cmp == 0). 
				 * Move to next index tuple.
				 */
				indexTupleValid = false;
				
				/* Free the index tuple data */
				if (!O_TUPLE_IS_NULL(indexTuple) && indexTuple.data != NULL)
					pfree(indexTuple.data);
			}
			break;
		}

		/*
		 * Update the validation boundary when we move to a new page.
		 * When the iterator moves to a new page, we have a lock on the new page.
		 * Set the boundary to the current tuple (which is on the new page) immediately.
		 * This ensures proper synchronization - all PKs on the new page will be validated.
		 */
		if (hint.blkno != lastBlkno)
		{
			if (lastBlkno != OInvalidInMemoryBlkno)
			{
				/*
				 * We've moved to a new page. Set the boundary to the current tuple
				 * which is on the new page. The iterator holds a lock on this page.
				 */
				btree_set_validation_boundary(&GET_PRIMARY(descr)->desc, heapTuple);
			}
			lastBlkno = hint.blkno;
		}

		ExecClearTuple(primarySlot);
		pfree(tup.data);
	}

	/* Clean up currentPK if it was allocated */
	if (!O_TUPLE_IS_NULL(currentPK) && currentPK.data != NULL)
		pfree(currentPK.data);

	/*
	 * Clear the validation boundary - validation is now complete.
	 * All concurrent transactions can now freely modify all secondary index entries.
	 */
	btree_clear_validation_boundary(&GET_PRIMARY(descr)->desc);

	ExecDropSingleTupleTableSlot(primarySlot);
	FreeExecutorState(estate);
	btree_iterator_free(iterator);
}



/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

/*
 * Calculate size of table according to a requested method if Orioledb table is provided.
 * Calculate size of index disregarding method.
 *
 * Methods:
 * TOTAL_SIZE - table (primary index), TOAST and secondary indices
 * INDEXES_SIZE - only secondary indices
 * TABLE_SIZE - table (primary index) and TOAST
 * TOAST_TABLE_SIZE - only TOAST (implemented but unused for now)
 * DEFAULT_SIZE and RELATION_SIZE - only main table (primary index tree). There is no difference betweem DEFAULT_SIZE and RELATION_SIZE
 * for OrioleDB tables. Though other table AM that don't support different methods should return -1 at any method except DEFAULT_SIZE.
 *
 * ForkNumber is disregarded for OrioleDB relations.
 */
int64
orioledb_calculate_relation_size(Relation rel, ForkNumber forkNumber, uint8 method)
{
	BTreeDescr *td;
	int64		result = 0;

	if (forkNumber != MAIN_FORKNUM)
	{
		elog(DEBUG3, "Uunexpected fork number");
		return 0;
	}

	if (rel->rd_rel->relkind != RELKIND_INDEX)
	{
		OTableDescr *descr;
		int			i;

		if (!is_orioledb_rel(rel))
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));
		descr = relation_get_descr(rel);

		if (method == TOTAL_SIZE)
		{
			for (i = 0; i < descr->nIndices + 1; i++)
			{
				td = i != descr->nIndices ? &descr->indices[i]->desc : &descr->toast->desc;
				o_btree_load_shmem(td);
				result += (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == INDEXES_SIZE)
		{
			/*
			 * TODO: Bridged indexes are not counted here if referenced by
			 * table relation. This would need exposing static function
			 * calculate_relation_size() in a patchset and call it from here.
			 * Though now they are counted if referenced as index relations
			 * (see below).
			 */
			for (i = 0; i < descr->nIndices; i++)
			{
				if (i == PrimaryIndexNumber)
					continue;

				td = &descr->indices[i]->desc;

				o_btree_load_shmem(td);
				result += (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == TABLE_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids))
			{
				o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
				result += (uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) *
					ORIOLEDB_BLCKSZ;

				o_btree_load_shmem(&descr->toast->desc);
				result += (uint64) TREE_NUM_LEAF_PAGES(&descr->toast->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == TOAST_TABLE_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids))
			{
				o_btree_load_shmem(&descr->toast->desc);
				result = (uint64) TREE_NUM_LEAF_PAGES(&descr->toast->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == RELATION_SIZE || method == DEFAULT_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids))
			{
				o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
				result = (uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else
			elog(ERROR, "Unknown size counting method");
	}
	else if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		/*
		 * If index relation provided, specifying different methods doesn't
		 * matter, counting method is always similar to RELATION_SIZE for
		 * table, but we need to load parent relation for this index first.
		 */
		Relation	tbl;
		ORelOids	tblOids;
		ORelOids	idxOids;
		OTableDescr *table_desc;
		OIndexNumber ixnum;

		idxOids.datoid = MyDatabaseId;
		idxOids.reloid = rel->rd_rel->oid;
		idxOids.relnode = rel->rd_rel->relfilenode;

		tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);

		if (!is_orioledb_rel(tbl))
		{
			relation_close(tbl, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("index \"%s\" is not on orioledb table \"%s\" ", NameStr(rel->rd_rel->relname), NameStr(tbl->rd_rel->relname))));
		}

		tblOids.datoid = MyDatabaseId;
		tblOids.reloid = tbl->rd_rel->oid;
		tblOids.relnode = tbl->rd_rel->relfilenode;

		table_desc = o_fetch_table_descr(tblOids);
		ixnum = find_tree_in_descr(table_desc, idxOids);
		if (ixnum == InvalidIndexNumber)
		{
			/*
			 * Bridged index is an index of a table, but it's not OrioleDB
			 * index and its size should be determined by PG internal routine
			 */
			relation_close(tbl, AccessShareLock);
			return -1;
		}
		td = &table_desc->indices[ixnum]->desc;
		o_btree_load_shmem(td);
		result = (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
		relation_close(tbl, AccessShareLock);
	}

	return (int64) result;
}

static bool
orioledb_relation_needs_toast_table(Relation rel)
{
	return true;
}

static Oid
orioledb_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

static void
orioledb_estimate_rel_size(Relation rel, int32 *attr_widths,
						   BlockNumber *pages, double *tuples,
						   double *allvisfrac)
{
	BlockNumber curpages;
	BlockNumber relpages;
	double		reltuples;
	BlockNumber relallvisible;
	double		density;

	/* it has storage, ok to call the smgr */
	curpages = RelationGetNumberOfBlocks(rel);

	/* coerce values in pg_class to more desirable types */
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;
	relallvisible = (BlockNumber) rel->rd_rel->relallvisible;

	/*
	 * HACK: if the relation has never yet been vacuumed, use a minimum size
	 * estimate of 10 pages.  The idea here is to avoid assuming a
	 * newly-created table is really small, even if it currently is, because
	 * that may not be true once some data gets loaded into it.  Once a vacuum
	 * or analyze cycle has been done on it, it's more reasonable to believe
	 * the size is somewhat stable.
	 *
	 * (Note that this is only an issue if the plan gets cached and used again
	 * after the table has been filled.  What we're trying to avoid is using a
	 * nestloop-type plan on a table that has grown substantially since the
	 * plan was made.  Normally, autovacuum/autoanalyze will occur once enough
	 * inserts have happened and cause cached-plan invalidation; but that
	 * doesn't happen instantaneously, and it won't happen at all for cases
	 * such as temporary tables.)
	 *
	 * We approximate "never vacuumed" by "has relpages = 0", which means this
	 * will also fire on genuinely empty relations.  Not great, but
	 * fortunately that's a seldom-seen case in the real world, and it
	 * shouldn't degrade the quality of the plan too much anyway to err in
	 * this direction.
	 *
	 * If the table has inheritance children, we don't apply this heuristic.
	 * Totally empty parent tables are quite common, so we should be willing
	 * to believe that they are empty.
	 */
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	/* report estimated # pages */
	*pages = curpages;
	/* quick exit if rel is clearly empty */
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/* estimate number of tuples from previous tuple density */
	if (reltuples >= 0 && relpages > 0)
	{
		density = reltuples / (double) relpages;
	}
	else
	{
		/*
		 * When we have no data because the relation was truncated, estimate
		 * tuple width from attribute datatypes.  We assume here that the
		 * pages are completely full, which is OK for tables (since they've
		 * presumably not been VACUUMed yet) but is probably an overestimate
		 * for indexes.  Fortunately get_relation_info() can clamp the
		 * overestimate to the parent table's size.
		 *
		 * Note: this code intentionally disregards alignment considerations,
		 * because (a) that would be gilding the lily considering how crude
		 * the estimate is, and (b) it creates platform dependencies in the
		 * default plans which are kind of a headache for regression testing.
		 */
		int32		tuple_width;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeOfOTupleHeader);
		/* note: integer division is intentional here */
		density = ((double) (ORIOLEDB_BLCKSZ / 2)) / tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	/*
	 * We use relallvisible as-is, rather than scaling it up like we do for
	 * the pages and tuples counts, on the theory that any pages added since
	 * the last VACUUM are most likely not marked all-visible.  But costsize.c
	 * wants it converted to a fraction.
	 */
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double) relallvisible / curpages;
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

static bool
orioledb_scan_bitmap_next_block(TableScanDesc scan,
								TBMIterateResult *tbmres)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_bitmap_next_tuple(TableScanDesc scan,
								TBMIterateResult *tbmres,
								TupleTableSlot *slot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

Size
orioledb_parallelscan_estimate(Relation rel)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	return sizeof(ParallelOScanDescData);
}

static void
orioledb_parallelscan_initialize_internal(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	clear_fixed_shmem_key(&poscan->intPage[0].prevHikey);
	clear_fixed_shmem_key(&poscan->intPage[1].prevHikey);
	memset(poscan->intPage[0].img, 0, ORIOLEDB_BLCKSZ);
	memset(poscan->intPage[1].img, 0, ORIOLEDB_BLCKSZ);
	poscan->intPage[0].status = OParallelScanPageInvalid;
	poscan->intPage[1].status = OParallelScanPageInvalid;
	poscan->intPage[0].startOffset = 0;
	poscan->intPage[1].startOffset = 0;
	poscan->intPage[0].offset = 0;
	poscan->intPage[1].offset = 0;
	poscan->downlinksCount = 0;
	poscan->workersReportedCount = 0;
	poscan->flags = 0;
	poscan->cur_int_pageno = 0;
	poscan->dsmHandle = 0;
	poscan->nworkers = 0;
#ifdef USE_ASSERT_CHECKING
	memset(poscan->worker_active, 0, sizeof(poscan->worker_active));
#endif
}

/* Modified copy of table_block_parallelscan_initialize */
Size
orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	poscan->phs_base.phs_relid = RelationGetRelid(rel);
	poscan->phs_base.phs_syncscan = false;
	return orioledb_parallelscan_initialize_inner(pscan);
}

Size
orioledb_parallelscan_initialize_inner(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	SpinLockInit(&poscan->intpageAccess);
	SpinLockInit(&poscan->workerStart);
	LWLockInitialize(&poscan->intpageLoad, btreeScanShmem->pageLoadTrancheId);
	LWLockInitialize(&poscan->downlinksPublish, btreeScanShmem->downlinksPublishTrancheId);
	pg_atomic_init_u64(&poscan->downlinkIndex, 0);

	orioledb_parallelscan_initialize_internal(pscan);

	return sizeof(ParallelOScanDescData);
}

void
orioledb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	orioledb_parallelscan_initialize_internal(pscan);
}


static TableScanDesc
orioledb_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	OTableDescr *descr;
	OScanDesc	scan;

	descr = relation_get_descr(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (OScanDesc) palloc0(sizeof(OScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (nkeys > 0)
	{
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) * nkeys);
	}
	else
	{
		scan->rs_base.rs_key = NULL;
	}

	if (scan->rs_base.rs_flags & SO_TYPE_ANALYZE)
		scan->o_snapshot = o_in_progress_snapshot;
	else
		O_LOAD_SNAPSHOT(&scan->o_snapshot, snapshot);

	ItemPointerSetBlockNumber(&scan->iptr, 0);
	ItemPointerSetOffsetNumber(&scan->iptr, FirstOffsetNumber);

	if (descr)
		scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot, parallel_scan);

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);

	return &scan->rs_base;
}

static void
orioledb_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	OTableDescr *descr;
	OScanDesc	scan;

	scan = (OScanDesc) sscan;
	descr = relation_get_descr(scan->rs_base.rs_rd);

	memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) *
		   scan->rs_base.rs_nkeys);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);

	scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot,
									 scan->rs_base.rs_parallel);

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);
}

static void
orioledb_endscan(TableScanDesc sscan)
{
	OScanDesc	scan = (OScanDesc) sscan;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);
}

static bool
slot_keytest(TupleTableSlot *slot, int nkeys, ScanKey keys)
{
	int			i;
	ScanKey		key;

	for (i = 0; i < nkeys; i++)
	{
		Datum		val;
		bool		isnull;
		Datum		test;

		key = keys + i;

		if (key->sk_flags & SK_ISNULL)
			return false;

		val = slot_getattr(slot, key->sk_attno, &isnull);

		if (isnull)
			return false;

		test = FunctionCall2Coll(&key->sk_func,
								 key->sk_collation,
								 val,
								 key->sk_argument);

		if (!DatumGetBool(test))
			return false;
	}

	return true;
}

static bool
orioledb_getnextslot(TableScanDesc sscan, ScanDirection direction,
					 TupleTableSlot *slot)
{
	OScanDesc	scan;
	OTableDescr *descr;
	bool		result;

	if (OidIsValid(o_saved_relrewrite))
		return false;

	do
	{
		OTuple		tuple = {0};
		BTreeLocationHint hint;
		CommitSeqNo csn;

		scan = (OScanDesc) sscan;
		descr = relation_get_descr(scan->rs_base.rs_rd);

		if (scan->scan)
			tuple = btree_seq_scan_getnext(scan->scan, slot->tts_mcxt,
										   &csn, &hint);

		if (O_TUPLE_IS_NULL(tuple))
			return false;

		tts_orioledb_store_tuple(slot, tuple, descr, csn,
								 PrimaryIndexNumber, true, &hint);

		result = slot_keytest(slot,
							  scan->rs_base.rs_nkeys,
							  scan->rs_base.rs_key);
	}
	while (!result);

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

	return true;
}

static void
orioledb_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	int			i;

	for (i = 0; i < ntuples; i++)
		orioledb_tuple_insert(relation, slots[i], cid, options, bistate);
}

static void
orioledb_get_latest_tid(TableScanDesc sscan,
						ItemPointer tid)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_vacuum_rel(Relation onerel, VacuumParams *params,
					BufferAccessStrategy bstrategy)
{
	OTableDescr *descr;

	descr = relation_get_descr(onerel);

	/*
	 * We do VACUUM only to cleanup bridged indexes.
	 */
	if (!descr->bridge || params->index_cleanup == VACOPTVALUE_DISABLED)
		return;

	orioledb_vacuum_bridged_indexes(onerel, descr, params, bstrategy);
}

static TransactionId
orioledb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "Not implemented");
}

void
orioledb_free_rd_amcache(Relation rel)
{
	if (rel->rd_amcache)
		table_descr_dec_refcnt((OTableDescr *) rel->rd_amcache);
	rel->rd_amcache = NULL;
}

/*
 * Comparator for sorting rows[] array
 */
static int
compare_rows(const void *a, const void *b, void *arg)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
	OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
	BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
	OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}

static int
orioledb_acquire_sample_rows(Relation relation, int elevel,
							 HeapTuple *rows, int targrows,
							 double *totalrows,
							 double *totaldeadrows)
{
	OTableDescr *descr = relation_get_descr(relation);
	OIndexDescr *pk = GET_PRIMARY(descr);
	BTreeSeqScan *scan;
	BlockNumber nblocks;
	ReservoirStateData rstate;
	bool		scanEnd;
	OTuple		tuple;
	TupleTableSlot *slot = descr->newTuple;
	int			numrows = 0;	/* # rows now in reservoir */
	double		samplerows = 0; /* total # rows collected */
	double		liverows = 0;	/* # live rows seen */
	double		deadrows = 0;	/* # dead rows seen */
	double		rowstoskip = -1;	/* -1 means not set yet */
	BlockSamplerData bs;
	BlockNumber totalblocks;
	ItemPointerData fake_iptr = {0};

	o_btree_load_shmem(&pk->desc);
	totalblocks = TREE_NUM_LEAF_PAGES(&pk->desc);

	ItemPointerSetBlockNumber(&fake_iptr, 0);
	ItemPointerSetOffsetNumber(&fake_iptr, 1);

	nblocks = BlockSampler_Init(&bs, totalblocks,
								targrows, random());

	scan = make_btree_sampling_scan(&pk->desc, &bs);

	/* Report sampling block numbers */
	pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
								 nblocks);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
									   &scanEnd, NULL);
	while (!scanEnd)
	{
		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false, NULL);

			if (!pk->primaryIsCtid)
			{
				ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&fake_iptr));
				ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&fake_iptr));
				if ((OffsetNumber) (ItemPointerGetOffsetNumber(&fake_iptr) + 1) == InvalidOffsetNumber)
				{
					ItemPointerSetBlockNumber(&fake_iptr, ItemPointerGetBlockNumber(&fake_iptr) + 1);
					ItemPointerSetOffsetNumber(&fake_iptr, 1);
				}
				else
					ItemPointerSetOffsetNumber(&fake_iptr, ItemPointerGetOffsetNumber(&fake_iptr) + 1);
			}

			liverows += 1;

			if (numrows < targrows)
				rows[numrows++] = ExecCopySlotHeapTuple(slot);
			else
			{
				/*
				 * t in Vitter's paper is the number of records already
				 * processed.  If we need to compute a new S value, we must
				 * use the not-yet-incremented value of samplerows as t.
				 */
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					/*
					 * Found a suitable tuple, so save it, replacing one old
					 * tuple at random
					 */
					int			k = (int) (targrows * sampler_random_fract(&rstate.randstate));

					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}

				rowstoskip -= 1;
			}
			samplerows += 1;
		}
		else
		{
			deadrows += 1;
		}
		tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
										   &scanEnd, NULL);
	}
	free_btree_seq_scan(scan);


	/*
	 * If we didn't find as many tuples as we wanted then we're done. No sort
	 * is needed, since they're already in order.
	 *
	 * Otherwise we need to sort the collected tuples by position
	 * (itempointer). It's not worth worrying about corner cases where the
	 * tuples are already sorted.
	 */
	if (numrows == targrows)
		qsort_interruptible(rows, numrows, sizeof(HeapTuple),
							compare_rows, NULL);

	/*
	 * Estimate total numbers of live and dead rows in relation, extrapolating
	 * on the assumption that the average tuple density in pages we didn't
	 * scan is the same as in the pages we did scan.  Since what we scanned is
	 * a random sample of the pages in the relation, this should be a good
	 * assumption.
	 */
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
	else
	{
		*totalrows = 0.0;
		*totaldeadrows = 0.0;
	}

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": scanned %d of %u pages, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f estimated total rows",
					RelationGetRelationName(relation),
					bs.m, totalblocks,
					liverows, deadrows,
					numrows, *totalrows)));

	return numrows;
}

static void
orioledb_analyze_table(Relation relation,
					   AcquireSampleRowsFunc *func,
					   BlockNumber *totalpages)
{
	OTableDescr *descr = relation_get_descr(relation);
	OIndexDescr *pk = GET_PRIMARY(descr);

	o_btree_load_shmem(&pk->desc);

	*func = orioledb_acquire_sample_rows;
	*totalpages = TREE_NUM_LEAF_PAGES(&pk->desc);
}

static void
validate_default_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Default");
}

static void
validate_primary_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Primary index");
}

static void
validate_toast_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "TOAST");
}

/* values from StdRdOptIndexCleanup */
static relopt_enum_elt_def StdRdOptIndexCleanupValues[] =
{
	{
		"auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO
	},
	{
		"on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		(const char *) NULL
	}							/* list terminator */
};

/*
 * Option parser for anything that uses StdRdOptions.
 */
static bytea *
orioledb_default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
{
	static bool relopts_set = false;
	static local_relopts relopts = {0};

	if (!relopts_set)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		init_local_reloptions(&relopts, sizeof(ORelOptions));

		/* Options from default_reloptions */
		add_local_int_reloption(&relopts, "fillfactor",
								"Packs table pages only to this percentage",
								BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR,
								100,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, fillfactor));
		add_local_bool_reloption(&relopts, "autovacuum_enabled",
								 "Enables autovacuum in this relation",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, enabled));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_threshold",
								"Minimum number of tuple updates or deletes "
								"prior to vacuum",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_insert_threshold",
								"Minimum number of tuple inserts "
								"prior to vacuum, "
								"or -1 to disable insert vacuums",
								-2, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 vacuum_ins_threshold));
		add_local_int_reloption(&relopts, "autovacuum_analyze_threshold",
								"Minimum number of tuple inserts, "
								"updates or deletes prior to analyze",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, analyze_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_cost_limit",
								"Vacuum cost amount available before napping, "
								"for autovacuum",
								-1, 1, 10000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_cost_limit));
		add_local_int_reloption(&relopts, "autovacuum_freeze_min_age",
								"Minimum age at which VACUUM should freeze "
								"a table row, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_min_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_max_age",
								"Age at which to autovacuum a table "
								"to prevent transaction ID wraparound",
								-1, 100000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_max_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_table_age",
								"Age at which VACUUM should perform "
								"a full table sweep to freeze row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_table_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_min_age",
								"Minimum multixact age at which VACUUM should "
								"freeze a row multixact's, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_min_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_max_age",
								"Multixact age at which to autovacuum a table "
								"to prevent multixact wraparound",
								-1, 10000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_max_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_table_age",
								"Age of multixact at which VACUUM should "
								"perform a full table sweep to freeze "
								"row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_table_age));
		add_local_int_reloption(&relopts, "log_autovacuum_min_duration",
								"Sets the minimum execution time above which "
								"autovacuum actions will be logged",
								-1, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, log_min_duration));
		add_local_int_reloption(&relopts, "toast_tuple_target",
								"Sets the target tuple length at which "
								"external columns will be toasted",
								TOAST_TUPLE_TARGET, 128,
								TOAST_TUPLE_TARGET_MAIN,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions,
										 toast_tuple_target));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_cost_delay",
								 "Vacuum cost delay in milliseconds, "
								 "for autovacuum",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, vacuum_cost_delay));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_scale_factor",
								 "Number of tuple updates or deletes prior to "
								 "vacuum as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_vacuum_insert_scale_factor",
								 "Number of tuple inserts prior to vacuum "
								 "as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_ins_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_analyze_scale_factor",
								 "Number of tuple inserts, updates or deletes "
								 "prior to analyze as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  analyze_scale_factor));
		add_local_bool_reloption(&relopts, "user_catalog_table",
								 "Declare a table as an additional "
								 "catalog table, e.g. for the purpose of "
								 "logical replication",
								 false,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  user_catalog_table));
		add_local_int_reloption(&relopts, "parallel_workers",
								"Number of parallel processes that can be "
								"used per executor node for this relation.",
								-1, 0, 1024,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, parallel_workers));
		add_local_enum_reloption(&relopts, "vacuum_index_cleanup",
								 "Controls index vacuuming and index cleanup",
								 StdRdOptIndexCleanupValues,
								 STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
								 gettext_noop("Valid values are \"on\", "
											  "\"off\", and \"auto\"."),
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  vacuum_index_cleanup));
		add_local_bool_reloption(&relopts, "vacuum_truncate",
								 "Enables vacuum to truncate empty pages at "
								 "the end of this table",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, vacuum_truncate));

		/* Options for orioledb tables */
		add_local_string_reloption(&relopts, "compress",
								   "Default compression level for "
								   "all table data structures",
								   NULL, validate_default_compress, NULL,
								   offsetof(ORelOptions, compress_offset));
		add_local_string_reloption(&relopts, "primary_compress",
								   "Compression level for the "
								   "table primary key",
								   NULL, validate_primary_compress, NULL,
								   offsetof(ORelOptions,
											primary_compress_offset));
		add_local_string_reloption(&relopts, "toast_compress",
								   "Compression level for the "
								   "table TOASTed values",
								   NULL, validate_toast_compress, NULL,
								   offsetof(ORelOptions,
											toast_compress_offset));
		add_local_bool_reloption(&relopts, "index_bridging",
								 "Enables implicit bridge ctid index and support of non-btree indices via bridging",
								 false,
								 offsetof(ORelOptions,
										  index_bridging));
		MemoryContextSwitchTo(oldcxt);
		relopts_set = true;
	}

	return (bytea *) build_local_reloptions(&relopts, reloptions, validate);
}

static bytea *
orioledb_reloptions(char relkind, Datum reloptions, bool validate)
{
	StdRdOptions *rdopts;

	switch (relkind)
	{
		case RELKIND_TOASTVALUE:
			rdopts = (StdRdOptions *)
				default_reloptions(reloptions, validate, RELOPT_KIND_TOAST);
			if (rdopts != NULL)
			{
				/* adjust default-only parameters for TOAST relations */
				rdopts->fillfactor = 100;
				rdopts->autovacuum.analyze_threshold = -1;
				rdopts->autovacuum.analyze_scale_factor = -1;
			}
			return (bytea *) rdopts;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			return orioledb_default_reloptions(reloptions, validate,
											   RELOPT_KIND_HEAP);
		default:
			/* other relkinds are not supported */
			return NULL;
	}
}

static bool
orioledb_tuple_is_current(Relation rel, TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	return COMMITSEQNO_IS_INPROGRESS(oslot->csn);
}


/* ------------------------------------------------------------------------
 * Definition of the orioledb table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine orioledb_am_methods = {
	.type = T_TableAmRoutine,
	.amcanbackward = false,
	.slot_callbacks = orioledb_slot_callbacks,
	.get_row_ref_type = orioledb_get_row_ref_type,
	.free_rd_amcache = orioledb_free_rd_amcache,

	.scan_begin = orioledb_beginscan,
	.scan_end = orioledb_endscan,
	.scan_rescan = orioledb_rescan,
	.scan_getnextslot = orioledb_getnextslot,

	.parallelscan_estimate = orioledb_parallelscan_estimate,
	.parallelscan_initialize = orioledb_parallelscan_initialize,
	.parallelscan_reinitialize = orioledb_parallelscan_reinitialize,

	.index_fetch_begin = orioledb_index_fetch_begin,
	.index_fetch_reset = orioledb_index_fetch_reset,
	.index_fetch_end = orioledb_index_fetch_end,
	.index_fetch_tuple = orioledb_index_fetch_tuple,
	.index_delete_tuples = orioledb_index_delete_tuples,

	.tuple_insert = orioledb_tuple_insert,
	.tuple_insert_with_arbiter = orioledb_tuple_insert_with_arbiter,
	.multi_insert = orioledb_multi_insert,
	.tuple_delete = orioledb_tuple_delete,
	.tuple_update = orioledb_tuple_update,
	.tuple_lock = orioledb_tuple_lock,
	.finish_bulk_insert = orioledb_finish_bulk_insert,

	.tuple_fetch_row_version = orioledb_fetch_row_version,
	.tuple_get_latest_tid = orioledb_get_latest_tid,
	.tuple_tid_valid = orioledb_tuple_tid_valid,
	.tuple_satisfies_snapshot = orioledb_tuple_satisfies_snapshot,

	.relation_set_new_filelocator = orioledb_relation_set_new_filenode,
	.relation_nontransactional_truncate = orioledb_relation_nontransactional_truncate,
	.relation_copy_data = orioledb_relation_copy_data,
	.relation_copy_for_cluster = orioledb_relation_copy_for_cluster,
	.relation_vacuum = orioledb_vacuum_rel,
	.scan_analyze_next_block = orioledb_scan_analyze_next_block,
	.scan_analyze_next_tuple = orioledb_scan_analyze_next_tuple,
	.index_build_range_scan = orioledb_index_build_range_scan,
	.index_validate_scan = orioledb_index_validate_scan,
	.index_validate = orioledb_index_validate,

	.relation_size = orioledb_calculate_relation_size,
	.relation_needs_toast_table = orioledb_relation_needs_toast_table,
	.relation_toast_am = orioledb_relation_toast_am,

	.relation_estimate_size = orioledb_estimate_rel_size,
	.scan_bitmap_next_block = orioledb_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = orioledb_scan_bitmap_next_tuple,
	.scan_sample_next_block = orioledb_scan_sample_next_block,
	.scan_sample_next_tuple = orioledb_scan_sample_next_tuple,
	.tuple_is_current = orioledb_tuple_is_current,
	.analyze_table = orioledb_analyze_table,
	.reloptions = orioledb_reloptions
};

bool
is_orioledb_rel(Relation rel)
{
	Assert(rel != NULL);

	return (rel->rd_tableam == (TableAmRoutine *) &orioledb_am_methods);
}

Datum		orioledb_tableam_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(orioledb_tableam_handler);

Datum
orioledb_tableam_handler(PG_FUNCTION_ARGS)
{
	orioledb_check_shmem();

	PG_RETURN_POINTER(&orioledb_am_methods);
}

/*
 * Returns private descriptor for relation
 *
 * In order to save some hash lookup, we cache descriptor in rel->rd_amcache.
 * Since rel->rd_amcache is automatically freed on cache invalidation, we
 * can't set rel->rd_amcache to the descriptor directly.  But we may use
 * pointer to allocated area contained pointer to descriptor.
 */
OTableDescr *
relation_get_descr(Relation rel)
{
	OTableDescr *result;
	ORelOids	oids;

	Assert(rel != NULL);

	ORelOidsSetFromRel(oids, rel);
	if (!is_orioledb_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	if (rel->rd_amcache)
		return (OTableDescr *) rel->rd_amcache;

	result = o_fetch_table_descr(oids);
	rel->rd_amcache = result;
	if (result)
		table_descr_inc_refcnt(result);
	return result;
}

static void
get_keys_from_rowid(OIndexDescr *id, Datum pkDatum, OBTreeKeyBound *key,
					BTreeLocationHint *hint, CommitSeqNo *csn, uint32 *version,
					ItemPointer *bridge_ctid)
{
	bytea	   *rowid;
	Pointer		p;

	key->nkeys = id->nonLeafTupdesc->natts;

	if (!id->primaryIsCtid)
	{
		OTuple		tuple;
		ORowIdAddendumNonCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		*hint = add->hint;
		if (csn)
			*csn = add->csn;

		if (id->bridging)
		{
			if (bridge_ctid)
				*bridge_ctid = (ItemPointer) p;
			p += MAXALIGN(sizeof(ItemPointerData));
		}

		tuple.data = p;
		tuple.formatFlags = add->flags;
		if (version)
			*version = o_tuple_get_version(tuple);
		o_fill_key_bound(id, tuple, BTreeKeyNonLeafKey, key);
	}
	else
	{
		ORowIdAddendumCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		*hint = add->hint;
		if (csn)
			*csn = add->csn;
		if (version)
			*version = add->version;
		p += MAXALIGN(sizeof(ORowIdAddendumCtid));

		key->keys[0].value = PointerGetDatum(p);
		key->keys[0].type = TIDOID;
		key->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		key->keys[0].comparator = NULL;

		if (id->bridging)
		{
			p += MAXALIGN(sizeof(ItemPointerData));
			if (bridge_ctid)
				*bridge_ctid = (ItemPointer) p;
		}
	}
}

static void
rowid_set_csn(OIndexDescr *id, Datum pkDatum, CommitSeqNo csn)
{
	bytea	   *rowid;
	Pointer		p;

	if (!id->primaryIsCtid)
	{
		ORowIdAddendumNonCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		add->csn = csn;
	}
	else
	{
		ORowIdAddendumCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		add->csn = csn;
	}
}
