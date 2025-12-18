/*-------------------------------------------------------------------------
 *
 * build.c
 *		Routines for sort-based B-tree index building.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/build.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/build.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/split.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "s3/worker.h"
#include "tableam/descr.h"
#include "tuple/toast.h"
#include "tuple/sort.h"
#include "transam/oxid.h"
#include "utils/seq_buf.h"

#include "access/genam.h"
#include "access/relation.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"

typedef struct OIndexBuildStackItem
{
	char		img[ORIOLEDB_BLCKSZ];
	BTreePageItemLocator loc;
	OFixedKey	key;
	int			keysize;
} OIndexBuildStackItem;

struct OBTreeBuildState
{
	BTreeDescr *desc;
	OIndexBuildStackItem *stack;
	int			root_level;
	BTreeMetaPage metaPageBlkno;
	bool		finished;
};

static bool put_item_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
							  int level, OTuple tuple, int tuplesize,
							  Pointer tupleheader, LocationIndex header_size,
							  int *root_level, BTreeMetaPage *metaPageBlkno);
static bool put_tuple_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
							   OTuple tuple, int *root_level,
							   BTreeMetaPage *metaPageBlkno);
static bool put_downlink_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
								  int level, uint64 downlink, OTuple key,
								  int keysize, int *root_level,
								  BTreeMetaPage *metaPageBlkno);

static void
stack_page_split(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
				 OTuple tuple, int tuplesize, Pointer tupleheader,
				 LocationIndex header_size, Page new_page)
{
	Page		img = stack[level].img;
	OffsetNumber left_count,
				rightbound_key_size;
	bool		key_palloc = false;
	Pointer		tuple_ptr;
	OTuple		rightbound_key;
	bool		leaf = O_PAGE_IS(img, LEAF);
	BTreePageItemLocator loc,
				newLoc;
	BTreeSplitItems items;
	OffsetNumber offset;

	btree_page_update_max_key_len(desc, img);
	offset = BTREE_PAGE_LOCATOR_GET_OFFSET(img, &stack[level].loc);

	make_split_items(desc, img, &items, &offset,
					 tupleheader, tuple, tuplesize, false,
					 COMMITSEQNO_INPROGRESS);

	left_count = btree_page_split_location(desc, &items, offset, 0.9, NULL);

	/* Distribute the tuples according the the split location */
	BTREE_PAGE_OFFSET_GET_LOCATOR(img, left_count, &loc);
	BTREE_PAGE_LOCATOR_FIRST(new_page, &newLoc);
	while (BTREE_PAGE_LOCATOR_IS_VALID(img, &loc))
	{
		LocationIndex itemsize;

		itemsize = BTREE_PAGE_GET_ITEM_SIZE(img, &loc);

		page_locator_insert_item(new_page, &newLoc, itemsize);
		memcpy(BTREE_PAGE_LOCATOR_GET_ITEM(new_page, &newLoc),
			   BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc),
			   itemsize);
		BTREE_PAGE_SET_ITEM_FLAGS(new_page, &newLoc, BTREE_PAGE_GET_ITEM_FLAGS(stack[level].img, &loc));

		BTREE_PAGE_LOCATOR_NEXT(img, &loc);
		BTREE_PAGE_LOCATOR_NEXT(new_page, &newLoc);
	}

	BTREE_PAGE_LOCATOR_TAIL(new_page, &newLoc);
	page_locator_insert_item(new_page, &newLoc,
							 MAXALIGN(tuplesize) + header_size);
	tuple_ptr = BTREE_PAGE_LOCATOR_GET_ITEM(new_page, &newLoc);
	memcpy(tuple_ptr, tupleheader, header_size);
	tuple_ptr += header_size;
	memcpy(tuple_ptr, tuple.data, tuplesize);
	BTREE_PAGE_SET_ITEM_FLAGS(new_page, &newLoc, tuple.formatFlags);

	/* Setup the new high key on the left page */
	BTREE_PAGE_LOCATOR_FIRST(new_page, &newLoc);
	BTREE_PAGE_READ_TUPLE(rightbound_key, new_page, &newLoc);
	if (leaf)
	{
		rightbound_key = o_btree_tuple_make_key(desc, rightbound_key, NULL, false, &key_palloc);
		rightbound_key_size = o_btree_len(desc, rightbound_key, OKeyLength);
	}
	else
	{
		rightbound_key_size = BTREE_PAGE_GET_ITEM_SIZE(new_page, &newLoc) -
			header_size;
	}

	btree_page_reorg(desc, img, &items.items[0], left_count,
					 rightbound_key_size, rightbound_key);

	if (key_palloc)
		pfree(rightbound_key.data);
}

static bool
put_item_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
				  OTuple tuple, int tuplesize, Pointer tupleheader,
				  LocationIndex header_size, int *root_level,
				  BTreeMetaPage *metaPageBlkno)
{
	BTreeItemPageFitType fit;
	Pointer		tuple_ptr;
	uint64		downlink = 0;

	Assert(level < ORIOLEDB_MAX_DEPTH);

	if (BTREE_PAGE_FREE_SPACE(stack[level].img) - MAXALIGN(tuplesize) - header_size >=
		ORIOLEDB_BLCKSZ * (100 - desc->fillfactor) / 100)
		fit = page_locator_fits_item(desc,
									 stack[level].img,
									 &stack[level].loc,
									 MAXALIGN(tuplesize) + header_size,
									 false,
									 COMMITSEQNO_INPROGRESS);
	else
		fit = BTreeItemPageFitSplitRequired;

	if (fit == BTreeItemPageFitAsIs)
	{
		page_locator_insert_item(stack[level].img, &stack[level].loc,
								 MAXALIGN(tuplesize) + header_size);
		tuple_ptr = BTREE_PAGE_LOCATOR_GET_ITEM(stack[level].img, &stack[level].loc);
		memcpy(tuple_ptr, tupleheader, header_size);
		tuple_ptr += header_size;
		memcpy(tuple_ptr, tuple.data, tuplesize);
		BTREE_PAGE_SET_ITEM_FLAGS(stack[level].img, &stack[level].loc, tuple.formatFlags);

		BTREE_PAGE_LOCATOR_NEXT(stack[level].img, &stack[level].loc);
	}
	else
	{
		FileExtent	extent;
		char		new_page[ORIOLEDB_BLCKSZ] = {0};
		OFixedKey	key;
		int			keysize;
		BTreePageHeader *new_page_header = (BTreePageHeader *) new_page;
		BTreePageHeader *header = (BTreePageHeader *) stack[level].img;
		BTreePageHeader *parent_header = (BTreePageHeader *) stack[level + 1].img;

		new_page_header->rightLink = InvalidRightLink;
		new_page_header->csn = COMMITSEQNO_FROZEN;
		new_page_header->undoLocation = InvalidUndoLocation;
		new_page_header->o_header.checkpointNum = 0;
		new_page_header->prevInsertOffset = MaxOffsetNumber;

		new_page_header->flags = O_BTREE_FLAG_RIGHTMOST;

		if (level == 0)
			new_page_header->flags |= O_BTREE_FLAG_LEAF;
		else
			PAGE_SET_LEVEL(new_page, level);

		init_page_first_chunk(desc, new_page, 0);

		header->rightLink = InvalidRightLink;
		header->csn = COMMITSEQNO_FROZEN;
		header->undoLocation = InvalidUndoLocation;
		header->o_header.checkpointNum = 0;
		header->prevInsertOffset = MaxOffsetNumber;

		header->flags &= ~O_BTREE_FLAG_RIGHTMOST;

		if (level == 0)
			header->flags |= O_BTREE_FLAG_LEAF;

		stack_page_split(desc, stack, level, tuple, tuplesize, tupleheader,
						 header_size, new_page);

		if (level == *root_level)
		{
			parent_header->flags = O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST;
			header->flags |= O_BTREE_FLAG_LEFTMOST;
			if (level != 0)
				PAGE_SET_LEVEL(stack[level].img, level);

			*root_level = level + 1;
		}

		if (level != 0)
			PAGE_SET_N_ONDISK(stack[level].img,
							  BTREE_PAGE_ITEMS_COUNT(stack[level].img));

		/* write old page to disk */

		extent.len = InvalidFileExtentLen;
		extent.off = InvalidFileExtentOff;

		VALGRIND_CHECK_MEM_IS_DEFINED(stack[level].img, ORIOLEDB_BLCKSZ);

		downlink = perform_page_io_build(desc, stack[level].img, &extent, metaPageBlkno);
		if (level == 0)
			pg_atomic_add_fetch_u32(&metaPageBlkno->leafPagesNum, 1);

		copy_fixed_key(desc, &key, stack[level].key.tuple);
		keysize = stack[level].keysize;

		stack[level].keysize = BTREE_PAGE_GET_HIKEY_SIZE(stack[level].img);
		copy_fixed_hikey(desc, &stack[level].key, stack[level].img);

		if (level > 0)
		{
#ifdef ORIOLEDB_CUT_FIRST_KEY
			page_cut_first_key(new_page);
#endif
		}

		/* copy new page to stack */
		memcpy(stack[level].img, new_page, ORIOLEDB_BLCKSZ);
		BTREE_PAGE_LOCATOR_TAIL(stack[level].img, &stack[level].loc);

		put_downlink_to_stack(desc, stack, level + 1, downlink,
							  key.tuple, keysize,
							  root_level, metaPageBlkno);
	}
	return true;
}

static bool
put_downlink_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
					  uint64 downlink, OTuple key, int keysize,
					  int *root_level, BTreeMetaPage *metaPageBlkno)
{
	BTreeNonLeafTuphdr internal_header = {0};
	bool		result;

	internal_header.downlink = downlink;
	result = put_item_to_stack(desc, stack, level, key, keysize,
							   (Pointer) &internal_header,
							   sizeof(internal_header), root_level,
							   metaPageBlkno);
	return result;
}

static bool
put_tuple_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
				   OTuple tuple, int *root_level, BTreeMetaPage *metaPageBlkno)
{
	BTreeLeafTuphdr leaf_header = {0};
	int			tuplesize;

	leaf_header.deleted = BTreeLeafTupleNonDeleted;
	leaf_header.undoLocation = InvalidUndoLocation;
	leaf_header.xactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, RowLockUpdate, false);
	tuplesize = o_btree_len(desc, tuple, OTupleLength);
	return put_item_to_stack(desc, stack, 0,
							 tuple, tuplesize, (Pointer) &leaf_header,
							 sizeof(leaf_header), root_level, metaPageBlkno);
}

void
btree_write_index_data(BTreeDescr *desc, TupleDesc tupdesc,
					   Tuplesortstate *sortstate, uint64 ctid,
					   uint64 bridge_ctid, CheckpointFileHeader *file_header)
{
	OTuple		idx_tup;
	OBTreeBuildState *state;

	/*
	 * tupdesc is maintained for API compatibility and future-proofing; callers
	 * already provide the leaf tuple descriptor alongside the tuplesort even
	 * though it is not required by the streaming builder today.
	 */
	(void) tupdesc;

	state = btree_build_state_start(desc, ctid, bridge_ctid);

	idx_tup = tuplesort_getotuple(sortstate, true);
	while (!O_TUPLE_IS_NULL(idx_tup))
	{
		btree_build_state_add_tuple(state, idx_tup);
		idx_tup = tuplesort_getotuple(sortstate, true);
	}

	btree_build_state_finish(state, file_header);
	btree_build_state_free(state);
}

S3TaskLocation
btree_write_file_header(BTreeDescr *desc, CheckpointFileHeader *file_header)
{
	File		file;
	uint32		checkpoint_number;
	bool		checkpoint_concurrent;
	char	   *filename;
	S3TaskLocation result = 0;

	Assert(desc->storageType == BTreeStoragePersistence ||
		   desc->storageType == BTreeStorageTemporary ||
		   desc->storageType == BTreeStorageUnlogged);

	checkpoint_number = get_cur_checkpoint_number(&desc->oids, desc->type,
												  &checkpoint_concurrent);

	if (desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged)
	{
		SeqBufTag	prev_chkp_tag;

		memset(&prev_chkp_tag, 0, sizeof(prev_chkp_tag));
		prev_chkp_tag.datoid = desc->oids.datoid;
		prev_chkp_tag.relnode = desc->oids.relnode;
		prev_chkp_tag.num = checkpoint_number;
		prev_chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&prev_chkp_tag);

		file = PathNameOpenFile(filename, O_WRONLY | O_CREAT | PG_BINARY);

		if (OFileWrite(file, (Pointer) file_header,
					   sizeof(CheckpointFileHeader), 0,
					   WAIT_EVENT_DATA_FILE_WRITE) !=
			sizeof(CheckpointFileHeader))
		{
			pfree(filename);
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not write checkpoint header to file %s: %m",
							filename)));
		}
		FileClose(file);
		pfree(filename);

		o_update_latest_chkp_num(desc->oids.datoid,
								 desc->oids.relnode,
								 checkpoint_number);

		if (orioledb_s3_mode)
		{
			result = s3_schedule_file_part_write(checkpoint_number,
												 desc->oids.datoid,
												 desc->oids.relnode,
												 -1,
												 -1);
		}
	}
	else
	{
		EvictedTreeData evicted_tree_data = {{0}};

		evicted_tree_data.key.datoid = desc->oids.datoid;
		evicted_tree_data.key.relnode = desc->oids.relnode;
		evicted_tree_data.file_header = *file_header;
		insert_evicted_data(&evicted_tree_data);
	}

	return result;
}

OBTreeBuildState *
btree_build_state_start(BTreeDescr *desc, uint64 ctid, uint64 bridge_ctid)
{
	OBTreeBuildState *state;
	int			i;

	state = (OBTreeBuildState *) palloc0(sizeof(OBTreeBuildState));
	state->desc = desc;

	btree_open_smgr(desc);

	state->stack = (OIndexBuildStackItem *) palloc0(sizeof(OIndexBuildStackItem) * ORIOLEDB_MAX_DEPTH);
	state->root_level = 0;

	pg_atomic_init_u64(&state->metaPageBlkno.datafileLength[0], 0);
	pg_atomic_init_u64(&state->metaPageBlkno.datafileLength[1], 0);
	pg_atomic_init_u64(&state->metaPageBlkno.numFreeBlocks, 0);
	pg_atomic_init_u32(&state->metaPageBlkno.leafPagesNum, 0);
	pg_atomic_init_u64(&state->metaPageBlkno.ctid, ctid);
	pg_atomic_init_u64(&state->metaPageBlkno.bridge_ctid, bridge_ctid);

	for (i = 0; i < ORIOLEDB_MAX_DEPTH; i++)
	{
		if (i == 0)
			((BTreePageHeader *) state->stack[i].img)->flags = O_BTREE_FLAG_LEAF;
		init_page_first_chunk(desc, state->stack[i].img, 0);
		BTREE_PAGE_LOCATOR_FIRST(state->stack[i].img, &state->stack[i].loc);
	}

	return state;
}

void
btree_build_state_add_tuple(OBTreeBuildState *state, OTuple tuple)
{
	Assert(state && !state->finished);
	Assert(o_tuple_size(tuple, &((OIndexDescr *) state->desc->arg)->leafSpec) <= O_BTREE_MAX_TUPLE_SIZE);
	put_tuple_to_stack(state->desc, state->stack, tuple, &state->root_level,
					   &state->metaPageBlkno);
}

void
btree_build_state_set_positions(OBTreeBuildState *state, uint64 ctid, uint64 bridge_ctid)
{
	Assert(state && !state->finished);
	pg_atomic_write_u64(&state->metaPageBlkno.ctid, ctid);
	pg_atomic_write_u64(&state->metaPageBlkno.bridge_ctid, bridge_ctid);
}

void
btree_build_state_finish(OBTreeBuildState *state, CheckpointFileHeader *file_header)
{
	int			saved_root_level;
	uint64		downlink;
	uint32		chkpNum;
	Page		root_page;
	BTreePageHeader *root_page_header;
	FileExtent	extent;
	int			i;

	Assert(state && !state->finished);

	saved_root_level = state->root_level;
	for (i = 0; i < saved_root_level; i++)
	{
		if (i != 0)
			PAGE_SET_N_ONDISK(state->stack[i].img, BTREE_PAGE_ITEMS_COUNT(state->stack[i].img));

		extent.len = InvalidFileExtentLen;
		extent.off = InvalidFileExtentOff;

		VALGRIND_CHECK_MEM_IS_DEFINED(state->stack[i].img, ORIOLEDB_BLCKSZ);

		split_page_by_chunks(state->desc, state->stack[i].img);
		downlink = perform_page_io_build(state->desc, state->stack[i].img,
										 &extent, &state->metaPageBlkno);
		if (i == 0)
			pg_atomic_add_fetch_u32(&state->metaPageBlkno.leafPagesNum, 1);

		put_downlink_to_stack(state->desc, state->stack, i + 1, downlink,
							  state->stack[i].key.tuple, state->stack[i].keysize,
							  &state->root_level, &state->metaPageBlkno);
	}

	root_page = state->stack[state->root_level].img;

	root_page_header = (BTreePageHeader *) root_page;
	if (state->root_level == 0)
		root_page_header->flags = O_BTREE_FLAGS_ROOT_INIT;
	root_page_header->rightLink = InvalidRightLink;
	root_page_header->csn = COMMITSEQNO_FROZEN;
	root_page_header->undoLocation = InvalidUndoLocation;
	root_page_header->o_header.checkpointNum = 0;
	root_page_header->prevInsertOffset = MaxOffsetNumber;

	if (!O_PAGE_IS(root_page, LEAF))
	{
		PAGE_SET_N_ONDISK(root_page, BTREE_PAGE_ITEMS_COUNT(root_page));
		PAGE_SET_LEVEL(root_page, state->root_level);
	}

	extent.len = InvalidFileExtentLen;
	extent.off = InvalidFileExtentOff;

	VALGRIND_CHECK_MEM_IS_DEFINED(root_page, ORIOLEDB_BLCKSZ);

	split_page_by_chunks(state->desc, root_page);
	downlink = perform_page_io_build(state->desc, root_page, &extent,
									 &state->metaPageBlkno);
	if (state->root_level == 0)
		pg_atomic_add_fetch_u32(&state->metaPageBlkno.leafPagesNum, 1);

	btree_close_smgr(state->desc);

	if (orioledb_s3_mode)
		chkpNum = S3_GET_CHKP_NUM(DOWNLINK_GET_DISK_OFF(downlink));
	else
		chkpNum = 0;

	memset(file_header, 0, sizeof(*file_header));
	file_header->rootDownlink = downlink;
	file_header->datafileLength = pg_atomic_read_u64(&state->metaPageBlkno.datafileLength[chkpNum % 2]);
	file_header->numFreeBlocks = pg_atomic_read_u64(&state->metaPageBlkno.numFreeBlocks);
	file_header->leafPagesNum = pg_atomic_read_u32(&state->metaPageBlkno.leafPagesNum);
	file_header->ctid = pg_atomic_read_u64(&state->metaPageBlkno.ctid);
	file_header->bridgeCtid = pg_atomic_read_u64(&state->metaPageBlkno.bridge_ctid);

	state->finished = true;
}

void
btree_build_state_free(OBTreeBuildState *state)
{
	if (!state)
		return;
	if (state->stack)
		pfree(state->stack);
	pfree(state);
}
