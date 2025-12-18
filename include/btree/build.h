/*-------------------------------------------------------------------------
 *
 * build.h
 * 		Declarations for sort-based B-tree index building.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/build.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_BUILD_H__
#define __BTREE_BUILD_H__

#include "btree.h"

typedef struct Tuplesortstate Tuplesortstate;
typedef struct OBTreeBuildState OBTreeBuildState;

extern void btree_write_index_data(BTreeDescr *desc, TupleDesc tupdesc,
								   Tuplesortstate *sortstate,
								   uint64 ctid, uint64 bridge_ctid,
								   CheckpointFileHeader *file_header);
extern S3TaskLocation btree_write_file_header(BTreeDescr *desc,
											  CheckpointFileHeader *file_header);
extern OBTreeBuildState *btree_build_state_start(BTreeDescr *desc,
												 uint64 ctid, uint64 bridge_ctid);
extern void btree_build_state_add_tuple(OBTreeBuildState *state, OTuple tuple);
extern void btree_build_state_set_positions(OBTreeBuildState *state,
											uint64 ctid, uint64 bridge_ctid);
extern void btree_build_state_finish(OBTreeBuildState *state,
									 CheckpointFileHeader *file_header);
extern void btree_build_state_free(OBTreeBuildState *state);

#endif							/* __BTREE_BUILD_H__ */
