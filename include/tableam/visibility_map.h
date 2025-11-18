/*-------------------------------------------------------------------------
 *
 * visibility_map.h
 *		Segment-tree-based visibility map for OrioleDB tables.
 *
 * Unlike traditional block-based visibility maps, OrioleDB's VM uses a
 * segment tree structure with bounds from primary index leaf pages.
 * Each segment node stores the AND of all-visible bits from its children,
 * with lazy propagation for efficient updates.
 *
 * The segment tree enables O(log n) lookup and update operations, with
 * persistent storage on disk for durability.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/visibility_map.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_VISIBILITY_MAP_H__
#define __TABLEAM_VISIBILITY_MAP_H__

#include "btree/btree.h"
#include "tableam/descr.h"
#include "storage/fd.h"

/*
 * Segment tree node for visibility map.
 * Each node represents a range of primary keys and stores whether
 * all tuples in that range are visible.
 */
typedef struct OVMapSegmentNode
{
	uint64		left_bound;		/* Left page boundary (primary index page number) */
	uint64		right_bound;	/* Right page boundary (primary index page number) */
	bool		all_visible;	/* AND of all children's all_visible bits */
	bool		lazy_mark;		/* Lazy propagation flag for batch updates */
	int32		left_child;		/* Index of left child node (-1 if leaf) */
	int32		right_child;	/* Index of right child node (-1 if leaf) */
} OVMapSegmentNode;

/*
 * Persistent segment tree structure for visibility map.
 * Stored on disk with memory-mapped access for efficiency.
 */
typedef struct OVMapSegmentTree
{
	uint32		num_nodes;		/* Total number of nodes in tree */
	uint32		num_leaves;		/* Number of leaf nodes (primary index pages) */
	uint32		tree_height;	/* Height of the segment tree */
	OVMapSegmentNode *nodes;	/* Array of segment tree nodes */
	bool		dirty;			/* True if tree needs to be flushed to disk */
} OVMapSegmentTree;

/*
 * Visibility map for a table, using segment tree.
 */
typedef struct OVisibilityMap
{
	ORelOids	oids;			/* Table OIDs for identification */
	OIndexDescr *primary_idx;	/* Primary index descriptor for comparisons */
	OVMapSegmentTree *tree;		/* Segment tree structure */
	File		vmap_file;		/* File descriptor for persistent storage */
	MemoryContext mctx;			/* Memory context for allocations */
	bool		loaded;			/* True if loaded from disk */
} OVisibilityMap;

/* VM file operations */
extern OVisibilityMap *o_visibility_map_create(OIndexDescr *primary_idx, ORelOids oids);
extern void o_visibility_map_free(OVisibilityMap *vmap);
extern void o_visibility_map_load(OVisibilityMap *vmap);
extern void o_visibility_map_flush(OVisibilityMap *vmap);

/* Segment tree operations */
extern void o_visibility_map_build_tree(OVisibilityMap *vmap, OTableDescr *descr);
extern bool o_visibility_map_check_page(OVisibilityMap *vmap, uint64 page_num);
extern void o_visibility_map_mark_page_range(OVisibilityMap *vmap, 
											 uint64 left_page, 
											 uint64 right_page,
											 bool all_visible);

/* Lazy propagation helpers */
extern void o_visibility_map_push_lazy(OVisibilityMap *vmap, int32 node_idx);
extern void o_visibility_map_update_node(OVisibilityMap *vmap, int32 node_idx);

/* Build VM by scanning the primary index */
extern void o_visibility_map_build(OVisibilityMap *vmap, OTableDescr *descr);

/* Get statistics about the VM for pg_class.relallvisible */
extern BlockNumber o_visibility_map_get_visible_pages(OVisibilityMap *vmap, 
													  BlockNumber total_pages);

/* File path helpers */
extern char *o_visibility_map_get_path(ORelOids oids);

#endif							/* __TABLEAM_VISIBILITY_MAP_H__ */
