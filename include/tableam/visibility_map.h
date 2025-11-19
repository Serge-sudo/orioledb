/*-------------------------------------------------------------------------
 *
 * visibility_map.h
 *		Buffer-based visibility map for OrioleDB tables.
 *
 * Unlike traditional block-based visibility maps, OrioleDB's VM uses a
 * segment tree structure with bounds from primary index leaf pages.
 * Each segment node stores the AND of all-visible bits from its children,
 * with lazy propagation for efficient updates.
 *
 * The VM uses a dedicated buffer pool with LRU eviction, similar to
 * OrioleDB's main page pool. VM pages are loaded on access, not during
 * ANALYZE/VACUUM.
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

/* Size of a VM page in nodes */
#define VMAP_NODES_PER_PAGE 64

/* VM buffer pool size (number of pages) */
#define VMAP_BUFFER_POOL_SIZE 128

/*
 * Segment tree node for visibility map.
 * Each node represents a range of primary index leaf pages.
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
 * VM page structure - holds VMAP_NODES_PER_PAGE nodes
 */
typedef struct OVMapPage
{
	uint32		page_num;		/* Page number in the VM file */
	OVMapSegmentNode nodes[VMAP_NODES_PER_PAGE];
} OVMapPage;

/*
 * VM buffer descriptor - tracks a buffered VM page
 */
typedef struct OVMapBufferDesc
{
	ORelOids	oids;			/* Table identifier */
	uint32		page_num;		/* VM page number */
	bool		dirty;			/* True if page needs write-back */
	bool		valid;			/* True if buffer contains valid data */
	int			usage_count;	/* For LRU eviction */
	OVMapPage  *page;			/* Pointer to actual page data */
} OVMapBufferDesc;

/*
 * VM buffer pool - manages VM page buffers with LRU eviction
 */
typedef struct OVMapBufferPool
{
	OVMapBufferDesc *buffers;	/* Array of buffer descriptors */
	OVMapPage  *pages;			/* Array of page data */
	int			num_buffers;	/* Size of buffer pool */
	int			clock_hand;		/* For clock-sweep eviction */
	LWLock	   *buffer_locks;	/* Locks for each buffer */
} OVMapBufferPool;

/*
 * VM metadata stored at the beginning of VM file
 */
typedef struct OVMapMetadata
{
	uint32		magic;			/* Magic number for validation */
	uint32		version;		/* File format version */
	uint32		num_nodes;		/* Total number of nodes in tree */
	uint32		num_leaves;		/* Number of leaf nodes (primary index pages) */
	uint32		tree_height;	/* Height of the segment tree */
	uint32		num_pages;		/* Number of VM pages */
} OVMapMetadata;

/*
 * Visibility map handle for a table
 */
typedef struct OVisibilityMap
{
	ORelOids	oids;			/* Table OIDs for identification */
	OIndexDescr *primary_idx;	/* Primary index descriptor */
	OVMapMetadata metadata;		/* VM metadata */
	bool		initialized;	/* True if VM file exists and is valid */
} OVisibilityMap;

/* Global VM buffer pool */
extern OVMapBufferPool *vmap_buffer_pool;

/* VM buffer pool initialization */
extern Size o_vmap_buffer_pool_shmem_needs(void);
extern void o_vmap_buffer_pool_shmem_init(void *ptr, bool found);

/* VM buffer operations */
extern OVMapPage *o_vmap_get_page(ORelOids oids, uint32 page_num, bool *found);
extern void o_vmap_release_page(OVMapPage *page, bool dirty);
extern void o_vmap_flush_dirty_pages(ORelOids oids);
extern void o_vmap_evict_pages(ORelOids oids);

/* VM operations */
extern OVisibilityMap *o_visibility_map_create(OIndexDescr *primary_idx, ORelOids oids);
extern void o_visibility_map_free(OVisibilityMap *vmap);
extern void o_visibility_map_init_file(OVisibilityMap *vmap, OTableDescr *descr);

/* Segment tree operations - load pages on demand */
extern bool o_visibility_map_check_page(OVisibilityMap *vmap, uint64 page_num);
extern void o_visibility_map_set_all_visible(OVisibilityMap *vmap, 
											 uint64 left_page, 
											 uint64 right_page);
extern void o_visibility_map_set_not_visible(OVisibilityMap *vmap,
											  uint64 left_page,
											  uint64 right_page);

/* ANALYZE helper - only sets all_visible if not already set */
extern void o_visibility_map_try_set_visible(OVisibilityMap *vmap, OTableDescr *descr);

/* Get statistics about the VM for pg_class.relallvisible */
extern BlockNumber o_visibility_map_get_visible_pages(OVisibilityMap *vmap, 
													  BlockNumber total_pages);

/* File path helpers */
extern char *o_visibility_map_get_path(ORelOids oids);

#endif							/* __TABLEAM_VISIBILITY_MAP_H__ */
