/*-------------------------------------------------------------------------
 *
 * visibility_map.h
 *		Primary-key-based visibility map for OrioleDB tables.
 *
 * Unlike traditional block-based visibility maps, OrioleDB's VM tracks
 * ranges of primary keys that are all-visible. This allows secondary
 * index-only scans to check visibility without accessing the primary index.
 *
 * The VM maintains sorted ranges like [pk:1..500] -> all visible,
 * [pk:770..1000] -> all visible. Adjacent ranges can be merged, and
 * ranges can be split when primary key changes occur.
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

/*
 * A range of primary keys that are all-visible.
 * The range is [start, end) - inclusive start, exclusive end.
 */
typedef struct OVisibilityRange
{
	OTuple		start_pk;		/* Start of range (inclusive) */
	OTuple		end_pk;			/* End of range (exclusive), or NULL for infinity */
	bool		all_visible;	/* True if all tuples in range are visible */
	struct OVisibilityRange *next;	/* Next range in linked list */
} OVisibilityRange;

/*
 * Visibility map for a table, tracking PK ranges.
 */
typedef struct OVisibilityMap
{
	OIndexDescr *primary_idx;	/* Primary index descriptor for PK comparisons */
	OVisibilityRange *ranges;	/* Linked list of ranges, sorted by start_pk */
	MemoryContext mctx;			/* Memory context for allocations */
	int			num_ranges;		/* Number of ranges in the list */
} OVisibilityMap;

/* Functions for managing visibility maps */
extern OVisibilityMap *o_visibility_map_create(OIndexDescr *primary_idx);
extern void o_visibility_map_free(OVisibilityMap *vmap);

/* Check if a PK is in an all-visible range */
extern bool o_visibility_map_check_pk(OVisibilityMap *vmap, OTuple pk);

/* Mark a range of PKs as all-visible */
extern void o_visibility_map_mark_range(OVisibilityMap *vmap, 
										OTuple start_pk, 
										OTuple end_pk,
										bool all_visible);

/* Build VM by scanning the primary index */
extern void o_visibility_map_build(OVisibilityMap *vmap, OTableDescr *descr);

/* Update VM when a PK is modified or deleted */
extern void o_visibility_map_update_pk(OVisibilityMap *vmap, OTuple pk, bool visible);

/* Merge adjacent ranges that are both all-visible */
extern void o_visibility_map_merge_ranges(OVisibilityMap *vmap);

/* Get statistics about the VM for pg_class.relallvisible */
extern BlockNumber o_visibility_map_get_visible_pages(OVisibilityMap *vmap, 
													  BlockNumber total_pages);

#endif							/* __TABLEAM_VISIBILITY_MAP_H__ */
