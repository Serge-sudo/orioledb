/*-------------------------------------------------------------------------
 *
 * fastpath.h
 *		Declarations for fastpath intra-page navigation in B-tree.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/fastpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_FASTPATH_H__
#define __BTREE_FASTPATH_H__

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/page_contents.h"

#define FASTPATH_FIND_DOWNLINK_MAX_KEYS (4)
#define FASTPATH_FIND_DOWNLINK_FLAG_MINUS_INF (1)
#define FASTPATH_FIND_DOWNLINK_FLAG_PLUS_INF (2)

typedef void (*ArraySearchFunc) (Pointer p, int stride,
								 int *lower, int *upper, Datum keyDatum);

typedef struct
{
	bool		enabled;
	bool		inclusive;
	int			numKeys;
	int			length;

	Datum		offsets[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	ArraySearchFunc funcs[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	Datum		values[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
	uint8		flags[FASTPATH_FIND_DOWNLINK_MAX_KEYS];
} FastpathFindDownlinkMeta;

typedef enum
{
	OBTreeFastPathFindOK,
	OBTreeFastPathFindRetry,
	OBTreeFastPathFindFailure,
	OBTreeFastPathFindSlowpath
} OBTreeFastPathFindResult;

/*
 * LRU cache for hot chunk lookups during index scans.
 * Entries are stored in a doubly-linked list with most recently used at head.
 */
typedef struct FastpathChunkCacheEntry
{
	OInMemoryBlkno blkno;		/* Page block number */
	uint64		changeCount;	/* Page change count for validation */
	int			chunkIndex;		/* Cached chunk index */
	struct FastpathChunkCacheEntry *prev;	/* Previous in LRU list */
	struct FastpathChunkCacheEntry *next;	/* Next in LRU list */
} FastpathChunkCacheEntry;

typedef struct FastpathChunkCache
{
	FastpathChunkCacheEntry *head;	/* Most recently used */
	FastpathChunkCacheEntry *tail;	/* Least recently used */
	int			size;			/* Current number of entries */
	int			capacity;		/* Maximum number of entries */
	MemoryContext mctx;			/* Memory context for allocations */
} FastpathChunkCache;

extern void can_fastpath_find_downlink(OBTreeFindPageContext *context,
									   void *key,
									   BTreeKeyType keyType,
									   FastpathFindDownlinkMeta *meta);
extern OBTreeFastPathFindResult fastpath_find_chunk(Pointer pagePtr,
													OInMemoryBlkno blkno,
													FastpathFindDownlinkMeta *meta,
													int *chunkIndex);
extern OBTreeFastPathFindResult fastpath_find_downlink(Pointer pagePtr,
													   OInMemoryBlkno blkno,
													   FastpathFindDownlinkMeta *meta,
													   BTreePageItemLocator *loc,
													   BTreeNonLeafTuphdr **tuphdrPtr,
													   FastpathChunkCache *cache);

/* Cache management functions */
extern FastpathChunkCache *fastpath_cache_init(int capacity, MemoryContext mctx);
extern bool fastpath_cache_lookup(FastpathChunkCache *cache,
								  OInMemoryBlkno blkno,
								  uint64 changeCount,
								  int *chunkIndex);
extern void fastpath_cache_insert(FastpathChunkCache *cache,
								  OInMemoryBlkno blkno,
								  uint64 changeCount,
								  int chunkIndex);
extern void fastpath_cache_destroy(FastpathChunkCache *cache);

#endif							/* __BTREE_FASTPATH_H__ */
