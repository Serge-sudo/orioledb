/*-------------------------------------------------------------------------
 *
 * page_pool.c
 *		OrioleDB logical page pool implementation.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/page_pool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/page_contents.h"
#include "btree/undo.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/ucm.h"

#include "utils/memdebug.h"

#define LOCAL_PPOOL_INIT_SIZE 1024

/* Shared memory based page pool operations */

OInMemoryBlkno o_ppool_get_page(PagePool *pool, int kind);
OInMemoryBlkno o_ppool_get_metapage(PagePool *pool);
void		o_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

void		o_ppool_reserve_pages(PagePool *pool, int kind, int count);
void		o_ppool_release_reserved(PagePool *pool, uint32 mask);

OInMemoryBlkno o_ppool_free_pages_count(PagePool *pool);
OInMemoryBlkno o_ppool_dirty_pages_count(PagePool *pool);
void		o_ppool_run_maintenance(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
OInMemoryBlkno o_ppool_size(PagePool *pool);

void		o_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno);
void		o_ucm_init(PagePool *pool, OInMemoryBlkno blkno);

Page		o_ppool_alloc_build_page(PagePool *pool, uint64 *handle);
uint64		o_ppool_finalize_build_page(PagePool *pool, BTreeDescr *desc, Page img, uint64 handle, FileExtent *extent, BTreeMetaPage *metaPage);
void		o_ppool_free_build_page(PagePool *pool, Page img, uint64 handle);

/* PagePoolOps for a shared memory based page pool */
static const PagePoolOps o_page_pool_ops = {
	.alloc_page = o_ppool_get_page,
	.alloc_metapage = o_ppool_get_metapage,
	.free_page = o_ppool_free_page,

	.reserve_pages = o_ppool_reserve_pages,
	.release_reserved = o_ppool_release_reserved,

	.free_pages_count = o_ppool_free_pages_count,
	.dirty_pages_count = o_ppool_dirty_pages_count,
	.run_maintenance = o_ppool_run_maintenance,
	.size = o_ppool_size,

	.ucm_inc_usage = o_ucm_inc_usage,
	.ucm_init = o_ucm_init,

	.alloc_build_page = o_ppool_alloc_build_page,
	.finalize_build_page = o_ppool_finalize_build_page,
	.free_build_page = o_ppool_free_build_page,
};

/* Shared local memory based page pool operations */

OInMemoryBlkno local_ppool_alloc_page(PagePool *pool, int kind);
OInMemoryBlkno local_ppool_alloc_metapage(PagePool *pool);
void		local_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock);

void		local_ppool_reserve_pages(PagePool *pool, int kind, int count);
void		local_ppool_release_reserved(PagePool *pool, uint32 mask);

OInMemoryBlkno local_ppool_free_pages_count(PagePool *pool);
OInMemoryBlkno local_ppool_dirty_pages_count(PagePool *pool);
void		local_ppool_run_maintenance(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested);
OInMemoryBlkno local_ppool_size(PagePool *pool);

void		local_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno);
void		local_ucm_init(PagePool *pool, OInMemoryBlkno blkno);

Page		local_ppool_alloc_build_page(PagePool *pool, uint64 *handle);
uint64		local_ppool_finalize_build_page(PagePool *pool, BTreeDescr *desc, Page img, uint64 handle, FileExtent *extent, BTreeMetaPage *metaPage);
void		local_ppool_free_build_page(PagePool *pool, Page img, uint64 handle);

/* PagePoolOps for a local memory based page pool */
static const PagePoolOps local_ppool_ops = {
	.alloc_page = local_ppool_alloc_page,
	.alloc_metapage = local_ppool_alloc_metapage,
	.free_page = local_ppool_free_page,

	.reserve_pages = local_ppool_reserve_pages,
	.release_reserved = local_ppool_release_reserved,

	.free_pages_count = local_ppool_free_pages_count,
	.dirty_pages_count = local_ppool_dirty_pages_count,
	.run_maintenance = local_ppool_run_maintenance,
	.size = local_ppool_size,

	.ucm_inc_usage = local_ucm_inc_usage,
	.ucm_init = local_ucm_init,

	.alloc_build_page = local_ppool_alloc_build_page,
	.finalize_build_page = local_ppool_finalize_build_page,
	.free_build_page = local_ppool_free_build_page,
};

/*
 * Calculates shared memory space needed for a page pool. Be careful,
 * it prepares local memory structures to initialize.
 */
Size
o_ppool_estimate_space(OPagePool *pool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug)
{
	Size		result = 0;

	if (!debug)
		Assert(size >= PPOOL_MIN_SIZE);
	/* TODO: check for ppool max size */

	pool->offset = offset;
	pool->size = size;

	result += CACHELINEALIGN(sizeof(pg_atomic_uint64));
	result += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	pool->ucmShmemSize = estimate_ucm_space(&pool->ucm, offset, size);

	result += pool->ucmShmemSize;
	return result;
}

/*
 * Initializes data in shared memory for the page pool. ppool_estimate_space()
 * must be already called for the pool.
 */
void
o_ppool_shmem_init(OPagePool *pool, Pointer ptr, bool found)
{
	pool->availablePagesCount = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	pool->dirtyPagesCount = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	if (!found)
	{
		pg_atomic_init_u64(pool->availablePagesCount, pool->size);
		pg_atomic_init_u32(pool->dirtyPagesCount, 0);
	}

	init_ucm(&pool->ucm, ptr, found);

	pg_prng_seed(&pool->prngSeed, MyBackendId);
	pool->location = pg_prng_uint64_range(&pool->prngSeed,
										  pool->offset,
										  pool->offset + pool->size - 1);
	pool->base.ops = &o_page_pool_ops;
}

/*
 * Reserve pages for further allocation.  Reserving pages might require running
 * clock algorithm with page eviction.  It shouldn't be called while holding
 * a page lock for two reasons.
 *
 * 1) Searching and eviction of page might take too long time for holding a
 *    page lock.
 * 2) Eviction of page places page locks itself.  And it's hard to guarantee
 *    there is no deadlocks assuming that we might evict almost any page.
 *
 * This is why one should reserve enough amount of pages _before_ taking a page
 * lock, and then allocate them using ucm_occupy_free_page().
 */
void
o_ppool_reserve_pages(PagePool *pool, int kind, int count)
{
	uint64		val;
	OPagePool  *o_pool = (OPagePool *) pool;

	Assert(!have_locked_pages());

	count -= o_pool->numPagesReserved[kind];
	if (count <= 0)
		return;

	val = pg_atomic_sub_fetch_u64(o_pool->availablePagesCount, count);
	while (val & (UINT64CONST(1) << 63))
	{
		(*pool->ops->run_maintenance) (pool, true, NULL);
		val = pg_atomic_read_u64(o_pool->availablePagesCount);
	}

	o_pool->numPagesReserved[kind] += count;
}

/*
 * Release previously reserved pages according to mask (multiple kinds can be
 * released in one call).
 */
void
o_ppool_release_reserved(PagePool *pool, uint32 mask)
{
	int			sum = 0,
				kind;
	OPagePool  *o_pool = (OPagePool *) pool;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
		{
			sum += o_pool->numPagesReserved[kind];
			o_pool->numPagesReserved[kind] = 0;
		}
	}
	if (sum != 0)
		pg_atomic_add_fetch_u64(o_pool->availablePagesCount, sum);
}

/*
 * Release all reserved pages in all the shared memory pools.
 */
void
ppool_release_all_pages(void)
{
	int			i;

	for (i = 0; i < (int) OPagePoolTypesCount; i++)
	{
		PagePool   *pool = get_ppool((OPagePoolType) i);

		(*pool->ops->release_reserved) (pool, PPOOL_RESERVE_MASK_ALL);
	}
}

/*
 * Reserves and allocate page for metadata. Metadata pages are typically
 * allocated without holding any page locks.
 */
/*  THOUGHT: can be shared for both ppool impls */
OInMemoryBlkno
o_ppool_get_metapage(PagePool *pool)
{
	(*pool->ops->reserve_pages) (pool, PPOOL_RESERVE_META, 1);
	return (*pool->ops->alloc_page) (pool, PPOOL_RESERVE_META);
}

/*
 * Get next free page from the pool.
 *
 * Free page should be previously reserved by o_pool_reserve_pages().
 */
OInMemoryBlkno
o_ppool_get_page(PagePool *pool, int kind)
{
	OPagePool  *o_pool = (OPagePool *) pool;
	OInMemoryBlkno result;

	Assert(o_pool->numPagesReserved[kind] > 0);
	o_pool->numPagesReserved[kind]--;

	result = ucm_occupy_free_page(&o_pool->ucm);
	Assert(o_pool->offset <= result && result < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(result), ORIOLEDB_BLCKSZ);

	return result;
}

/*
 * Return free page to the pool.
 */
void
o_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	OPagePool  *o_pool = (OPagePool *) pool;

	Assert(o_pool->offset <= blkno && blkno < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	Assert(!IS_DIRTY(blkno));

	/*
	 * Reset page header and descriptor.  Do this while holding a page lock in
	 * order to prevent race condition with walk_page().
	 */
	if (!haveLock)
		lock_page(blkno);
	O_PAGE_CHANGE_COUNT_INC(p);
	ORelOidsSetInvalid(page_desc->oids);
	page_desc->type = 0;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	unlock_page(blkno);

	page_change_usage_count(&o_pool->ucm, blkno, UCM_FREE_PAGES_LEVEL);

	pg_atomic_add_fetch_u64(o_pool->availablePagesCount, 1);
}

/*
 * Return count of free pages in the pool.
 */
OInMemoryBlkno
o_ppool_free_pages_count(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;
	uint64		count = pg_atomic_read_u64(o_pool->availablePagesCount);

	if (count & (UINT64CONST(1) << 63))
		return 0;
	else
		return (OInMemoryBlkno) count;
}

/*
 * Return count of dirty pages in the pool.
 */
OInMemoryBlkno
o_ppool_dirty_pages_count(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return pg_atomic_read_u32(o_pool->dirtyPagesCount);
}

/*
 * Run clock replacement algorithm until we evict at least one page.
 *
 * This can be called from any backend that needs pages (via
 * ppool_reserve_pages) or from the bgwriter.  Because the caller may
 * already have undo space reserved for its own operation, we save and
 * restore the undo reservation state around the eviction work.
 *
 * We save both the reserved undo sizes and whether
 * transactionUndoRetainLocation was set for UndoLogRegularPageLevel and
 * UndoLogSystem.  Page merges during walk_page() may set these via
 * get_undo_record() → set_my_reserved_location().  After we're done, we
 * restore the caller's original reservation and free any retain locations
 * that we introduced (i.e., that weren't set before we entered).
 *
 * Note: we only manage UndoLogRegularPageLevel and UndoLogSystem here
 * because page-level merges only write undo to these types (via
 * GET_PAGE_LEVEL_UNDO_TYPE).  UndoLogRegular is not touched by merges.
 */
void
o_ppool_run_maintenance(PagePool *pool, bool evict,
						volatile sig_atomic_t *shutdown_requested)
{
	uint64		blkno;
	Size		undoRegularSize = get_reserved_undo_size(UndoLogRegularPageLevel);
	Size		undoSystemSize = get_reserved_undo_size(UndoLogSystem);
	bool		haveRetainRegularLoc = undo_type_has_retained_location(UndoLogRegularPageLevel);
	bool		haveRetainSystemLoc = undo_type_has_retained_location(UndoLogSystem);
	OPagePool  *o_pool = (OPagePool *) pool;

	blkno = pg_prng_uint64_range(&o_pool->prngSeed,
								 o_pool->offset,
								 o_pool->offset + o_pool->size - 1);

	/*
	 * Shouldn't be called while holding a page lock: one should reserve the
	 * pages in advance.
	 */
	Assert(!have_locked_pages());

	/* We might need to merge pages */
	reserve_undo_size(UndoLogRegularPageLevel, 2 * O_MERGE_UNDO_IMAGE_SIZE);
	reserve_undo_size(UndoLogSystem, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);
	/* Our attempts to evict pages shouldn't themselves affect UCM */
	set_skip_ucm();

	while (true)
	{
		if (shutdown_requested != NULL && *shutdown_requested)
			break;

		blkno = ucm_next_blkno(&o_pool->ucm, blkno, 1);

		Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);
		if (walk_page(blkno, evict) != OWalkPageSkipped)
		{
			Assert(!have_locked_pages());
			break;
		}
		Assert(!have_locked_pages());
		blkno++;
		if (blkno >= o_pool->offset + o_pool->size)
			blkno = o_pool->offset;
	}

	unset_skip_ucm();

	/*
	 * The caller might have the undo location reserved.  We need to carefully
	 * put the undo location back.
	 */
	if (undoRegularSize > 0)
		reserve_undo_size(UndoLogRegularPageLevel, undoRegularSize);
	else
		release_undo_size(UndoLogRegularPageLevel);

	if (undoSystemSize > 0)
		reserve_undo_size(UndoLogSystem, undoSystemSize);
	else
		release_undo_size(UndoLogSystem);

	if (!haveRetainRegularLoc)
		free_retained_undo_location(UndoLogRegularPageLevel);
	if (!haveRetainSystemLoc)
		free_retained_undo_location(UndoLogSystem);

	if ((shutdown_requested == NULL || !*shutdown_requested) && ucm_epoch_needs_shift(&o_pool->ucm))
	{
		ucm_epoch_shift(&o_pool->ucm);
	}
}

/*
 * Return the size of the page pool.
 */
OInMemoryBlkno
o_ppool_size(PagePool *pool)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	return o_pool->size;
}

void
o_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	page_inc_usage_count(&o_pool->ucm, blkno);
}

void
o_ucm_init(PagePool *pool, OInMemoryBlkno blkno)
{
	OPagePool  *o_pool = (OPagePool *) pool;

	page_change_usage_count(&o_pool->ucm, blkno, (pg_atomic_read_u32(o_pool->ucm.epoch) + 2) % UCM_USAGE_LEVELS);
}

/*
 * Allocate a page for index building. For the shared memory pool,
 * we just palloc a temporary buffer since the page will be written to disk.
 */
Page
o_ppool_alloc_build_page(PagePool *pool, uint64 *handle)
{
	Page		img = (Page) palloc0(ORIOLEDB_BLCKSZ);

	*handle = 0;				/* unused for disk-based pool */
	return img;
}

/*
 * Finalize a build page by writing it to disk and freeing the temporary buffer.
 */
uint64
o_ppool_finalize_build_page(PagePool *pool, BTreeDescr *desc, Page img,
							uint64 handle, FileExtent *extent,
							BTreeMetaPage *metaPage)
{
	uint64		downlink;

	downlink = perform_page_io_build(desc, img, extent, metaPage);
	pfree(img);
	return downlink;
}

/*
 * Free a build page that was never finalized.
 */
void
o_ppool_free_build_page(PagePool *pool, Page img, uint64 handle)
{
	pfree(img);
}

void
local_ppool_init(LocalPagePool *pool, int32 max_size)
{
	int			init_size;

	/*
	 * Treat max_size=0 the same as -1 (unbounded) since a pool with 0 pages
	 * is not useful.
	 */
	if (max_size == 0)
		max_size = -1;

	init_size = (max_size >= 0) ? max_size : LOCAL_PPOOL_INIT_SIZE;

	local_ppool_pages = calloc(init_size, sizeof(Page));
	local_ppool_page_descs = calloc(init_size, sizeof(OrioleDBPageDesc));
	if (!local_ppool_pages || !local_ppool_page_descs)
		ereport(ERROR, errmsg("Failed to allocate memory for local page pool"));

	for (int i = 0; i < init_size; i++)
		o_page_desc_init(&local_ppool_page_descs[i]);

	pool->size = init_size;
	pool->max_size = max_size;
	pool->current_slot = 0;
	memset(pool->numReserved, 0, sizeof(pool->numReserved));
	pool->slab_context = SlabContextCreate(TopMemoryContext, "oriole local page pool", ORIOLEDB_BLCKSZ * 16, ORIOLEDB_BLCKSZ);
	/* This might lead to PANIC on allocation failure in critical section */
	MemoryContextAllowInCriticalSection(pool->slab_context, true);
	pool->base.ops = &local_ppool_ops;

	if (LOCAL_PPOOL_IS_BOUNDED(pool))
	{
		Size		ucm_size;
		int			i;

		/*
		 * Initialize the UCM for the bounded pool.  Local blknos have bit 31
		 * set (slot | 0x80000000), so the UCM offset is 0x80000000 and size
		 * is max_size.  This mirrors how OPagePool uses estimate_ucm_space /
		 * init_ucm with its shared-memory offset.
		 */
		ucm_size = estimate_ucm_space(&pool->ucm, (OInMemoryBlkno) 0x80000000U,
									  (OInMemoryBlkno) max_size);
		pool->ucm_memory = palloc(ucm_size);
		init_ucm(&pool->ucm, pool->ucm_memory, false);

		/*
		 * Pre-allocate all page buffers and set their initial state to
		 * UCM_FREE_PAGES_LEVEL.  ucm_occupy_free_page() reads the page header
		 * state to find a free slot, so the buffers must exist up front.
		 * init_ucm() already told the UCM tree that all pages are free; we
		 * just need the page states to agree.
		 */
		for (i = 0; i < max_size; i++)
		{
			local_ppool_pages[i] = (Page) MemoryContextAllocZero(pool->slab_context, ORIOLEDB_BLCKSZ);
			pg_atomic_init_u64(&O_PAGE_HEADER(local_ppool_pages[i])->state,
							   O_PAGE_STATE_SET_USAGE_COUNT(0, UCM_FREE_PAGES_LEVEL));
		}

		pool->numFreePages = max_size;
	}
	else
	{
		pool->ucm_memory = NULL;
		pool->numFreePages = 0;	/* unused for unbounded */
	}
}

/*
 * Try to evict page at slot 'slot' from the bounded local pool.
 *
 * Finds the parent's downlink in the btree, writes the page to the BTree's
 * own smgr file (orioledb_data/{datoid}/{relnode}), updates the parent's
 * downlink to a regular on-disk downlink, and frees the in-memory slot.
 *
 * Returns true if eviction succeeded, false if the page cannot be evicted
 * (e.g. it is a non-leaf page, the root/meta page, or the btree descriptor
 * is unavailable).
 */
static bool
local_ppool_evict_page(LocalPagePool *local_pool, uint32 slot)
{
	OInMemoryBlkno local_blkno = slot | 0x80000000;
	OrioleDBPageDesc *page_desc = &local_ppool_page_descs[slot];
	Page		p = local_ppool_pages[slot];
	BTreeDescr *desc;
	OBTreeFindPageContext context;
	OFindPageResult findResult;
	Page		parent_page;
	BTreeNonLeafTuphdr *int_hdr;
	FileExtent	extent;
	uint64		disk_downlink;

	/*
	 * In bounded mode all page buffers are pre-allocated; a free slot is
	 * identified by the OIDs being invalid (set to invalid in free_page and
	 * evict_page cleanup).  NULL check is only needed for the unbounded case,
	 * but this function is only called for bounded pools.
	 */
	if (!ORelOidsIsValid(page_desc->oids))
		return false;

	/* Skip system in-memory trees (they use local pages too) */
	if (IS_SYS_TREE_OIDS(page_desc->oids))
		return false;

	/*
	 * Only evict leaf pages.  Non-leaf pages contain in-memory downlinks to
	 * children; evicting them would require tracking and updating all child
	 * descriptors, which is complex.  In practice, leaf pages make up the
	 * vast majority of a btree, so this restriction is acceptable.
	 */
	if (!O_PAGE_IS(p, LEAF))
		return false;

	/* Get the btree descriptor for this page */
	desc = index_oids_get_btree_descr(page_desc->oids, page_desc->type);
	if (desc == NULL)
		return false;

	/* Never evict the root or meta page of a tree */
	if (desc->rootInfo.rootPageBlkno == local_blkno ||
		desc->rootInfo.metaPageBlkno == local_blkno)
		return false;

	/*
	 * Find the parent page that holds the downlink to our page.  We use
	 * find_page() with BTREE_PAGE_FIND_DOWNLINK_LOCATION to locate the
	 * parent entry.  For local (single-session) pages, locking is a no-op,
	 * so this traversal is safe.
	 */
	init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY
						   | BTREE_PAGE_FIND_TRY_LOCK
						   | BTREE_PAGE_FIND_DOWNLINK_LOCATION
						   | BTREE_PAGE_FIND_NO_FIX_SPLIT);

	if (O_PAGE_IS(p, RIGHTMOST))
		findResult = find_page(&context, NULL, BTreeKeyRightmost,
							   PAGE_GET_LEVEL(p) + 1);
	else
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, p);
		findResult = find_page(&context, &hikey, BTreeKeyPageHiKey,
							   PAGE_GET_LEVEL(p) + 1);
	}

	if (findResult != OFindPageResultSuccess)
		return false;

	/*
	 * Verify that the downlink in the parent actually points to our page.
	 * It might not if a split moved the page (though for single-session
	 * local pages this shouldn't happen).
	 */
	parent_page = O_GET_IN_MEMORY_PAGE(context.items[context.index].blkno);
	int_hdr = (BTreeNonLeafTuphdr *)
		BTREE_PAGE_LOCATOR_GET_ITEM(parent_page,
									&context.items[context.index].locator);

	if (!DOWNLINK_IS_IN_MEMORY(int_hdr->downlink) ||
		DOWNLINK_GET_IN_MEMORY_BLKNO(int_hdr->downlink) != local_blkno)
		return false;

	/*
	 * Ensure the BTree's smgr file infrastructure is set up.  For
	 * BTreeStorageInMemory trees this is normally never done, but we need it
	 * to write evicted pages to orioledb_data/{datoid}/{relnode}.
	 *
	 * In S3 mode, btree_open_smgr() initializes smgr.hash; in non-S3 mode
	 * it initializes smgr.array.files.  Use the non-S3 null-check to detect
	 * whether smgr has been initialized yet.  When S3 mode is enabled, the
	 * hash table is always initialized by btree_open_smgr, so re-calling is
	 * harmless (the function is idempotent for the array case).
	 */
	if (!orioledb_s3_mode && desc->smgr.array.files == NULL)
	{
		char	   *prefix;
		char	   *db_prefix;

		/*
		 * Make sure the database directory exists before opening the file.
		 */
		o_get_prefixes_for_relnode(desc->oids.datoid, desc->oids.relnode,
								   &prefix, &db_prefix);
		o_verify_dir_exists_or_create(prefix, NULL, NULL);
		o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
		pfree(db_prefix);

		btree_open_smgr(desc);
	}

	/*
	 * Before writing to disk, determine which segment the next free offset
	 * falls in, and ensure all preceding segment files (0..N-1) exist.  File
	 * segments are expected to be created in order (0, 1, 2, ...).  If
	 * local_page_pool_size exceeds one segment's worth of pages (131072 for
	 * 8 KiB pages / 1 GiB segment), the pool can accumulate evictions that
	 * reach segment N without the OS files for 0..N-1 having been explicitly
	 * created.  btree_smgr_ensure_segments_up_to() opens/creates each
	 * missing file so no segment is skipped.
	 *
	 * Single-backend pool: no other process modifies datafileLength[0], so
	 * peeking at it before the atomic increment is safe.
	 */
	if (!orioledb_s3_mode)
	{
		BTreeMetaPage *metaPage = BTREE_GET_META(desc);
		uint64		next_off = pg_atomic_read_u64(&metaPage->datafileLength[0]);
		uint64		target_segno = (next_off * ORIOLEDB_BLCKSZ) /
								   ORIOLEDB_SEGMENT_SIZE;

		if (target_segno > 0)
			btree_smgr_ensure_segments_up_to(desc, (uint32) (target_segno - 1));
	}

	/*
	 * Write the page to the BTree's own smgr file.  We use
	 * perform_page_io_autonomous() which atomically allocates a file offset
	 * from datafileLength[0] and writes the page image.  This uses the same
	 * file as the BTree would use for regular eviction, so the file is
	 * properly cleaned up when the temp table is dropped.
	 */
	disk_downlink = perform_page_io_autonomous(desc, 0, p, &extent);
	if (disk_downlink == InvalidDiskDownlink)
		return false;

	/*
	 * Store as a regular on-disk downlink.  Local pool downlinks are either
	 * in-memory or on-disk — never IO-in-progress.  find.c distinguishes
	 * local vs. shared pool on-disk downlinks by checking O_PAGE_IS_LOCAL().
	 */
	int_hdr->downlink = disk_downlink;

	/* Free the in-memory page slot via UCM */
	O_PAGE_CHANGE_COUNT_INC(local_ppool_pages[slot]);
	ORelOidsSetInvalid(local_ppool_page_descs[slot].oids);
	local_ppool_page_descs[slot].fileExtent.off = InvalidFileExtentOff;
	local_ppool_page_descs[slot].fileExtent.len = InvalidFileExtentLen;
	local_pool->numFreePages++;
	page_change_usage_count(&local_pool->ucm, local_blkno, UCM_FREE_PAGES_LEVEL);

	return true;
}

OInMemoryBlkno
local_ppool_alloc_page(PagePool *pool, int kind)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;

	if (!LOCAL_PPOOL_IS_BOUNDED(local_pool))
	{
		/*
		 * Unbounded mode: original growing repalloc logic.
		 */
		int			start = local_pool->current_slot;
		int			i = start;
		int			old_size = local_pool->size;
		int			new_size;
		Page	   *new_pages;
		OrioleDBPageDesc *new_page_descs;

		/* Iterate through local_pool->pages to find a free slot */
		do
		{
			i++;
			if (i >= (int) local_pool->size)
				i = 0;
			if (local_ppool_pages[i] == NULL)
			{
				local_ppool_pages[i] = (Page) MemoryContextAllocZero(local_pool->slab_context, ORIOLEDB_BLCKSZ);
				local_pool->current_slot = i;
				/* Set the local page bit */
				return i | 0x80000000;
			}
		} while (i != start);

		/* Failed to find a free slot - increase pages array size */

		new_size = local_pool->size * 2;
		new_pages = realloc(local_ppool_pages, new_size * sizeof(Page));
		new_page_descs = realloc(local_ppool_page_descs, new_size * sizeof(OrioleDBPageDesc));

		if (!new_pages || !new_page_descs)
		{
			/*
			 * Original pointers remain valid if their realloc failed, keeping
			 * state consistent.
			 */
			ereport(ERROR, errmsg("Failed to allocate memory for local page pool"));
		}

		local_ppool_pages = new_pages;
		local_ppool_page_descs = new_page_descs;
		local_pool->size = new_size;
		memset(local_ppool_pages + old_size, 0, old_size * sizeof(Page));

		for (int j = old_size; j < new_size; j++)
			o_page_desc_init(&local_ppool_page_descs[j]);

		local_pool->current_slot = old_size;
		local_ppool_pages[old_size] = (Page) MemoryContextAllocZero(local_pool->slab_context, ORIOLEDB_BLCKSZ);

		/* Set the local page bit */
		return old_size | 0x80000000;
	}
	else
	{
		/*
		 * Bounded mode: reserve_pages() is responsible for pre-evicting pages
		 * before a critical section.  alloc_page() finds a free slot via the
		 * UCM (same as o_ppool_get_page does with o_pool->ucm) and returns it.
		 * All page buffers are pre-allocated, so no palloc happens here.
		 */
		OInMemoryBlkno blkno;

		/*
		 * Consume one pre-reserved slot for this kind, if available.
		 */
		if (kind >= 0 && kind < PPOOL_RESERVE_COUNT &&
			local_pool->numReserved[kind] > 0)
			local_pool->numReserved[kind]--;

		Assert(local_pool->numFreePages > 0);
		local_pool->numFreePages--;

		blkno = ucm_occupy_free_page(&local_pool->ucm);
		Assert(OInMemoryBlknoIsValid(blkno));

		/*
		 * Update current_slot hint to the next position (mirrors the
		 * unbounded path for consistent hint-tracking semantics).
		 */
		local_pool->current_slot = ((blkno & O_BLKNO_MASK) + 1) % local_pool->size;

		return blkno;
	}
}

OInMemoryBlkno
local_ppool_alloc_metapage(PagePool *pool)
{
	/* Kind is not used */
	return local_ppool_alloc_page(pool, 0);
}

void
local_ppool_free_page(PagePool *pool, OInMemoryBlkno blkno, bool haveLock)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;
	int			i = blkno & O_BLKNO_MASK;

	if (LOCAL_PPOOL_IS_BOUNDED(local_pool))
	{
		/*
		 * Bounded mode: page buffer is pre-allocated and stays allocated.
		 * Increment change count (to invalidate stale downlinks), reset the
		 * descriptor, and mark the page free in the UCM.
		 */
		O_PAGE_CHANGE_COUNT_INC(local_ppool_pages[i]);
		ORelOidsSetInvalid(local_ppool_page_descs[i].oids);
		local_ppool_page_descs[i].fileExtent.off = InvalidFileExtentOff;
		local_ppool_page_descs[i].fileExtent.len = InvalidFileExtentLen;
		local_pool->numFreePages++;
		page_change_usage_count(&local_pool->ucm, blkno, UCM_FREE_PAGES_LEVEL);
	}
	else
	{
		pfree(local_ppool_pages[i]);
		local_ppool_pages[i] = NULL;
	}
}

void
local_ppool_reserve_pages(PagePool *pool, int kind, int count)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;
	int			need;
	uint32		tries;

	/*
	 * For bounded pools we must pre-evict pages here, outside any critical
	 * section, so that the subsequent alloc_page calls (which may run inside
	 * START_CRIT_SECTION) only need to grab a pre-freed slot from the pool.
	 * Those grabs use ucm_occupy_free_page() which does no allocation and is
	 * safe inside a critical section.
	 *
	 * For unbounded pools the array grows with realloc() (not palloc) and the
	 * slot is filled from the slab context, so no pre-eviction is needed.
	 */
	if (!LOCAL_PPOOL_IS_BOUNDED(local_pool))
		return;

	Assert(kind >= 0 && kind < PPOOL_RESERVE_COUNT);

	/* How many additional free slots do we still need? */
	need = count - (int) local_pool->numReserved[kind];
	if (need <= 0)
		return;

	/*
	 * Run the clock sweep (via run_maintenance) until we have at least 'need'
	 * free slots available, as tracked by numFreePages.
	 */
	for (tries = 0; tries < 2 * local_pool->size; tries++)
	{
		if ((int) local_pool->numFreePages >= need)
			break;

		local_ppool_run_maintenance(pool, true, NULL);
	}

	local_pool->numReserved[kind] += need;
}

void
local_ppool_release_reserved(PagePool *pool, uint32 mask)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;
	int			kind;

	if (!LOCAL_PPOOL_IS_BOUNDED(local_pool))
		return;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
			local_pool->numReserved[kind] = 0;
	}
}

OInMemoryBlkno
local_ppool_free_pages_count(PagePool *pool)
{
	return UINT32_MAX;
}

OInMemoryBlkno
local_ppool_dirty_pages_count(PagePool *pool)
{
	return 0;
}

void
local_ppool_run_maintenance(PagePool *pool, bool evict, volatile sig_atomic_t *shutdown_requested)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;
	OInMemoryBlkno init_blkno,
				blkno;
	uint32		tries;

	if (!LOCAL_PPOOL_IS_BOUNDED(local_pool))
		return;

	/*
	 * Use ucm_next_blkno() to find the page with the lowest relative usage
	 * count — the same strategy used by the shared pool (o_ppool_run_maintenance).
	 * current_slot serves as the rotating start position so successive calls
	 * scan different parts of the pool rather than always revisiting the same
	 * pages.
	 *
	 * Our eviction attempts shouldn't themselves affect UCM counters (mirrors
	 * the shared pool's set_skip_ucm() / unset_skip_ucm() pattern).
	 */
	init_blkno = local_pool->ucm.offset + local_pool->current_slot;
	set_skip_ucm();

	for (tries = 0; tries < local_pool->size; tries++)
	{
		blkno = ucm_next_blkno(&local_pool->ucm, init_blkno, 1);

		/* Advance start position for the next run_maintenance call */
		local_pool->current_slot = ((blkno & O_BLKNO_MASK) + 1) % local_pool->size;
		init_blkno = blkno + 1;
		if (init_blkno >= local_pool->ucm.offset + local_pool->size)
			init_blkno = local_pool->ucm.offset;

		if (local_ppool_evict_page(local_pool, blkno & O_BLKNO_MASK))
		{
			unset_skip_ucm();
			if (ucm_epoch_needs_shift(&local_pool->ucm))
				ucm_epoch_shift(&local_pool->ucm);
			return;
		}
	}

	unset_skip_ucm();
}

OInMemoryBlkno
local_ppool_size(PagePool *pool)
{
	LocalPagePool *o_pool = (LocalPagePool *) pool;

	return o_pool->size;
}

void
local_ucm_inc_usage(PagePool *pool, OInMemoryBlkno blkno)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;

	if (LOCAL_PPOOL_IS_BOUNDED(local_pool))
		page_inc_usage_count(&local_pool->ucm, blkno);
}

void
local_ucm_init(PagePool *pool, OInMemoryBlkno blkno)
{
	LocalPagePool *local_pool = (LocalPagePool *) pool;

	if (LOCAL_PPOOL_IS_BOUNDED(local_pool))
	{
		/*
		 * Set initial usage count to (epoch + 2) % UCM_USAGE_LEVELS, the
		 * same value used by o_ucm_init for the shared pool.  This places
		 * freshly allocated pages two steps ahead of the current eviction
		 * frontier, giving them a chance to be reused before being evicted.
		 */
		page_change_usage_count(&local_pool->ucm, blkno,
								(pg_atomic_read_u32(local_pool->ucm.epoch) + 2) % UCM_USAGE_LEVELS);
	}
}

/*
 * Allocate a page directly from the local page pool.
 * This allows building directly into pool pages, avoiding a copy.
 */
Page
local_ppool_alloc_build_page(PagePool *pool, uint64 *handle)
{
	OInMemoryBlkno blkno = (*pool->ops->alloc_page) (pool, PPOOL_RESERVE_META);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	*handle = blkno;
	return p;
}

/*
 * Finalize a build page. For local page pool, the page is already in the pool,
 * so we just return the blkno as the downlink.
 */
uint64
local_ppool_finalize_build_page(PagePool *pool, BTreeDescr *desc, Page img,
								uint64 handle, FileExtent *extent,
								BTreeMetaPage *metaPage)
{
	/* The handle is the blkno - just return it as the downlink */
	return handle;
}

/*
 * Free a build page that was never finalized.
 */
void
local_ppool_free_build_page(PagePool *pool, Page img, uint64 handle)
{
	local_ppool_free_page(pool, (OInMemoryBlkno) handle, false);
}

/*
 * Load a previously evicted local page back into the local page pool.
 *
 * Called from find.c when a parent page in a local pool tree has an on-disk
 * downlink.  Allocates a new local pool slot, reads the page content from
 * the BTree's own smgr file (orioledb_data/{datoid}/{relnode}), copies
 * descriptor info from the parent, and updates the parent's downlink to the
 * new in-memory blkno.  After this call the find-page traversal can continue
 * as if the downlink had been in-memory all along.
 */
void
local_load_page(OBTreeFindPageContext *context)
{
	BTreeDescr *desc = context->desc;
	int			context_index = context->index;
	OInMemoryBlkno parent_blkno = context->items[context_index].blkno;
	BTreePageItemLocator *parent_loc = &context->items[context_index].locator;
	Page		parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);
	BTreeNonLeafTuphdr *int_hdr;
	uint64		disk_downlink;
	FileExtent	extent;
	OInMemoryBlkno new_blkno;
	OrioleDBPageDesc *parent_page_desc,
			   *new_page_desc;
	char	   *tmp_buf;

	int_hdr = (BTreeNonLeafTuphdr *)
		BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	Assert(DOWNLINK_IS_ON_DISK(int_hdr->downlink));

	disk_downlink = int_hdr->downlink;
	extent.off = DOWNLINK_GET_DISK_OFF(disk_downlink);
	extent.len = DOWNLINK_GET_DISK_LEN(disk_downlink);

	/*
	 * Reserve one FIND-kind slot before allocating.  For the shared pool this
	 * is done in load_page() (io.c) before alloc_page.  For the local pool,
	 * local_load_page is called from find.c after encountering an on-disk
	 * downlink during traversal; the caller may not have reserved a FIND slot,
	 * so we do it here to ensure eviction can happen outside any critical
	 * section if the pool is full.
	 */
	(*desc->ppool->ops->reserve_pages) (desc->ppool, PPOOL_RESERVE_FIND, 1);

	/*
	 * Allocate a new slot in the local pool for the reloaded page.  This may
	 * itself trigger eviction of another page, but that's fine as long as
	 * we're not trying to evict the page we're about to load.
	 */
	new_blkno = (*desc->ppool->ops->alloc_page) (desc->ppool, PPOOL_RESERVE_FIND);

	/* Set up the descriptor before reading so read_page_from_disk can use it */
	parent_page_desc = O_GET_IN_MEMORY_PAGEDESC(parent_blkno);
	new_page_desc = O_GET_IN_MEMORY_PAGEDESC(new_blkno);
	new_page_desc->type = parent_page_desc->type;
	new_page_desc->oids = parent_page_desc->oids;
	new_page_desc->flags = 0;
	new_page_desc->fileExtent = extent;

	/*
	 * Read the page into a temporary buffer first.  write_page_to_disk()
	 * stores an OrioleDBOndiskPageHeader in the first 16 bytes of the file
	 * image (with page_version, checkpointNum, etc.), so we cannot read
	 * directly into the in-memory slot — that would corrupt the in-memory
	 * OrioleDBPageHeader (state, pageChangeCount).  Instead, mirroring what
	 * load_page() does for the shared pool: read into tmp_buf, then copy via
	 * put_page_image() which skips the first sizeof(OrioleDBPageHeader) bytes,
	 * preserving the in-memory state.  Finally call ucm_init to assign a
	 * proper UCM usage count.
	 */
	tmp_buf = (char *) palloc(ORIOLEDB_BLCKSZ);
	if (!read_page_from_disk(desc, tmp_buf, disk_downlink,
							 &new_page_desc->fileExtent))
	{
		pfree(tmp_buf);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read evicted local page from BTree file: %m")));
	}

	put_page_image(new_blkno, tmp_buf);
	pfree(tmp_buf);

	/* Assign a proper UCM usage count to the freshly loaded page */
	(*desc->ppool->ops->ucm_init) (desc->ppool, new_blkno);

	/*
	 * Update the parent's downlink to point to the new in-memory blkno.
	 * After this, the find-page loop in find.c will see an in-memory downlink
	 * and proceed normally.
	 */
	int_hdr->downlink = MAKE_IN_MEMORY_DOWNLINK(new_blkno,
												O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(new_blkno));
}
