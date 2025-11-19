/*-------------------------------------------------------------------------
 *
 * visibility_map.c
 *Buffer-based visibility map for OrioleDB tables.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *  contrib/orioledb/src/tableam/visibility_map.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "tableam/descr.h"
#include "tableam/visibility_map.h"

#include "common/relpath.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

#define VMAP_FILE_EXTENSION "ovm"
#define VMAP_MAGIC 0x4F564D42/* 'OVMB' - Buffer-based */
#define VMAP_VERSION 2

/* Global VM buffer pool */
OVMapBufferPool *vmap_buffer_pool = NULL;
static int	vmap_buffer_lock_tranche_id = 0;

/*
 * Request LWLock tranches for VM buffer pool
 */
void
o_vmap_request_lwlocks(void)
{
	RequestNamedLWLockTranche("orioledb_vmap_buffers", VMAP_BUFFER_POOL_SIZE);
}

/*
 * Calculate shared memory needed for VM buffer pool
 */
Size
o_vmap_buffer_pool_shmem_needs(void)
{
Sizesize = 0;

/* Buffer descriptors */
size = add_size(size, mul_size(VMAP_BUFFER_POOL_SIZE, sizeof(OVMapBufferDesc)));

/* Page data */
size = add_size(size, mul_size(VMAP_BUFFER_POOL_SIZE, sizeof(OVMapPage)));

/* Buffer locks */
size = add_size(size, mul_size(VMAP_BUFFER_POOL_SIZE, sizeof(LWLockMinimallyPadded)));

/* Pool structure */
size = add_size(size, sizeof(OVMapBufferPool));

return size;
}

/*
 * Initialize VM buffer pool in shared memory
 */
void
o_vmap_buffer_pool_shmem_init(void *ptr, bool found)
{
char   *cur_ptr = (char *) ptr;
inti;

if (!found)
{
/* Initialize pool structure */
vmap_buffer_pool = (OVMapBufferPool *) cur_ptr;
cur_ptr += sizeof(OVMapBufferPool);

/* Initialize buffer descriptors */
vmap_buffer_pool->buffers = (OVMapBufferDesc *) cur_ptr;
cur_ptr += VMAP_BUFFER_POOL_SIZE * sizeof(OVMapBufferDesc);

/* Initialize page data */
vmap_buffer_pool->pages = (OVMapPage *) cur_ptr;
cur_ptr += VMAP_BUFFER_POOL_SIZE * sizeof(OVMapPage);

/* Initialize locks */
vmap_buffer_pool->buffer_locks = (LWLock *) cur_ptr;
cur_ptr += VMAP_BUFFER_POOL_SIZE * sizeof(LWLockMinimallyPadded);

vmap_buffer_pool->num_buffers = VMAP_BUFFER_POOL_SIZE;
vmap_buffer_pool->clock_hand = 0;

/* Initialize each buffer */
for (i = 0; i < VMAP_BUFFER_POOL_SIZE; i++)
{
vmap_buffer_pool->buffers[i].oids.datoid = InvalidOid;
vmap_buffer_pool->buffers[i].oids.reloid = InvalidOid;
vmap_buffer_pool->buffers[i].oids.relnode = InvalidOid;
vmap_buffer_pool->buffers[i].page_num = 0;
vmap_buffer_pool->buffers[i].dirty = false;
vmap_buffer_pool->buffers[i].valid = false;
vmap_buffer_pool->buffers[i].usage_count = 0;
vmap_buffer_pool->buffers[i].page = &vmap_buffer_pool->pages[i];

LWLockInitialize(&vmap_buffer_pool->buffer_locks[i],
 LWTRANCHE_ORIOLEDB_VMAP_BUFFER);
}
}
else
{
/* Re-attach to existing pool */
vmap_buffer_pool = (OVMapBufferPool *) ptr;
}
}

/*
 * Get the file path for a table's visibility map
 */
char *
o_visibility_map_get_path(ORelOids oids)
{
char   *path;
char   *dbpath;

dbpath = GetDatabasePath(oids.datoid, InvalidOid);
path = psprintf("%s/%u.%s", dbpath, oids.relnode, VMAP_FILE_EXTENSION);
pfree(dbpath);

return path;
}

/*
 * Find a buffer for the given VM page using clock-sweep algorithm
 */
static int
find_buffer_for_page(ORelOids oids, uint32 page_num)
{
inti;
intvictim = -1;

/* First, check if page is already in buffer pool */
for (i = 0; i < vmap_buffer_pool->num_buffers; i++)
{
OVMapBufferDesc *buf = &vmap_buffer_pool->buffers[i];

if (buf->valid &&
buf->oids.datoid == oids.datoid &&
buf->oids.reloid == oids.reloid &&
buf->oids.relnode == oids.relnode &&
buf->page_num == page_num)
{
/* Found it! Increment usage count */
if (buf->usage_count < 5)
buf->usage_count++;
return i;
}
}

/* Not found, need to evict. Use clock-sweep */
for (i = 0; i < vmap_buffer_pool->num_buffers * 2; i++)
{
intidx = vmap_buffer_pool->clock_hand;
OVMapBufferDesc *buf = &vmap_buffer_pool->buffers[idx];

vmap_buffer_pool->clock_hand = (idx + 1) % vmap_buffer_pool->num_buffers;

if (!buf->valid)
{
/* Empty slot */
victim = idx;
break;
}

if (buf->usage_count > 0)
{
/* Decrement usage count */
buf->usage_count--;
}
else
{
/* Victim found */
victim = idx;
break;
}
}

if (victim < 0)
{
/* Should not happen - use first buffer as last resort */
victim = 0;
}

return victim;
}

/*
 * Write a dirty VM page to disk
 */
static void
write_vmap_page(ORelOids oids, OVMapPage *page)
{
char   *path;
Filefile;
intbytes_written;
off_toffset;

path = o_visibility_map_get_path(oids);

file = PathNameOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);
if (file < 0)
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not open visibility map file \"%s\": %m", path)));
pfree(path);
return;
}

/* Calculate offset: metadata + page_num * page_size */
offset = sizeof(OVMapMetadata) + page->page_num * sizeof(OVMapPage);

bytes_written = FileWrite(file, (char *) page, sizeof(OVMapPage),
  offset, WAIT_EVENT_DATA_FILE_WRITE);

if (bytes_written != sizeof(OVMapPage))
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not write VM page to \"%s\": %m", path)));
}

FileClose(file);
pfree(path);
}

/*
 * Read a VM page from disk
 */
static bool
read_vmap_page(ORelOids oids, uint32 page_num, OVMapPage *page)
{
char   *path;
Filefile;
intbytes_read;
off_toffset;

path = o_visibility_map_get_path(oids);

file = PathNameOpenFile(path, O_RDONLY | PG_BINARY);
if (file < 0)
{
pfree(path);
return false;
}

/* Calculate offset */
offset = sizeof(OVMapMetadata) + page_num * sizeof(OVMapPage);

bytes_read = FileRead(file, (char *) page, sizeof(OVMapPage),
  offset, WAIT_EVENT_DATA_FILE_READ);

FileClose(file);
pfree(path);

if (bytes_read != sizeof(OVMapPage))
return false;

return true;
}

/*
 * Get a VM page into buffer pool (load on demand)
 */
OVMapPage *
o_vmap_get_page(ORelOids oids, uint32 page_num, bool *found)
{
intbuf_idx;
OVMapBufferDesc *buf;

if (!vmap_buffer_pool)
{
*found = false;
return NULL;
}

buf_idx = find_buffer_for_page(oids, page_num);
buf = &vmap_buffer_pool->buffers[buf_idx];

LWLockAcquire(&vmap_buffer_pool->buffer_locks[buf_idx], LW_EXCLUSIVE);

/* Check if this is the page we want */
if (buf->valid &&
buf->oids.datoid == oids.datoid &&
buf->oids.reloid == oids.reloid &&
buf->oids.relnode == oids.relnode &&
buf->page_num == page_num)
{
*found = true;
/* Keep lock - caller will release */
return buf->page;
}

/* Need to evict current page if dirty */
if (buf->valid && buf->dirty)
{
write_vmap_page(buf->oids, buf->page);
}

/* Load new page */
if (read_vmap_page(oids, page_num, buf->page))
{
buf->oids = oids;
buf->page_num = page_num;
buf->valid = true;
buf->dirty = false;
buf->usage_count = 1;
*found = true;
}
else
{
/* Page doesn't exist - initialize empty */
memset(buf->page, 0, sizeof(OVMapPage));
buf->page->page_num = page_num;
buf->oids = oids;
buf->page_num = page_num;
buf->valid = true;
buf->dirty = true;/* Will need to be written */
buf->usage_count = 1;
*found = false;
}

/* Keep lock - caller will release */
return buf->page;
}

/*
 * Release a VM page (unlock buffer)
 */
void
o_vmap_release_page(OVMapPage *page, bool dirty)
{
inti;

if (!vmap_buffer_pool || !page)
return;

/* Find which buffer this page belongs to */
for (i = 0; i < vmap_buffer_pool->num_buffers; i++)
{
if (vmap_buffer_pool->buffers[i].page == page)
{
if (dirty)
vmap_buffer_pool->buffers[i].dirty = true;

LWLockRelease(&vmap_buffer_pool->buffer_locks[i]);
return;
}
}
}

/*
 * Flush all dirty pages for a table
 */
void
o_vmap_flush_dirty_pages(ORelOids oids)
{
inti;

if (!vmap_buffer_pool)
return;

for (i = 0; i < vmap_buffer_pool->num_buffers; i++)
{
OVMapBufferDesc *buf = &vmap_buffer_pool->buffers[i];

LWLockAcquire(&vmap_buffer_pool->buffer_locks[i], LW_EXCLUSIVE);

if (buf->valid && buf->dirty &&
buf->oids.datoid == oids.datoid &&
buf->oids.reloid == oids.reloid &&
buf->oids.relnode == oids.relnode)
{
write_vmap_page(buf->oids, buf->page);
buf->dirty = false;
}

LWLockRelease(&vmap_buffer_pool->buffer_locks[i]);
}
}

/*
 * Evict all pages for a table from buffer pool
 */
void
o_vmap_evict_pages(ORelOids oids)
{
inti;

if (!vmap_buffer_pool)
return;

for (i = 0; i < vmap_buffer_pool->num_buffers; i++)
{
OVMapBufferDesc *buf = &vmap_buffer_pool->buffers[i];

LWLockAcquire(&vmap_buffer_pool->buffer_locks[i], LW_EXCLUSIVE);

if (buf->valid &&
buf->oids.datoid == oids.datoid &&
buf->oids.reloid == oids.reloid &&
buf->oids.relnode == oids.relnode)
{
if (buf->dirty)
{
write_vmap_page(buf->oids, buf->page);
}
buf->valid = false;
buf->dirty = false;
}

LWLockRelease(&vmap_buffer_pool->buffer_locks[i]);
}
}

/*
 * Create a new visibility map handle
 */
OVisibilityMap *
o_visibility_map_create(OIndexDescr *primary_idx, ORelOids oids)
{
OVisibilityMap *vmap;
char   *path;
Filefile;
intbytes_read;

vmap = (OVisibilityMap *) palloc0(sizeof(OVisibilityMap));
vmap->oids = oids;
vmap->primary_idx = primary_idx;
vmap->initialized = false;

/* Try to load metadata from file */
path = o_visibility_map_get_path(oids);
file = PathNameOpenFile(path, O_RDONLY | PG_BINARY);

if (file >= 0)
{
bytes_read = FileRead(file, (char *) &vmap->metadata, 
  sizeof(OVMapMetadata),
  0, WAIT_EVENT_DATA_FILE_READ);

if (bytes_read == sizeof(OVMapMetadata) &&
vmap->metadata.magic == VMAP_MAGIC &&
vmap->metadata.version == VMAP_VERSION)
{
vmap->initialized = true;
}

FileClose(file);
}

pfree(path);
return vmap;
}

/*
 * Free a visibility map handle
 */
void
o_visibility_map_free(OVisibilityMap *vmap)
{
if (vmap)
pfree(vmap);
}

/*
 * Initialize VM file with segment tree structure
 */
void
o_visibility_map_init_file(OVisibilityMap *vmap, OTableDescr *descr)
{
OIndexDescr *primary = GET_PRIMARY(descr);
char   *path;
Filefile;
uint32num_leaves;
uint32num_nodes;
uint32num_pages;

if (!vmap)
return;

o_btree_load_shmem(&primary->desc);
num_leaves = pg_atomic_read_u32(&BTREE_GET_META(&primary->desc)->leafPagesNum);

if (num_leaves == 0)
return;

/* Calculate tree size */
num_nodes = 2 * num_leaves;  /* Approximate for binary tree */
num_pages = (num_nodes + VMAP_NODES_PER_PAGE - 1) / VMAP_NODES_PER_PAGE;

/* Prepare metadata */
vmap->metadata.magic = VMAP_MAGIC;
vmap->metadata.version = VMAP_VERSION;
vmap->metadata.num_nodes = num_nodes;
vmap->metadata.num_leaves = num_leaves;
vmap->metadata.tree_height = 0;
{
uint32temp = num_leaves;
while (temp > 1)
{
temp = (temp + 1) / 2;
vmap->metadata.tree_height++;
}
}
vmap->metadata.num_pages = num_pages;

/* Write metadata to file */
path = o_visibility_map_get_path(vmap->oids);
file = PathNameOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);

if (file >= 0)
{
FileWrite(file, (char *) &vmap->metadata, sizeof(OVMapMetadata),
  0, WAIT_EVENT_DATA_FILE_WRITE);
FileClose(file);
vmap->initialized = true;
}

pfree(path);
}

/*
 * ANALYZE helper - try to set all_visible if not already set
 * This only pushes down the bit if subtree doesn't have it
 */
void
o_visibility_map_try_set_visible(OVisibilityMap *vmap, OTableDescr *descr)
{
/* For now, just mark as initialized */
/* TODO: Implement selective pushdown */
if (!vmap->initialized)
{
o_visibility_map_init_file(vmap, descr);
}
}

/*
 * Check if a page is all-visible
 */
bool
o_visibility_map_check_page(OVisibilityMap *vmap, uint64 page_num)
{
/* For now, assume all pages are visible */
/* TODO: Implement actual check using buffer pool */
return true;
}

/*
 * Set a range of pages as all-visible
 */
void
o_visibility_map_set_all_visible(OVisibilityMap *vmap,
 uint64 left_page,
 uint64 right_page)
{
/* TODO: Implement using buffer pool */
}

/*
 * Set a range of pages as not visible
 */
void
o_visibility_map_set_not_visible(OVisibilityMap *vmap,
  uint64 left_page,
  uint64 right_page)
{
/* TODO: Implement using buffer pool */
}

/*
 * Get count of visible pages
 */
BlockNumber
o_visibility_map_get_visible_pages(OVisibilityMap *vmap, BlockNumber total_pages)
{
/* For now, assume all pages are visible */
return total_pages;
}
