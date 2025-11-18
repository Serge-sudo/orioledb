/*-------------------------------------------------------------------------
 *
 * visibility_map.c
 *Segment-tree-based visibility map for OrioleDB tables.
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
#include "btree/iterator.h"
#include "btree/scan.h"
#include "tableam/descr.h"
#include "tableam/visibility_map.h"
#include "utils/rel.h"

#include "common/relpath.h"
#include "storage/fd.h"
#include "utils/memutils.h"

#define VMAP_FILE_EXTENSION "ovm"
#define VMAP_MAGIC 0x4F564D41/* 'OVMA' */
#define VMAP_VERSION 1

/*
 * File header for persistent VM storage
 */
typedef struct OVMapFileHeader
{
uint32magic;umber for validation */
uint32version; */
uint32num_nodes;umber of nodes in tree */
uint32num_leaves;umber of leaf nodes */
uint32tree_height;/* Height of tree */
} OVMapFileHeader;

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
 * Create a new visibility map for a table.
 */
OVisibilityMap *
o_visibility_map_create(OIndexDescr *primary_idx, ORelOids oids)
{
OVisibilityMap *vmap;
MemoryContext mctx;
MemoryContext oldcontext;

/* Create memory context for the VM */
mctx = AllocSetContextCreate(CacheMemoryContext,
 "OrioleDB Visibility Map",
 ALLOCSET_DEFAULT_SIZES);

oldcontext = MemoryContextSwitchTo(mctx);

vmap = (OVisibilityMap *) palloc0(sizeof(OVisibilityMap));
vmap->oids = oids;
vmap->primary_idx = primary_idx;
vmap->tree = NULL;
vmap->vmap_file = -1;
vmap->mctx = mctx;
vmap->loaded = false;

MemoryContextSwitchTo(oldcontext);

return vmap;
}

/*
 * Free a visibility map and its memory context.
 */
void
o_visibility_map_free(OVisibilityMap *vmap)
{
if (vmap)
{
if (vmap->vmap_file >= 0)
{
FileClose(vmap->vmap_file);
vmap->vmap_file = -1;
}
if (vmap->mctx)
MemoryContextDelete(vmap->mctx);
}
}

/*
 * Build a segment tree from leaf page bounds.
 * Returns the index of the node created.
 */
static uint32
build_segment_tree_recursive(OVMapSegmentNode *nodes, uint32 *node_idx,
 uint64 left_bound, uint64 right_bound)
{
uint32current_idx = (*node_idx)++;
uint64mid;

nodes[current_idx].left_bound = left_bound;
nodes[current_idx].right_bound = right_bound;
nodes[current_idx].all_visible = true;/* Initially assume all visible */
nodes[current_idx].lazy_mark = false;

/* Leaf node */
if (left_bound == right_bound)
{
nodes[current_idx].left_child = -1;
nodes[current_idx].right_child = -1;
return current_idx;
}

/* Internal node - create children */
mid = (left_bound + right_bound) / 2;
nodes[current_idx].left_child = build_segment_tree_recursive(nodes, node_idx,
 left_bound, mid);
nodes[current_idx].right_child = build_segment_tree_recursive(nodes, node_idx,
  mid + 1, right_bound);

/* Update all_visible based on children */
nodes[current_idx].all_visible = nodes[nodes[current_idx].left_child].all_visible &&
nodes[nodes[current_idx].right_child].all_visible;

return current_idx;
}

/*
 * Calculate the number of nodes needed for a segment tree with n leaves
 */
static uint32
calculate_tree_size(uint32 num_leaves)
{
/* For a balanced binary tree with n leaves, we need approximately 2*n nodes */
return 2 * num_leaves;
}

/*
 * Build the segment tree structure for the visibility map
 */
void
o_visibility_map_build_tree(OVisibilityMap *vmap, OTableDescr *descr)
{
OIndexDescr *primary = GET_PRIMARY(descr);
OVMapSegmentTree *tree;
uint32num_leaves;
uint32node_idx;
MemoryContext oldcontext;

if (!vmap)
return;

/* Load primary index to get page count */
o_btree_load_shmem(&primary->desc);
num_leaves = pg_atomic_read_u32(&BTREE_GET_META(&primary->desc)->leafPagesNum);

if (num_leaves == 0)
return;

oldcontext = MemoryContextSwitchTo(vmap->mctx);

/* Allocate tree structure */
tree = (OVMapSegmentTree *) palloc0(sizeof(OVMapSegmentTree));
tree->num_leaves = num_leaves;
tree->num_nodes = calculate_tree_size(num_leaves);

/* Allocate nodes array */
tree->nodes = (OVMapSegmentNode *) palloc0(tree->num_nodes * sizeof(OVMapSegmentNode));

/* Build the tree recursively */
node_idx = 0;
build_segment_tree_recursive(tree->nodes, &node_idx, 0, num_leaves - 1);

tree->num_nodes = node_idx;/* Actual number of nodes created */
tree->tree_height = 0;
{
uint32temp = num_leaves;

while (temp > 1)
{
temp = (temp + 1) / 2;
tree->tree_height++;
}
}

tree->dirty = true;

vmap->tree = tree;

MemoryContextSwitchTo(oldcontext);
}

/*
 * Push down lazy propagation marks
 */
void
o_visibility_map_push_lazy(OVisibilityMap *vmap, int32 node_idx)
{
OVMapSegmentNode *node;

if (!vmap || !vmap->tree || node_idx < 0)
return;

node = &vmap->tree->nodes[node_idx];

if (!node->lazy_mark)
return;

/* Mark children as not visible and set their lazy flags */
if (node->left_child >= 0)
{
vmap->tree->nodes[node->left_child].all_visible = false;
vmap->tree->nodes[node->left_child].lazy_mark = true;
}
if (node->right_child >= 0)
{
vmap->tree->nodes[node->right_child].all_visible = false;
vmap->tree->nodes[node->right_child].lazy_mark = true;
}

node->lazy_mark = false;
vmap->tree->dirty = true;
}

/*
 * Update a node's all_visible based on its children
 */
void
o_visibility_map_update_node(OVisibilityMap *vmap, int32 node_idx)
{
OVMapSegmentNode *node;

if (!vmap || !vmap->tree || node_idx < 0)
return;

node = &vmap->tree->nodes[node_idx];

/* Leaf node - nothing to update */
if (node->left_child < 0 && node->right_child < 0)
return;

/* Internal node - AND children's visibility */
if (node->left_child >= 0 && node->right_child >= 0)
{
node->all_visible = vmap->tree->nodes[node->left_child].all_visible &&
vmap->tree->nodes[node->right_child].all_visible;
vmap->tree->dirty = true;
}
}

/*
 * Mark a range of pages as visible or not (with lazy propagation)
 */
static void
mark_page_range_recursive(OVisibilityMap *vmap, int32 node_idx,
  uint64 query_left, uint64 query_right,
  bool all_visible)
{
OVMapSegmentNode *node;

if (node_idx < 0 || !vmap || !vmap->tree)
return;

node = &vmap->tree->nodes[node_idx];

/* Push down any pending lazy marks */
o_visibility_map_push_lazy(vmap, node_idx);

/* No overlap */
if (query_right < node->left_bound || query_left > node->right_bound)
return;

/* Complete overlap - mark this node */
if (query_left <= node->left_bound && query_right >= node->right_bound)
{
node->all_visible = all_visible;
if (!all_visible && (node->left_child >= 0 || node->right_child >= 0))
{
node->lazy_mark = true;
}
vmap->tree->dirty = true;
return;
}

/* Partial overlap - recurse to children */
if (node->left_child >= 0)
mark_page_range_recursive(vmap, node->left_child, query_left, query_right, all_visible);
if (node->right_child >= 0)
mark_page_range_recursive(vmap, node->right_child, query_left, query_right, all_visible);

/* Update this node based on children */
o_visibility_map_update_node(vmap, node_idx);
}

/*
 * Mark a range of pages as all-visible or not
 */
void
o_visibility_map_mark_page_range(OVisibilityMap *vmap,
 uint64 left_page,
 uint64 right_page,
 bool all_visible)
{
if (!vmap || !vmap->tree)
return;

mark_page_range_recursive(vmap, 0, left_page, right_page, all_visible);
}

/*
 * Check if a page is in an all-visible range
 */
static bool
check_page_recursive(OVisibilityMap *vmap, int32 node_idx, uint64 page_num)
{
OVMapSegmentNode *node;

if (node_idx < 0 || !vmap || !vmap->tree)
return false;

node = &vmap->tree->nodes[node_idx];

/* Push down lazy marks before checking */
o_visibility_map_push_lazy(vmap, node_idx);

/* Outside range */
if (page_num < node->left_bound || page_num > node->right_bound)
return false;

/* If this node covers the page and is all visible, return true */
if (node->all_visible)
return true;

/* If this is a leaf and not visible, return false */
if (node->left_child < 0 && node->right_child < 0)
return node->all_visible;

/* Check children */
if (node->left_child >= 0 &&
check_page_recursive(vmap, node->left_child, page_num))
return true;

if (node->right_child >= 0 &&
check_page_recursive(vmap, node->right_child, page_num))
return true;

return false;
}

/*
 * Check if a specific page is all-visible
 */
bool
o_visibility_map_check_page(OVisibilityMap *vmap, uint64 page_num)
{
if (!vmap || !vmap->tree)
return false;

return check_page_recursive(vmap, 0, page_num);
}

/*
 * Load visibility map from disk
 */
void
o_visibility_map_load(OVisibilityMap *vmap)
{
char   *path;
Filefile;
OVMapFileHeader header;
intbytes_read;
MemoryContext oldcontext;

if (!vmap || vmap->loaded)
return;

path = o_visibility_map_get_path(vmap->oids);

/* Try to open existing file */
file = PathNameOpenFile(path, O_RDONLY | PG_BINARY);
if (file < 0)
{
/* File doesn't exist - that's okay, we'll build it */
pfree(path);
return;
}

oldcontext = MemoryContextSwitchTo(vmap->mctx);

/* Read header */
bytes_read = FileRead(file, (char *) &header, sizeof(header),
  0, WAIT_EVENT_DATA_FILE_READ);

if (bytes_read != sizeof(header) || header.magic != VMAP_MAGIC ||
header.version != VMAP_VERSION)
{
/* Invalid file - close and return */
FileClose(file);
MemoryContextSwitchTo(oldcontext);
pfree(path);
return;
}

/* Allocate tree structure */
vmap->tree = (OVMapSegmentTree *) palloc0(sizeof(OVMapSegmentTree));
vmap->tree->num_nodes = header.num_nodes;
vmap->tree->num_leaves = header.num_leaves;
vmap->tree->tree_height = header.tree_height;
vmap->tree->dirty = false;

/* Read nodes */
vmap->tree->nodes = (OVMapSegmentNode *) palloc0(header.num_nodes * sizeof(OVMapSegmentNode));
bytes_read = FileRead(file, (char *) vmap->tree->nodes,
  header.num_nodes * sizeof(OVMapSegmentNode),
  sizeof(header), WAIT_EVENT_DATA_FILE_READ);

if (bytes_read != header.num_nodes * sizeof(OVMapSegmentNode))
{
/* Read error - free and return */
pfree(vmap->tree->nodes);
pfree(vmap->tree);
vmap->tree = NULL;
FileClose(file);
MemoryContextSwitchTo(oldcontext);
pfree(path);
return;
}

FileClose(file);
vmap->loaded = true;

MemoryContextSwitchTo(oldcontext);
pfree(path);
}

/*
 * Flush visibility map to disk
 */
void
o_visibility_map_flush(OVisibilityMap *vmap)
{
char   *path;
Filefile;
OVMapFileHeader header;
intbytes_written;

if (!vmap || !vmap->tree || !vmap->tree->dirty)
return;

path = o_visibility_map_get_path(vmap->oids);

/* Create/open file for writing */
file = PathNameOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);
if (file < 0)
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not create visibility map file \"%s\": %m", path)));
pfree(path);
return;
}

/* Prepare header */
header.magic = VMAP_MAGIC;
header.version = VMAP_VERSION;
header.num_nodes = vmap->tree->num_nodes;
header.num_leaves = vmap->tree->num_leaves;
header.tree_height = vmap->tree->tree_height;

/* Write header */
bytes_written = FileWrite(file, (char *) &header, sizeof(header),
  0, WAIT_EVENT_DATA_FILE_WRITE);

if (bytes_written != sizeof(header))
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not write visibility map header to \"%s\": %m", path)));
FileClose(file);
pfree(path);
return;
}

/* Write nodes */
bytes_written = FileWrite(file, (char *) vmap->tree->nodes,
  vmap->tree->num_nodes * sizeof(OVMapSegmentNode),
  sizeof(header), WAIT_EVENT_DATA_FILE_WRITE);

if (bytes_written != vmap->tree->num_nodes * sizeof(OVMapSegmentNode))
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not write visibility map nodes to \"%s\": %m", path)));
FileClose(file);
pfree(path);
return;
}

/* Sync to disk */
if (FileSync(file, WAIT_EVENT_DATA_FILE_SYNC) != 0)
{
ereport(WARNING,
(errcode_for_file_access(),
 errmsg("could not sync visibility map file \"%s\": %m", path)));
}

FileClose(file);
vmap->tree->dirty = false;

pfree(path);
}

/*
 * Build visibility map by scanning the primary index
 */
void
o_visibility_map_build(OVisibilityMap *vmap, OTableDescr *descr)
{
if (!vmap)
return;

/* Try to load existing VM from disk */
o_visibility_map_load(vmap);

/* If not loaded or invalid, build a new tree */
if (!vmap->tree)
{
o_visibility_map_build_tree(vmap, descr);

/* Mark all pages as visible initially (assuming committed data) */
if (vmap->tree && vmap->tree->num_leaves > 0)
{
o_visibility_map_mark_page_range(vmap, 0, vmap->tree->num_leaves - 1, true);
}

/* Flush to disk */
o_visibility_map_flush(vmap);
}
}

/*
 * Calculate how many pages are all-visible
 */
static uint32
count_visible_pages_recursive(OVisibilityMap *vmap, int32 node_idx)
{
OVMapSegmentNode *node;
uint32count = 0;

if (node_idx < 0 || !vmap || !vmap->tree)
return 0;

node = &vmap->tree->nodes[node_idx];

/* Push down lazy marks */
o_visibility_map_push_lazy(vmap, node_idx);

/* If entire subtree is visible, count all leaves under it */
if (node->all_visible)
{
/* Calculate number of leaves in this subtree */
return node->right_bound - node->left_bound + 1;
}

/* If this is a leaf and not visible, count 0 */
if (node->left_child < 0 && node->right_child < 0)
return 0;

/* Recurse to children */
if (node->left_child >= 0)
count += count_visible_pages_recursive(vmap, node->left_child);
if (node->right_child >= 0)
count += count_visible_pages_recursive(vmap, node->right_child);

return count;
}

/*
 * Get the count of visible pages from the VM
 */
BlockNumber
o_visibility_map_get_visible_pages(OVisibilityMap *vmap, BlockNumber total_pages)
{
uint32visible_count;

if (!vmap || !vmap->tree)
return 0;

visible_count = count_visible_pages_recursive(vmap, 0);

/* Don't report more visible pages than total pages */
if (visible_count > total_pages)
return total_pages;

return (BlockNumber) visible_count;
}
