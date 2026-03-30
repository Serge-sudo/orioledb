# Batch Tuple Insertion Implementation

## Overview

This implementation extends OrioleDB's B-tree insertion functionality to support adding multiple tuples at once to a single page, instead of the previous one-by-one approach.

## Problem Statement

Previously, tuples were added to OrioleDB tables one by one, using `find_page()` with the modify flag for each tuple. This approach:
- Made multiple function calls per tuple
- Had overhead from repeated page locking/unlocking
- Didn't optimize for cases where multiple tuples fit on the same page

## Solution

Added a new function `o_btree_insert_tuples_to_leaf()` that:
1. Accepts an array of tuples to insert
2. Finds the appropriate location for each tuple on the current page
3. Inserts as many tuples as will fit on the page
4. Returns the count of successfully inserted tuples
5. Allows the caller to find a new page location for remaining tuples

## API

### Function Signature

```c
int o_btree_insert_tuples_to_leaf(
    OBTreeFindPageContext *context,  // Page context with target page already locked
    OTuple *tuples,                  // Array of tuples to insert
    LocationIndex *tuplens,          // Array of tuple lengths
    BTreeLeafTuphdr *leaf_headers,   // Array of tuple headers
    int ntuples,                     // Number of tuples
    int reserve_kind,                // Page pool reservation kind
    int *inserted_count              // Output: number of tuples inserted
);
```

### Return Value

Returns the number of tuples successfully inserted (may be less than `ntuples` if the page runs out of space).

### Usage Pattern

```c
int remaining = ntuples;
int offset = 0;

while (remaining > 0) {
    int inserted;

    // Reserve pages for potential splits
    ppool_reserve_pages(desc->ppool, reserve_kind, 2);

    // Find page for the next tuple
    init_page_find_context(&context, desc, csn,
                          BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_FIX_LEAF_SPLIT);
    find_page(&context, &tuples[offset], BTreeKeyLeafTuple, 0);

    // Insert as many tuples as fit on this page
    o_btree_insert_tuples_to_leaf(&context,
                                   &tuples[offset],
                                   &tuplens[offset],
                                   &headers[offset],
                                   remaining,
                                   reserve_kind,
                                   &inserted);

    offset += inserted;
    remaining -= inserted;
}
```

## Implementation Details

### Key Design Decisions

1. **Single Critical Section**: All tuple insertions on a single page are performed within one critical section, reducing overhead.

2. **Page Space Checking**: Uses `page_locator_fits_item()` to check if each tuple fits before attempting insertion.

3. **Graceful Degradation**: When a tuple doesn't fit, the function stops and returns the count of successfully inserted tuples, allowing the caller to handle the remaining tuples.

4. **Maintains Existing Behavior**: The function follows the same patterns as the existing single-tuple insertion:
   - Proper page locking via the find_page mechanism
   - Chunk splitting when needed via `page_split_chunk_if_needed()`
   - Maintains page header fields like `maxKeyLen` and `prevInsertOffset`
   - Uses same memory context management

### Performance Benefits

- **Reduced Function Call Overhead**: Instead of calling the full insertion stack for each tuple, multiple tuples are processed in one call
- **Optimized Page Operations**: Single critical section for multiple insertions
- **Better Page Utilization**: Fills pages more efficiently by attempting to insert all provided tuples

### Limitations

1. **No Automatic Page Finding**: The caller must handle finding new pages for remaining tuples (by design - allows flexible usage patterns)

2. **No Compaction/Split Handling**: Only handles the `BTreeItemPageFitAsIs` case. If compaction or page splits are needed, those tuples are left for the caller to handle.

3. **Sequential Processing**: Tuples are processed in order; if tuple N doesn't fit, tuples N+1, N+2, etc. are not attempted (even if they might fit).

## Future Enhancements

Possible improvements for future iterations:

1. **Automatic Page Finding Loop**: Create a wrapper function that automatically finds new pages for remaining tuples

2. **Compaction Support**: Handle the `BTreeItemPageFitCompactRequired` case within batch insertion

3. **Smart Tuple Ordering**: Attempt to reorder tuples to maximize page filling efficiency

4. **Parallel Page Insertion**: When multiple pages are needed, consider parallel insertion strategies

## Files Modified

- `include/btree/insert.h`: Added function declaration
- `src/btree/insert.c`: Implemented `o_btree_insert_tuples_to_leaf()`

## Testing

The implementation can be tested by:

1. Creating a table with OrioleDB
2. Inserting multiple rows in a transaction
3. Observing that multiple tuples are added to pages together when space permits

Example test pattern:
```sql
CREATE TABLE test_batch (id int PRIMARY KEY, data text) USING orioledb;
INSERT INTO test_batch SELECT i, repeat('x', 100) FROM generate_series(1, 100) i;
```

## Compatibility

- Fully backward compatible - existing single-tuple insertion still works
- New API is additive, doesn't modify existing behavior
- Can be adopted incrementally by callers who want batch insertion
