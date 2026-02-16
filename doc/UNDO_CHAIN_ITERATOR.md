# Undo Chain Iterator

## Overview

The undo chain iterator provides a way to traverse not only items visible by snapshot but also all items in undo chains. This allows for complete visibility into all versions of tuples stored in OrioleDB B-trees.

The iterator is **stateful** and returns one version at a time through repeated calls. It automatically handles:
- Current versions from leaf pages
- All historical versions from undo chains
- Deleted and lock-only records

## Key Function

### `btree_iterate_undo_chain()`

Iterates over leaf page tuples including all versions in their undo chains, returning one version per call.

```c
OTuple btree_iterate_undo_chain(
    BTreeIterator *it,
    void *end,
    BTreeKeyType endKind,
    bool endInclude,
    bool *scanEnd,
    BTreeLocationHint *hint,
    BTreeLeafTuphdr **tupHdr
);
```

**Parameters:**
- `it`: Iterator state
- `end`, `endKind`, `endInclude`: Scan boundary conditions
- `scanEnd`: Set to true when scan is complete
- `hint`: Optional location hint for optimization
- `tupHdr`: Returns tuple header for current version

**Returns:** OTuple for the current version (may be NULL for deleted/lock-only versions)

**Behavior:**
1. First call returns the current version from the leaf page
2. Subsequent calls return versions from the undo chain (oldest to newest)
3. Automatically moves to next tuple when undo chain is exhausted
4. Returns NULL with `*scanEnd = true` when iteration is complete

## Comparison with Other Iterators

| Iterator | Undo Chain Access | Snapshot Check | Deleted Tuples | Returns All Versions |
|----------|-------------------|----------------|----------------|----------------------|
| `btree_iterate_raw` | No | No | Returned as NULL | No |
| `btree_iterate_all` | No | No | Returned with header | No |
| `o_btree_iterator_fetch` | Yes | Yes | Filtered by snapshot | No |
| **`btree_iterate_undo_chain`** | **Yes** | **No** | **All versions returned** | **Yes** |

## Use Cases

### 1. Debugging and Analysis
Examine all versions of tuples to understand transaction history and diagnose issues.

### 2. Auditing
Track all changes to data over time, regardless of transaction state.

### 3. MVCC Implementation
Build custom multi-version concurrency control logic.

### 4. Data Recovery
Access historical versions of data for recovery purposes.

### 5. System Utilities
Implement system-level operations that need complete visibility into all tuple versions.

## Basic Usage Pattern

```c
BTreeIterator *it = o_btree_iterator_create(desc, NULL, BTreeKeyNone,
                                             NULL, ForwardScanDirection);
bool scanEnd = false;

while (!scanEnd)
{
    BTreeLeafTuphdr *tupHdr;
    
    OTuple tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
                                          &scanEnd, NULL, &tupHdr);
    
    if (!O_TUPLE_IS_NULL(tup) || tupHdr != NULL)
    {
        // Process each version (current + all undo versions)
        // Check tupHdr->deleted to identify deleted tuples
        process_version(tup, tupHdr);
    }
}

btree_iterator_free(it);
```

## Important Notes

1. **Stateful Iterator**: The iterator maintains state to track position in undo chains. Each call returns the next version automatically.

2. **No Snapshot Filtering**: Unlike `o_btree_iterator_fetch`, this iterator does NOT apply snapshot visibility checks. ALL versions are returned regardless of transaction state.

3. **Memory Management**: The iterator manages memory internally. Tuple data is freed automatically when moving to the next tuple or when the iterator is freed.

4. **Lock-Only Records**: The iterator automatically skips lock-only undo records via `find_non_lock_only_undo_record()`.

5. **Deleted Tuples**: Deleted tuples are included in the iteration. Check `tupHdr->deleted` to identify them.

6. **Performance**: Walking undo chains can be expensive for tuples with many versions. Consider limiting iteration depth if needed.

## Implementation Details

The implementation uses a stateful iterator approach:

1. **State Management**: The `BTreeIterator` structure contains fields to track:
   - `processingUndoChain`: Whether currently iterating through an undo chain
   - `undoChainTupHdr`: Current version's tuple header
   - `undoChainTuple`: Current version's tuple data
   - `undoChainTupleAllocated`: Memory allocation tracking

2. **Iteration Flow**:
   - First call returns current version from leaf page
   - Subsequent calls return versions from undo chain
   - Automatically moves to next tuple when chain is exhausted
   - Uses existing undo infrastructure (`get_prev_leaf_header_from_undo` and `get_prev_leaf_header_and_tuple_from_undo`)

The design follows OrioleDB's existing patterns for undo chain traversal (similar to `o_find_tuple_version`) but removes snapshot visibility filtering to provide complete access to all versions.
