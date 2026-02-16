# Undo Chain Iterator

## Overview

The undo chain iterator provides a way to traverse not only items visible by snapshot but also all items in undo chains. This allows for complete visibility into all versions of tuples stored in OrioleDB B-trees.

## Key Functions

### `btree_iterate_undo_chain()`

Iterates over leaf page tuples including all versions in their undo chains.

```c
OTuple btree_iterate_undo_chain(
    BTreeIterator *it,
    void *end,
    BTreeKeyType endKind,
    bool endInclude,
    bool *scanEnd,
    BTreeLocationHint *hint,
    BTreeLeafTuphdr **tupHdr,
    UndoLocation *undoLocation
);
```

**Parameters:**
- `it`: Iterator state
- `end`, `endKind`, `endInclude`: Scan boundary conditions
- `scanEnd`: Set to true when scan is complete
- `hint`: Optional location hint for optimization
- `tupHdr`: Returns tuple header for current version
- `undoLocation`: Returns undo location, or InvalidUndoLocation if no more versions

**Returns:** Current tuple from leaf page

### `o_walk_undo_chain()`

Walks through all versions of a tuple in its undo chain, calling a callback for each version.

```c
void o_walk_undo_chain(
    BTreeDescr *desc,
    BTreeLeafTuphdr *tupHdr,
    MemoryContext mcxt,
    UndoChainCallback callback,
    void *arg
);
```

**Parameters:**
- `desc`: B-tree descriptor
- `tupHdr`: Initial tuple header (will be modified to contain each version)
- `mcxt`: Memory context for tuple allocations
- `callback`: Function to call for each version (can be NULL)
- `arg`: Argument to pass to callback

**Callback signature:**
```c
typedef bool (*UndoChainCallback)(
    OTuple tuple,
    BTreeLeafTuphdr *tupHdr,
    void *arg
);
```

Returns `false` to stop iteration early, `true` to continue.

## Comparison with Other Iterators

| Iterator | Undo Chain Access | Snapshot Check | Deleted Tuples |
|----------|-------------------|----------------|----------------|
| `btree_iterate_raw` | No | No | Returned as NULL |
| `btree_iterate_all` | No | No | Returned with header |
| `o_btree_iterator_fetch` | Yes | Yes | Filtered by snapshot |
| **`btree_iterate_undo_chain`** | **Yes** | **No** | **All versions returned** |

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
    UndoLocation undoLoc;
    
    OTuple tup = btree_iterate_undo_chain(it, NULL, BTreeKeyNone, false,
                                          &scanEnd, NULL, &tupHdr, &undoLoc);
    
    if (!O_TUPLE_IS_NULL(tup))
    {
        // Process current version
        process_current_version(tup, tupHdr);
        
        // Walk undo chain if available
        if (UndoLocationIsValid(undoLoc))
        {
            BTreeLeafTuphdr undoHdr = *tupHdr;
            o_walk_undo_chain(desc, &undoHdr, CurrentMemoryContext,
                              my_callback, my_arg);
        }
    }
}

btree_iterator_free(it);
```

## Important Notes

1. **No Snapshot Filtering**: Unlike `o_btree_iterator_fetch`, this iterator does NOT apply snapshot visibility checks. ALL versions are returned regardless of transaction state.

2. **Memory Management**: Tuple data returned by the iterator is allocated in the specified memory context. The caller is responsible for freeing it or ensuring the context is reset appropriately.

3. **Lock-Only Records**: The iterator automatically skips lock-only undo records via `find_non_lock_only_undo_record()`.

4. **Deleted Tuples**: Deleted tuples are included in the iteration. Check `tupHdr->deleted` to identify them.

5. **Performance**: Walking undo chains can be expensive for tuples with many versions. Consider limiting iteration depth if needed.

## Example Code

See `doc/undo_chain_iterator_example.c` for comprehensive examples including:
- Simple iteration over all tuples and versions
- Using callbacks to process versions
- Bounded iteration with start/end keys
- Collecting all versions of a specific tuple

## Implementation Details

The implementation consists of two main components:

1. **`btree_iterate_undo_chain`**: Builds on top of `btree_iterate_all` to return current page tuples along with their undo locations.

2. **`o_walk_undo_chain`**: Traverses the undo chain starting from a given tuple header, using the existing undo infrastructure (`get_prev_leaf_header_from_undo` and `get_prev_leaf_header_and_tuple_from_undo`).

The design follows OrioleDB's existing patterns for undo chain traversal (similar to `o_find_tuple_version`) but removes snapshot visibility filtering to provide complete access to all versions.
