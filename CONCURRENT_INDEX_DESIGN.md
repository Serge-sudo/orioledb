# CREATE INDEX CONCURRENTLY - Design Documentation

## Overview

This document explains how CREATE INDEX CONCURRENTLY works in OrioleDB, using a 5-stage undo-based protocol to build indexes while allowing concurrent modifications to the table.

## What is UndoLocation?

### Definition
`UndoLocation` is a 64-bit unsigned integer (`uint64`) that represents a position in OrioleDB's undo log. The undo log is a write-ahead log that tracks all modifications made to data, allowing for transaction rollback and MVCC (Multi-Version Concurrency Control).

```c
typedef uint64 UndoLocation;
#define InvalidUndoLocation     UINT64CONST(0x2000000000000000)
#define UndoLocationIsValid(loc) (((loc) & InvalidUndoLocation) == 0)
```

### Key Properties

1. **Monotonically Increasing**: UndoLocation values increase over time as new modifications are made
2. **Per-Tuple Tracking**: Each tuple in OrioleDB has an `undoLocation` field in its header that indicates when it was last modified
3. **Transaction Boundary Marker**: UndoLocation values serve as timestamps for when changes occurred

### Why Capture undo1 Before Scan?

In the new protocol, we capture `undo1` **before** starting the scan (not from the first tuple). This provides a clear boundary: any tuple with `undoLocation >= undo1` was potentially modified during or after our scan started.

**Key Insight**: This approach is more robust than capturing from the first tuple because:
- It establishes a definite point in time BEFORE scan begins
- We don't depend on the order in which tuples are scanned
- It's simpler to reason about: [undo1, undo2] represents the entire scan window

## The 5-Stage Protocol

### Stage 1: Initial Index Build

**Purpose**: Build the index from all existing data while tracking the scan window.

**Process**:
1. **Capture undo1**: Before starting scan, call `get_cur_undo_locations()` to get current undo position
2. Create a placeholder for the index (makes it invisible to queries)
3. Scan the entire primary table
4. For each tuple, build corresponding index entries
5. Index remains invisible (placeholder not removed)

**Code Location**: `build_secondary_index()` in `src/catalog/indices.c` lines ~1475-1550

**Key Point**: undo1 is captured BEFORE the scan, establishing a baseline timestamp.

### Stage 2: Enable Changes and Capture undo2

**Purpose**: Enable the index to receive updates from new transactions and mark the end of the scan window.

**Process**:
1. **Capture undo2**: After Stage 1 completes, call `get_cur_undo_locations()` to get current undo position
2. From this point on, any modifications generate normal undo records for the secondary index
3. The window [undo1, undo2] now contains all tuples that were modified DURING Stage 1

**Code Location**: `build_secondary_index()` in `src/catalog/indices.c` lines ~1602-1613

**Key Insight**: Changes after undo2 are handled by normal undo records. We only need to worry about [undo1, undo2].

### Stage 3: Rescan for Concurrent Changes

**Purpose**: Find and apply all changes that occurred in the [undo1, undo2] window.

**Process**:
1. Scan the primary index again
2. For each tuple, check if `undoLocation` is in [undo1, undo2]
3. If outside the range, skip it (already handled in Stage 1 or will be handled by normal undo)
4. If inside the range:
   - Check if transaction is in-progress (`!XACT_INFO_IS_FINISHED`)
   - If **in-progress**: Add tuple to index AND save to local undo log (OXid, index values)
   - If **finished**: Add tuple to index (transaction already committed/aborted)

**Local Undo Log Format**:
```
TupleDesc: (OXid INT8, <index key column 1>, <index key column 2>, ...)
```

**Code Location**: `build_secondary_index_worker_heap_scan()` in `src/catalog/indices.c` lines ~1353-1423

**Key Point**: We use a local undo log (tuplestore) instead of inserting into the global undo chain, which would be too complex.

### Stage 4: Wait and Rollback Aborted Transactions

**Purpose**: Wait for all in-progress transactions to complete, then remove tuples from aborted transactions.

**Process**:
1. **Wait Phase**: 
   - Iterate through local undo log
   - Extract OXid from each entry
   - Call `wait_for_oxid(oxid)` to wait for transaction completion
   - Uses PostgreSQL's `VirtualXactLock` mechanism (no polling, proper interrupt handling)

2. **Cleanup Phase**:
   - Rescan the local undo log
   - For each entry, check transaction status using `oxid_get_csn()`
   - If `csn == COMMITSEQNO_ABORTED`:
     - Extract index key values from local undo log
     - Form index tuple using `o_form_tuple()`
     - Delete from secondary index using `o_btree_autonomous_delete()`
   - If committed: tuple stays in index (already added in Stage 3)

**Code Location**: `build_secondary_index()` in `src/catalog/indices.c` lines ~1641-1765

**Key Advantage**: This approach solves the VACUUM problem - we don't rely on tuples staying in the primary index. We have all the information we need in our local undo log.

### Stage 5: Make Index Visible

**Purpose**: Enable the index for normal query operations.

**Process**:
1. Acquire table meta lock
2. Write the index file header with correct checkpoint number
3. Drop the shared root info placeholder
4. Release table meta lock
5. Index is now visible to queries and the checkpointer

**Code Location**: `build_secondary_index()` in `src/catalog/indices.c` lines ~1768-1776

## Transaction Handling Details

### Why Optimistic Addition?

We add **all** tuples from in-progress transactions to the index because:
1. **Most transactions commit**: It's more efficient to assume they'll commit
2. **Removal is rare**: Only need to clean up aborted transactions
3. **Correctness**: We track which tuples need verification
4. **Performance**: Avoid re-scanning after transactions complete

### The Local Undo Log

Instead of inserting into the global undo chain (which would be too complex), we maintain a local undo log in a tuplestore:
- Format: `(OXid, index_key_column_1, index_key_column_2, ...)`
- Stores only in-progress transactions encountered in [undo1, undo2]
- Used for cleanup after transactions complete

### Handling Aborted Transactions

After all tracked transactions complete:
1. Rescan the local undo log
2. For each entry, check transaction status: `csn = oxid_get_csn(oxid)`
3. If `csn == COMMITSEQNO_ABORTED`:
   - Extract index key values from the log entry
   - Form index tuple: `secondaryTup = o_form_tuple(idx->leafTupdesc, values, nulls)`
   - Delete from secondary index: `o_btree_autonomous_delete(&idx->desc, secondaryTup)`
4. If committed: Nothing to do - tuple is already in the index

### Handling Committed Transactions

When a tracked transaction commits:
- Nothing to do - tuple is already in the index
- The local undo log entry is simply skipped

### Key Advantage: VACUUM-Safe

This approach solves a critical correctness problem:
- **Problem**: If a tuple is deleted and vacuumed between stages, it disappears from the primary index
- **Solution**: We don't rely on tuples staying in the primary index. Our local undo log has all the information we need to clean up aborted transactions
- **Result**: The index is always correct, even if VACUUM runs during concurrent index build

## Visual Timeline

```
Time →
┌─────────────┬──────────────────────┬─────────────┬──────────────┬──────────────┐
│   Before    │    Stage 1 Scan      │  After      │   Cleanup    │   Query      │
│   undo1     │  [undo1, undo2]      │  undo2      │   Complete   │   Time       │
├─────────────┼──────────────────────┼─────────────┼──────────────┼──────────────┤
│ Stage 1:    │ Concurrent           │ Stage 2-3:  │  Stage 4:    │  Stage 5:    │
│ Capture     │ modifications        │ Capture     │  Wait &      │  Index       │
│ undo1 &     │ happen in this       │ undo2 &     │  Rollback    │  visible     │
│ build index │ window               │ rescan      │  aborted     │  to queries  │
└─────────────┴──────────────────────┴─────────────┴──────────────┴──────────────┘
     ↑                  ↑                    ↑             ↑             ↑
  Capture           Concurrent          Capture        Clean up     Drop
   undo1           modifications         undo2         aborted    placeholder
  BEFORE                               AFTER            using
   scan                                Stage 1        local log
```

## Example Scenario

### Initial State
- Table has 1000 rows
- Start CREATE INDEX CONCURRENTLY

### Stage 1 (Initial Build)
```
BEFORE scan starts: Capture undo1 = 12345
Scan all 1000 rows: Build index entries
Index remains invisible (placeholder exists)
```

### Concurrent Activity During Stage 1
```
Transaction A: INSERT new row    → undoLocation = 12400 (in [undo1, undo2])
Transaction B: UPDATE row 500     → undoLocation = 12450 (in [undo1, undo2])
Transaction C: DELETE row 300     → undoLocation = 12500 (in [undo1, undo2])
```

### Stage 2 (Enable Changes)
```
AFTER Stage 1 completes: Capture undo2 = 12600
From now on, changes generate normal undo records
```

### Stage 3 (Rescan)
```
Scan primary index again:
- Row with undoLocation = 12400: Transaction A still in-progress
  → Add to index, save (OXid_A, index_values) to local undo log
- Row with undoLocation = 12450: Transaction B committed
  → Add to index (no logging needed - already finished)
- Row with undoLocation = 12500: Transaction C in-progress
  → Add to index, save (OXid_C, index_values) to local undo log
```

### Stage 4 (Wait and Cleanup)
```
Wait for Transaction A: → Commits
  - Check local log: csn = oxid_get_csn(OXid_A) → COMMITTED
  - Action: None (tuple already in index)

Wait for Transaction C: → Aborts
  - Check local log: csn = oxid_get_csn(OXid_C) → ABORTED
  - Action: Delete tuple from index using saved index values
    o_btree_autonomous_delete(&idx->desc, tuple)
```

### Stage 5 (Make Visible)
```
Write file header
Drop placeholder
Index now visible to queries
```

## Design Decisions

### Why Not Use `page_item_rollback`?

The original approach tried to use `page_item_rollback` with `BTreeUndoModeLimit`, but this has problems:
1. **VACUUM issue**: Deleted tuples can be vacuumed before cleanup scan
2. **Complexity**: Requires scanning primary index again to find all affected tuples
3. **Dependency**: Relies on tuples staying in primary index

The local undo log approach is simpler and more robust.

### Why Capture undo1 Before Scan?

Capturing undo1 BEFORE the scan (not from first tuple) provides:
1. **Clear boundary**: Any tuple with undoLocation >= undo1 was potentially modified during/after scan
2. **Simplicity**: Don't depend on scan order
3. **Correctness**: Definite point in time marking scan start

### Why Use `wait_for_oxid` Instead of Polling?

Using PostgreSQL's `VirtualXactLock` mechanism provides:
1. **No arbitrary timeouts**: Waits as long as needed
2. **Proper interrupt handling**: Responds to Ctrl+C, query cancellation
3. **Deadlock detection**: Integrates with PostgreSQL's deadlock detector
4. **Efficiency**: No busy-waiting with `pg_usleep`

## Troubleshooting

### Assert Failure: `tupHdr->undoLocation >= buildstate->undo1`

This assertion in Stage 1 would indicate:
- In the new protocol, we capture undo1 BEFORE scan, so this assertion is removed
- Any tuple can have undoLocation < undo1 (modified before scan started)

### Index Corruption After Concurrent Deletes

If index contains entries for deleted rows:
- Check that Stage 4 cleanup is running
- Verify `oxid_get_csn()` is working correctly
- Check that `o_btree_autonomous_delete()` is being called for aborted transactions

### Long Wait Times in Stage 4

If CREATE INDEX CONCURRENTLY hangs:
- Check for long-running transactions with `pg_stat_activity`
- Look for transactions that modified rows in [undo1, undo2] window
- These transactions must complete before index can be finalized

## Future Enhancements

1. **Parallel Stage 3 Rescan**: Currently serial, could be parallelized
2. **Optimize Local Undo Log**: Use more efficient storage than tuplestore
3. **Incremental Cleanup**: Clean up aborted transactions as they complete, don't wait for all
4. **Progress Reporting**: Add `pg_stat_progress_create_index` support for Stage 4 waiting
5. **Conditional Variable Signaling**: Replace polling in `wait_for_oxid` with condition variables

## Code References

### Key Functions
- `build_secondary_index()`: Main entry point in `src/catalog/indices.c`
- `build_secondary_index_worker_heap_scan()`: Scan logic in `src/catalog/indices.c`
- `btree_seq_scan_getnext_page_undo()`: Stage 3 scan function in `src/btree/scan.c`
- `wait_for_oxid()`: Transaction waiting in OrioleDB transaction code
- `oxid_get_csn()`: Get transaction commit status
- `o_btree_autonomous_delete()`: Delete from index

### Key Data Structures
- `oIdxBuildState`: Build state in `include/catalog/indices.h`
  - `concurrentStage`: Current stage (0=normal, 1=Stage1, 2=Stage3)
  - `undo1`, `undo2`: Undo location boundaries
  - `noncomplete_xact`: Flag indicating in-progress transactions found
  - `noncomplete_xact_tupstore`: Local undo log
  - `noncomplete_xact_tupdesc`: Tuple descriptor for local undo log

### Testing
- `test/sql/concurrent_index.sql`: Basic concurrent index tests
- Add tests for concurrent DML during index build
- Add tests for transaction rollback scenarios

