# CREATE INDEX CONCURRENTLY - Design Documentation

## Overview

This document explains how CREATE INDEX CONCURRENTLY works in OrioleDB, using a 4-stage undo-based protocol to build indexes while allowing concurrent modifications to the table.

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

### Why First Scan Takes First Locator?

During the first scan (Stage 1), we capture the `undoLocation` from the **first tuple** we encounter and store it as `undo1`. This serves as our baseline timestamp.

**Key Insight**: Since undo locations are monotonically increasing, and we're scanning the table sequentially:
- The first tuple we scan has some `undoLocation` value (call it `undo1`)
- All subsequent tuples will have `undoLocation >= undo1` because:
  - If they were modified BEFORE the first tuple, they would have been scanned first
  - If they were modified AFTER, they naturally have higher undo locations

**The Assert Statement**:
```c
Assert(tupHdr->undoLocation >= buildstate->undo1);
```

This assertion validates that the monotonic property holds during our scan. If this fails, it indicates a bug in the scanning logic or undo log system.

## The 4-Stage Protocol

### Stage 1: Initial Index Build

**Purpose**: Build the index from all existing data while tracking when we started.

**Process**:
1. Create a placeholder for the index (makes it invisible to queries)
2. Scan the entire primary table
3. For the **first tuple** encountered, capture its `undoLocation` as `undo1`
   - This marks the "beginning of time" for our index build
   - Represents the undo log position at the start of our scan
4. Build index entries for all tuples
5. Index remains invisible (placeholder exists)

**Code Location**: `src/catalog/indices.c` lines 1346-1358

```c
if (buildstate->concurrentStage == 1)
{
    /* Remember undo location undo1 for second scan */
    if (!UndoLocationIsValid(buildstate->undo1))
    {
        buildstate->undo1 = tupHdr->undoLocation;  // Capture undo1
    }
    else
    {
        Assert(tupHdr->undoLocation >= buildstate->undo1);  // Verify monotonicity
    }
}
```

**Why undo1 is Important**: It marks the boundary between:
- Tuples we've already indexed (undoLocation < undo1)
- Tuples that might have been modified during our scan (undoLocation >= undo1)

### Stage 2: Enable Change Tracking

**Purpose**: Capture the end of our initial scan and prepare to handle concurrent changes.

**Process**:
1. After Stage 1 completes, capture the **current** undo log position as `undo2`
   ```c
   undoLocations = get_cur_undo_locations(UndoLogRegular);
   buildstate.undo2 = undoLocations.location;
   ```
2. This creates a "window" `[undo1, undo2]` representing:
   - All modifications that occurred during Stage 1 scan
   - These are the "concurrent changes" we need to handle

**Code Location**: `src/catalog/indices.c` lines 1593-1600

**The Critical Window [undo1, undo2]**:
- **Before undo1**: Already indexed in Stage 1
- **Between undo1 and undo2**: Modified during Stage 1 - need special handling
- **After undo2**: Will have normal undo records for the new index

### Stage 3: Rescan for Concurrent Changes

**Purpose**: Handle all modifications that occurred during Stage 1.

**Process**:
1. Scan the primary table again using `btree_seq_scan_getnext_page_undo`
2. For each tuple, check its `undoLocation`:
   
   ```c
   if (tupHdr->undoLocation < buildstate->undo1 || 
       tupHdr->undoLocation > buildstate->undo2)
   {
       // Skip - either already indexed or will be handled normally
       continue;
   }
   ```

3. For tuples in the `[undo1, undo2]` window:
   - **If transaction is complete**: Add to index (already committed/aborted)
   - **If transaction is in-progress**: 
     - Add to index **optimistically**
     - Store tuple info with OXid in tuplestore for later cleanup

**Code Location**: `src/catalog/indices.c` lines 1359-1426

**Optimistic Approach**:
```c
if (!XACT_INFO_IS_FINISHED(tupHdr->OTupleXactInfo))
{
    // Store info about this in-progress transaction
    // But STILL add the tuple to the index (fall through)
}
// Add tuple to index (for both finished and in-progress)
```

4. **Wait for in-progress transactions**:
   - After the scan, wait for all tracked in-progress transactions to complete
   - Use `xid_is_finished(oxid)` with 10-minute timeout
   - Log progress every minute

5. **Clean up aborted transactions**:
   ```c
   if (csn == COMMITSEQNO_ABORTED)
   {
       // Remove tuple from index using o_btree_autonomous_delete
   }
   ```

**Code Location**: `src/catalog/indices.c` lines 1620-1750

### Stage 4: Make Index Visible

**Purpose**: Activate the index for normal use.

**Process**:
1. Write the index file header
2. Drop the placeholder
3. Index becomes visible to the checkpointer
4. Index is now available for queries

**Code Location**: `src/catalog/indices.c` lines 1752-1760

## Transaction Handling Details

### Why Optimistic Addition?

We add **all** tuples from in-progress transactions to the index because:
1. **Most transactions commit**: It's more efficient to assume they'll commit
2. **Removal is rare**: Only need to clean up aborted transactions
3. **Correctness**: We track which tuples need verification
4. **Performance**: Avoid re-scanning after transactions complete

### Handling Aborted Transactions

When a tracked transaction aborts:
1. Extract the stored index values (excluding the OXid we added for tracking)
2. Form the index tuple using `o_form_tuple()`
3. Delete it from the index using `o_btree_autonomous_delete()`
4. Log the removal at DEBUG1 level

### Handling Committed Transactions

When a tracked transaction commits:
- Nothing to do - tuple is already in the index
- Just log at DEBUG1 level for debugging

## Visual Timeline

```
Time →
┌─────────────┬──────────────────────┬─────────────┬──────────────┐
│   Before    │    Stage 1 Scan      │  After      │   Query      │
│   undo1     │  [undo1, undo2]      │  undo2      │   Time       │
├─────────────┼──────────────────────┼─────────────┼──────────────┤
│ Tuples:     │ Tuples:              │ Tuples:     │              │
│ Already     │ Modified during      │ Modified    │ Index        │
│ indexed     │ concurrent scan      │ after scan  │ visible      │
│ in Stage 1  │ (need handling)      │ (has undo)  │ to queries   │
└─────────────┴──────────────────────┴─────────────┴──────────────┘
     ↑                  ↑                    ↑             ↑
  Capture           Concurrent          Capture      Drop
   undo1           modifications         undo2     placeholder
```

## Example Scenario

### Initial State
- Table has 1000 rows
- Start CREATE INDEX CONCURRENTLY

### Stage 1 (Initial Build)
```
Scan row 1:  undoLocation = 12345  → Capture as undo1 = 12345
Scan row 2:  undoLocation = 12346  → undo1 <= 12346 ✓
Scan row 3:  undoLocation = 12347  → undo1 <= 12347 ✓
...
[Meanwhile: Transaction T1 modifies row 500, undoLocation becomes 12450]
...
Scan row 500: undoLocation = 12450 → undo1 <= 12450 ✓ (captures modified version)
...
Scan row 1000: undoLocation = 12999 → undo1 <= 12999 ✓
```
Stage 1 complete. Capture undo2 = 13000 (current undo log position)

### Stage 2 (Rescan Window)
```
Window is [12345, 13000]

Scan row 1:   undoLocation = 12345 → In window, finished transaction → Add
Scan row 500: undoLocation = 12450 → In window, T1 still in progress → Add + Track
...

[Transaction T2 starts and modifies row 600]
Scan row 600: undoLocation = 13100 → OUTSIDE window (> undo2) → Skip
```

### Stage 3 (Wait and Cleanup)
```
Wait for T1 to complete...
T1 commits → Keep row 500 in index
```

### Stage 4 (Activate)
```
Write header, drop placeholder
Index now visible to queries
```

## Key Design Decisions

1. **Why track undo1?**: Marks the baseline - all tuples seen in Stage 1
2. **Why track undo2?**: Marks the cutoff - changes after this have normal undo records
3. **Why the window [undo1, undo2]?**: These changes occurred DURING our scan and need special handling
4. **Why optimistic addition?**: More efficient than pessimistic - most transactions commit
5. **Why autonomous delete?**: Can delete from index without affecting current transaction

## Error Handling

- **Timeout**: 10 minutes waiting for transactions to complete
- **Logging**: Progress logged every minute during waits
- **Cleanup**: Tuplestore and slots properly freed on completion
- **Assertions**: Validate undo location monotonicity

## Files Modified

- `src/catalog/indices.c`: Main implementation
- `src/btree/scan.c`: New scan function for Stage 2
- `include/btree/scan.h`: Function declaration
- `test/sql/concurrent_index.sql`: Basic tests

## Related Concepts

- **MVCC**: Multi-Version Concurrency Control
- **Undo Log**: Transaction rollback mechanism
- **Snapshot Isolation**: How queries see consistent data
- **Autonomous Transactions**: Independent operations within index build

## Future Enhancements

1. Async transaction waiting (use condition variables)
2. Better progress reporting
3. More comprehensive tests for edge cases
4. Handling of very long-running transactions
