# Concurrent Index Build Validation Implementation

## Overview
This document describes the implementation of improved concurrent index build validation
as specified in the requirements. The implementation adds infrastructure for tracking
validation progress via a monotonically increasing boundary variable.

## Current Implementation Status

### Completed Infrastructure

1. **Validation Boundary Variable** (`validation_boundary` in `BTreeMetaPage`)
   - Added to `BTreeMetaPage` structure in `include/btree/page_contents.h`
   - Initialized in `src/btree/page_contents.c` and `src/btree/build.c`
   - Persisted via `CheckpointFileHeader` in `include/checkpoint/checkpoint.h`
   - Read/written during checkpoint operations in `src/checkpoint/checkpoint.c`
   - Also handled in `src/btree/io.c` for I/O operations

2. **Boundary Management Functions** (in `src/btree/btree.c`)
   - `btree_get_validation_boundary()` - Read current validation boundary
   - `btree_set_validation_boundary()` - Update validation boundary
   - `btree_check_pk_against_boundary()` - Check if PK modification is allowed (placeholder)

3. **Validation Phase Updates** (in `src/tableam/handler.c`)
   - Modified `orioledb_index_validate_scan()` to:
     - Initialize boundary to 0 at start of validation
     - Update boundary periodically (every 1000 tuples) as validation progresses
     - Set boundary to UINT64_MAX when validation completes

4. **Undo Infrastructure**
   - Added `ValidationBoundaryUndoItemType` to `UndoItemType` enum

## Remaining Work

### 1. PK Encoding and Comparison
The current `btree_check_pk_against_boundary()` function uses a placeholder for PK encoding.
This needs to be implemented based on the actual primary key structure:

```c
// TODO: Implement proper pk encoding based on index structure
// Should convert OTuple primary key to uint64 for comparison
// May need to handle multi-column PKs and different data types
```

### 2. Boundary Checking in Secondary Index Modifications
Need to modify `o_update_secondary_index()` in `src/tableam/operations.c` to:
- Extract the primary key from the slot being modified
- Check it against the validation boundary using `btree_check_pk_against_boundary()`
- Only proceed with secondary index modification if PK < boundary
- Store the boundary value at modification time for rollback handling

### 3. Transaction Rollback Logic (3 Cases)
Implement the three-case rollback logic as specified:

**Case 1: PK was less than boundary when added**
- Transaction added the tuple to secondary index
- Has undo record
- Normal rollback - nothing special needed

**Case 2: PK was greater than boundary when added, but less during rollback**
- Validator added the tuple (not transaction)
- No undo record for it
- Need to rollback from secondary index alongside primary
- Requires checking current boundary vs. stored boundary

**Case 3: PK is still greater than current boundary**
- Neither transaction nor validator added anything
- Do nothing on rollback

Implementation approach:
```c
typedef struct
{
    UndoStackItem header;
    uint64 pk_encoded;           // Encoded PK value
    uint64 boundary_at_modify;   // Boundary value when modification occurred
    ORelOids index_oids;         // Index being modified
} ValidationBoundaryUndoItem;
```

### 4. Undo Callback for Validation Boundary
Add a callback function similar to `modify_undo_callback()` in `src/btree/undo.c`:

```c
void
validation_boundary_undo_callback(UndoLogType undoType, UndoLocation location,
                                 Pointer data, Size size, void *arg)
{
    ValidationBoundaryUndoItem *item = (ValidationBoundaryUndoItem *) data;
    uint64 current_boundary = btree_get_validation_boundary(&desc);
    
    // Implement 3-case logic here
    if (item->pk_encoded < item->boundary_at_modify)
    {
        // Case 1: Normal rollback, already handled
    }
    else if (item->pk_encoded < current_boundary)
    {
        // Case 2: Need to remove from secondary index
        // Call delete operation on secondary index
    }
    else
    {
        // Case 3: Do nothing
    }
}
```

### 5. Integration Points

**In `o_update_secondary_index()`:**
```c
// Before modification
uint64 boundary = btree_get_validation_boundary(&id->desc);
OTuple pk = extract_pk_from_slot(newSlot, descr);
uint64 pk_encoded = encode_pk_for_comparison(pk);

if (pk_encoded >= boundary && boundary != UINT64_MAX)
{
    // Modification not allowed yet, validation hasn't reached this PK
    // Add validation boundary undo item for rollback handling
    add_validation_boundary_undo_item(pk_encoded, boundary, id->desc.oids);
    return res; // Skip modification
}

// Proceed with normal modification
```

## Testing Strategy

1. **Unit Tests**
   - Test boundary initialization and updates
   - Test PK encoding/comparison functions
   - Test boundary checking logic

2. **Concurrent Tests**
   - Create concurrent index during active transactions
   - Verify transactions can only modify secondary index for validated PKs
   - Test rollback scenarios for all 3 cases

3. **Edge Cases**
   - Validation boundary overflow (very large tables)
   - Multi-column primary keys
   - Different PK data types
   - Rollback during validation

## Performance Considerations

1. **Boundary Update Frequency**
   - Currently updating every 1000 tuples
   - May need tuning based on performance testing
   - Could use page-based boundaries instead

2. **PK Encoding Overhead**
   - Need efficient encoding scheme for comparison
   - Consider caching encoded PK values

3. **Undo Record Size**
   - ValidationBoundaryUndoItem adds overhead per secondary index modification
   - Only needed during validation phase

## Future Enhancements

1. **Page-Level Boundaries**
   - Instead of tuple-based encoding, use page high keys
   - Matches problem statement: "highkey of the page currently being checked"
   - More efficient for large tables

2. **Multiple Index Validation**
   - Support concurrent validation of multiple secondary indexes
   - Separate boundary per index

3. **Validation Progress Reporting**
   - Expose boundary via SQL function for monitoring
   - Integration with pg_stat_progress_create_index
