# Analysis: Lock Undo in Secondary Indexes

## Executive Summary

**Question:** Is it possible to have lock undo in secondary indexes?

**Answer:** **NO**, lock undo is NOT possible in secondary indexes in the current OrioleDB implementation. Row-level locks are only applied to the primary index, and therefore lock undo records (`RowLockUndoItemType`) can only exist for primary index operations.

## Detailed Analysis

### 1. Row Lock Operations Are Primary-Index-Only

The function `o_tbl_lock()` in `src/tableam/operations.c` (lines 355-383) is the entry point for row-level locking operations:

```c
OBTreeModifyResult
o_tbl_lock(OTableDescr *descr, OBTreeKeyBound *pkey, LockTupleMode mode,
           OXid oxid, OLockCallbackArg *larg, BTreeLocationHint *hint)
{
    // ...
    o_btree_load_shmem(&GET_PRIMARY(descr)->desc);  // Line 370
    
    lock_mode = tuple_lock_mode_to_row_lock_mode(mode);
    
    O_TUPLE_SET_NULL(nullTup);
    res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationLock,  // Line 375
                         nullTup, BTreeKeyNone, (Pointer) pkey, BTreeKeyBound,
                         oxid, larg->csn, lock_mode,
                         hint, &callbackInfo);
    
    return res;
}
```

**Key observation:** Line 375 shows that `BTreeOperationLock` is ONLY called on `GET_PRIMARY(descr)->desc`, which is the primary index. There is no corresponding code path that applies `BTreeOperationLock` to secondary indexes.

### 2. Secondary Indexes Only Support Insert/Update/Delete Operations

When secondary indexes are modified during table operations, they only use these operations:

- **Insert:** `src/tableam/operations.c`, line 1479 - `o_btree_modify(bd, BTreeOperationInsert, ...)`
- **Delete:** `src/tableam/operations.c`, line 1329 - `o_btree_modify(&id->desc, BTreeOperationDelete, ...)`
- **Update:** Implemented as a delete followed by an insert

There is no code path that calls `o_btree_modify()` with `BTreeOperationLock` for secondary indexes.

### 3. Lock Undo Record Creation

In `src/btree/undo.c` (lines 324-327), lock undo records are created as follows:

```c
if (action == BTreeOperationLock)
    item->header.type = RowLockUndoItemType;
else
    item->header.type = ModifyUndoItemType;
```

Since `BTreeOperationLock` is never passed for secondary index operations, `RowLockUndoItemType` undo records are never created for secondary indexes.

### 4. Lock Undo Callback Registration

In `src/transam/undo.c` (lines 108-111), the lock undo callback is registered:

```c
{
    .type = RowLockUndoItemType,
    .callback = lock_undo_callback,
    .callOnCommit = false
},
```

The `lock_undo_callback` function in `src/btree/undo.c` (lines 774-850) handles rollback of row locks. However, since `RowLockUndoItemType` records are only created for primary index locks, this callback is also only invoked for primary index operations.

### 5. Secondary Index Undo Records

Secondary indexes DO have undo support, but only for modify operations:

- Secondary indexes use `UndoLogRegular` undo type (assigned in `src/tableam/tree.c`, line 124)
- They create `ModifyUndoItemType` undo records for insert/update/delete operations
- They also create `SecondaryIndexUndoItemType` undo records during concurrent index validation (lines 415-463 in `src/btree/undo.c`)

But they do NOT create `RowLockUndoItemType` records because they do not support the `BTreeOperationLock` operation.

### 6. Why This Design Makes Sense

This design is logical because:

1. **Row-level locks are conceptually applied to table rows, not index entries:** A lock on a row should prevent concurrent modifications to that row, regardless of which index is used to access it.

2. **Primary index represents the row:** In OrioleDB, the primary index stores the actual table tuple data, while secondary indexes only store index keys plus a reference to the primary key.

3. **Locking the primary index is sufficient:** Since all row modifications must update the primary index (which contains the actual row data), locking at the primary index level provides complete protection for the row.

4. **Secondary indexes don't need row locks:** Secondary index entries are implicitly protected by the lock on the corresponding primary index entry. When a row is locked via the primary index, any attempt to modify secondary index entries for that row will detect the lock through the primary index.

## Conclusion

**Lock undo is NOT possible in secondary indexes by design.** The current implementation:
- Only applies `BTreeOperationLock` to primary indexes
- Only creates `RowLockUndoItemType` undo records for primary index locks
- Protects secondary index entries implicitly through primary index locks

This design is architecturally sound and does not represent a limitation or bug. Secondary indexes correctly participate in transaction rollback through `ModifyUndoItemType` and `SecondaryIndexUndoItemType` undo records, which handle insert/update/delete operations but not lock operations.

## Code References

1. **Row lock function:** `src/tableam/operations.c:355-383`
2. **Lock undo record creation:** `src/btree/undo.c:324-327`
3. **Lock undo callback:** `src/btree/undo.c:774-850`
4. **Lock undo registration:** `src/transam/undo.c:108-111`
5. **Secondary index operations:** `src/tableam/operations.c:1479, 1329`
6. **Secondary index undo:** `src/btree/undo.c:415-550`
7. **Undo type assignment:** `src/tableam/tree.c:124`
