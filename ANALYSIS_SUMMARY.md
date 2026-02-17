# Quick Summary: Lock Undo in Secondary Indexes

## Question
Is it possible to have lock undo in secondary indexes?

## Answer
**NO** - Lock undo is NOT possible in secondary indexes, and this is the correct design.

## Why?

### Technical Reason
- Row locks are only applied to the **primary index** via `o_tbl_lock()` 
- `BTreeOperationLock` is never called for secondary indexes
- Only primary index operations create `RowLockUndoItemType` undo records
- Secondary indexes only create `ModifyUndoItemType` undo records for insert/update/delete

### Architectural Reason
- Row locks conceptually apply to **table rows**, not individual index entries
- The primary index stores the actual row data
- Secondary indexes only store keys + primary key references
- Locking the primary index automatically protects all secondary index entries for that row

## Evidence
See `LOCK_UNDO_ANALYSIS.md` for detailed code analysis and references.

## Code Location
The key function `o_tbl_lock()` at `src/tableam/operations.c:375` shows:
```c
res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationLock, ...);
```
Note: It only operates on `GET_PRIMARY(descr)->desc`, never on secondary indexes.

## Conclusion
This is **correct behavior**, not a bug or limitation. The design ensures that row-level locking works consistently while secondary indexes are protected through the primary index lock.
