# OrioleDB System Trees Architecture Diagram

This document provides visual representations of how OrioleDB system trees work and how they differ from PostgreSQL's catalog system.

## 1. PostgreSQL vs OrioleDB Metadata Storage

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Approach                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Application                                                          │
│      │                                                                │
│      ├── CREATE INDEX → pg_index (heap table)                        │
│      │                    ↓                                           │
│      │              Write to disk as heap tuple                      │
│      │              (blocknum, offset)                               │
│      │                    ↓                                           │
│      │              SysCache invalidation                            │
│      │                    ↓                                           │
│      └── SELECT → Read from pg_index via syscache                    │
│                   Sequential scan or index scan                      │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    OrioleDB Approach                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Application                                                          │
│      │                                                                │
│      ├── CREATE INDEX → o_indices_add()                              │
│      │                    ↓                                           │
│      │              Serialize OIndex structure                       │
│      │                    ↓                                           │
│      │              Split into TOAST chunks                          │
│      │                    ↓                                           │
│      │              Insert into SYS_TREES_O_INDICES (B-tree)         │
│      │                    ↓                                           │
│      │              Create UNDO log records                          │
│      │                    ↓                                           │
│      └── SELECT → o_indices_get()                                    │
│                    ↓                                                  │
│              B-tree lookup by (type, oids)                           │
│                    ↓                                                  │
│              Fetch chunks, deserialize OIndex                        │
│                    ↓                                                  │
│              Apply MVCC visibility (check CSN)                       │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## 2. System Trees Hierarchy

```
┌──────────────────────────────────────────────────────────────────────┐
│                    OrioleDB System Trees                              │
│                    (Database: datoid = 1)                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  SYS_TREES_SHARED_ROOT_INFO (1)                                       │
│  ├─ Stores root page info for all trees                              │
│  └─ Key: (datoid, relnode) → Value: BTreeRootInfo                    │
│                                                                        │
│  SYS_TREES_O_TABLES (2)                                               │
│  ├─ Table metadata (columns, constraints, options)                   │
│  └─ Key: (type, datoid, reloid, relnode, chunknum)                   │
│                                                                        │
│  SYS_TREES_O_INDICES (3) ◄── YOUR FOCUS AREA                         │
│  ├─ Index metadata (fields, predicates, flags)                       │
│  ├─ Key: (type, datoid, reloid, relnode, chunknum)                   │
│  └─ Value: OIndexChunk (serialized OIndex)                           │
│                                                                        │
│  SYS_TREES_OPCLASS_CACHE (4)                                          │
│  ├─ Operator class cache from pg_opclass                             │
│  └─ Key: opclassoid → Value: cached metadata                         │
│                                                                        │
│  SYS_TREES_ENUM_CACHE (5)                                             │
│  ├─ Enum type cache from pg_enum                                     │
│  └─ Key: enumoid → Value: cached metadata                            │
│                                                                        │
│  ... (18 more system trees for various caches and metadata)          │
│                                                                        │
│  SYS_TREES_EXTENTS_OFF_LEN (9)                                        │
│  ├─ Free space extents sorted by offset                              │
│  └─ For compressed tree space management                             │
│                                                                        │
│  SYS_TREES_EXTENTS_LEN_OFF (10)                                       │
│  ├─ Free space extents sorted by length                              │
│  └─ For finding best-fit free blocks                                 │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 3. OIndex Structure Layout in Memory

```
┌──────────────────────────────────────────────────────────────────────┐
│                        OIndex Structure                               │
│               (Stored in SYS_TREES_O_INDICES)                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Fixed-size section (serialized to system tree):                     │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │ indexOids:          {datoid, reloid, relnode}              │      │
│  │ indexType:          oIndexPrimary/Secondary/TOAST/Bridge   │      │
│  │ tableOids:          {datoid, reloid, relnode}              │      │
│  │ table_persistence:  'p'/'t'/'u'                            │      │
│  │ fillfactor:         90 (default)                           │      │
│  │ data_version:       3 (schema version)                     │      │
│  │ createOxid:         Transaction ID that created index      │      │
│  │ name:               Index name (64 bytes)                  │      │
│  │ primaryIsCtid:      bool                                   │      │
│  │ bridging:           bool ◄── EXISTING FLAG EXAMPLE         │      │
│  │ compress:           NONE/LZ4/ZSTD                          │      │
│  │ nulls_not_distinct: bool                                   │      │
│  │                                                             │      │
│  │ ┌─────────────────────────────────────────────────────┐   │      │
│  │ │ *** ADD YOUR FLAGS HERE ***                         │   │      │
│  │ │ indislive:        bool                               │   │      │
│  │ │ indisready:       bool                               │   │      │
│  │ │ indisvalid:       bool                               │   │      │
│  │ └─────────────────────────────────────────────────────┘   │      │
│  │                                                             │      │
│  │ nIncludedFields:    Number of INCLUDE columns             │      │
│  │ nLeafFields:        Total leaf fields                     │      │
│  │ nNonLeafFields:     Total non-leaf fields                 │      │
│  │ nUniqueFields:      Unique constraint fields              │      │
│  │ nKeyFields:         Index key fields                      │      │
│  │ nPrimaryFields:     Primary key fields                    │      │
│  │ primaryFieldsAttnums[INDEX_MAX_KEYS]: Field mappings     │      │
│  └────────────────────────────────────────────────────────────┘      │
│                                                                        │
│  Variable-size section (palloc'ed after deserialization):             │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │ leafTableFields:    OTableField* (array)                   │      │
│  │ leafFields:         OTableIndexField* (array)              │      │
│  │ predicate:          List* (WHERE clause, parsed)           │      │
│  │ predicate_str:      char* (serialized predicate)           │      │
│  │ expressions:        List* (index expressions)              │      │
│  │ duplicates:         List* (duplicate field tracking)       │      │
│  │ tablespace:         Oid (tablespace ID)                    │      │
│  │ index_mctx:         MemoryContext                          │      │
│  └────────────────────────────────────────────────────────────┘      │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 4. Index Creation Lifecycle with Flags

```
┌──────────────────────────────────────────────────────────────────────┐
│                  Index Creation Lifecycle                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. CREATE INDEX command issued                                       │
│     ↓                                                                  │
│     State: indislive=true, indisready=false, indisvalid=false         │
│     ↓                                                                  │
│     make_o_index() creates OIndex structure                           │
│     ↓                                                                  │
│     o_indices_add() stores in SYS_TREES_O_INDICES                     │
│     ↓                                                                  │
│     UNDO log records transaction                                      │
│                                                                        │
│  2. Build phase starts                                                │
│     ↓                                                                  │
│     Scan table and insert tuples into new index                       │
│     ↓ (may take minutes/hours for large tables)                       │
│     Build completes successfully                                      │
│     ↓                                                                  │
│     State: indislive=true, indisready=TRUE, indisvalid=false          │
│     ↓                                                                  │
│     o_indices_update() persists flag change                           │
│     ↓                                                                  │
│     New inserts now go to this index                                  │
│                                                                        │
│  3. Validation phase (for CONCURRENT builds)                          │
│     ↓                                                                  │
│     Verify index correctness                                          │
│     ↓                                                                  │
│     Check all tuples are present                                      │
│     ↓                                                                  │
│     Validation passes                                                 │
│     ↓                                                                  │
│     State: indislive=true, indisready=true, indisvalid=TRUE           │
│     ↓                                                                  │
│     o_indices_update() persists flag change                           │
│     ↓                                                                  │
│     Query planner now uses this index                                 │
│                                                                        │
│  4. Normal operation                                                  │
│     ↓                                                                  │
│     Index used for:                                                   │
│     - INSERT (because indisready=true)                                │
│     - SELECT (because indisvalid=true)                                │
│     - UPDATE/DELETE (uses both flags)                                 │
│                                                                        │
│  5. DROP INDEX (if needed)                                            │
│     ↓                                                                  │
│     State: indislive=FALSE, indisready=false, indisvalid=false        │
│     ↓                                                                  │
│     o_indices_del() removes from SYS_TREES_O_INDICES                  │
│     ↓                                                                  │
│     Physical index tree deleted                                       │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 5. Serialization/Deserialization Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│              Serialization Flow (OIndex → System Tree)                │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  OIndex structure in memory                                           │
│     │                                                                  │
│     ├── serialize_o_index(oIndex, &size)                              │
│     │      │                                                           │
│     │      ├── Allocate buffer                                        │
│     │      ├── Copy fixed-size fields (indexOids, flags, etc.)        │
│     │      ├── Copy arrays (primaryFieldsAttnums)                     │
│     │      ├── Serialize expressions as strings                       │
│     │      └── Return binary data + size                              │
│     │                                                                  │
│     ├── Split into chunks (TOAST API)                                 │
│     │      │                                                           │
│     │      ├── Chunk 0: bytes 0-5000                                  │
│     │      ├── Chunk 1: bytes 5001-10000                              │
│     │      └── Chunk N: remaining bytes                               │
│     │                                                                  │
│     └── Insert chunks into SYS_TREES_O_INDICES                        │
│            │                                                           │
│            ├── Key: (type=oIndexPrimary, oids={1,2,3}, chunknum=0)   │
│            ├── Key: (type=oIndexPrimary, oids={1,2,3}, chunknum=1)   │
│            └── Key: (type=oIndexPrimary, oids={1,2,3}, chunknum=N)   │
│                   │                                                    │
│                   └── B-tree stores on disk with UNDO support         │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│              Deserialization Flow (System Tree → OIndex)              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  o_indices_get(oids, type)                                            │
│     │                                                                  │
│     ├── B-tree lookup in SYS_TREES_O_INDICES                          │
│     │      │                                                           │
│     │      └── Find all chunks with matching (type, oids)             │
│     │                                                                  │
│     ├── Reassemble chunks (TOAST API)                                 │
│     │      │                                                           │
│     │      ├── Read chunk 0, 1, 2, ..., N                             │
│     │      └── Concatenate into single buffer                         │
│     │                                                                  │
│     ├── deserialize_o_index(data, length)                             │
│     │      │                                                           │
│     │      ├── Allocate OIndex structure                              │
│     │      ├── Copy fixed-size fields                                 │
│     │      │                                                           │
│     │      ├── Check data_version                                     │
│     │      │   ├── if (version >= 3)  // Your new version            │
│     │      │   │   └── Read indislive/indisready/indisvalid           │
│     │      │   └── else                                               │
│     │      │       └── Set default values (true, true, true)          │
│     │      │                                                           │
│     │      ├── Palloc arrays (leafTableFields, etc.)                  │
│     │      ├── Parse expression strings back to Lists                 │
│     │      └── Return OIndex*                                         │
│     │                                                                  │
│     └── Apply MVCC visibility                                         │
│            │                                                           │
│            ├── Check transaction status (oxid, csn)                   │
│            └── Return if visible to current snapshot                  │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 6. Why pg_index Changes Don't Work

```
┌──────────────────────────────────────────────────────────────────────┐
│          What Happens When You Update pg_index                        │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  UPDATE pg_catalog.pg_index SET indisvalid = false WHERE ...;         │
│     │                                                                  │
│     ├── PostgreSQL writes to heap table pg_index                      │
│     │      │                                                           │
│     │      └── Tuple stored at (blocknum=123, offset=5)               │
│     │                                                                  │
│     ├── Syscache invalidation message broadcast                       │
│     │      │                                                           │
│     │      └── Other PostgreSQL backends refresh their syscache       │
│     │                                                                  │
│     └── OrioleDB code continues running...                            │
│            │                                                           │
│            ├── Query planner calls o_indices_get()                    │
│            │      │                                                    │
│            │      ├── Looks up SYS_TREES_O_INDICES (NOT pg_index!)    │
│            │      │                                                    │
│            │      └── Deserializes OIndex with old indisvalid=true    │
│            │                                                           │
│            └── Uses index anyway! ◄── YOUR PROBLEM                    │
│                                                                        │
│  Result: pg_index change has NO EFFECT on OrioleDB                    │
│                                                                        │
│  Why: OrioleDB never reads pg_index after initial index creation.     │
│       All metadata comes from SYS_TREES_O_INDICES.                    │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│          Correct Way: Update OrioleDB System Tree                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  oIndex->indisvalid = false;                                          │
│  o_indices_update(oTable, ixNum, oxid, csn);                          │
│     │                                                                  │
│     ├── Serialize OIndex with new flag value                          │
│     │      │                                                           │
│     │      └── Binary data: ...indisvalid=0x00...                     │
│     │                                                                  │
│     ├── Update SYS_TREES_O_INDICES                                    │
│     │      │                                                           │
│     │      ├── Create UNDO record (old value)                         │
│     │      ├── Write new chunks to B-tree                             │
│     │      └── Tag with transaction ID and CSN                        │
│     │                                                                  │
│     └── On next o_indices_get()                                       │
│            │                                                           │
│            ├── Deserialize new value                                  │
│            └── indisvalid=false ◄── CORRECTLY REFLECTED               │
│                                                                        │
│  Result: All OrioleDB code sees the updated flag!                     │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 7. Transaction and MVCC Support

```
┌──────────────────────────────────────────────────────────────────────┐
│          System Tree Transaction Support                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  Transaction 1 (oxid=100):                                            │
│     │                                                                  │
│     ├── BEGIN                                                         │
│     ├── oIndex->indisvalid = false                                    │
│     ├── o_indices_update(..., oxid=100, csn=Invalid)                  │
│     │      │                                                           │
│     │      ├── BEFORE: indisvalid=true (csn=50)                       │
│     │      │                                                           │
│     │      ├── Create UNDO record:                                    │
│     │      │   UndoRecord {                                           │
│     │      │     type: INDEX_UPDATE,                                  │
│     │      │     oxid: 100,                                           │
│     │      │     old_data: [indisvalid=true],                         │
│     │      │     new_data: [indisvalid=false]                         │
│     │      │   }                                                       │
│     │      │                                                           │
│     │      └── Write to SYS_TREES_O_INDICES (tagged with oxid=100)    │
│     │                                                                  │
│     └── COMMIT                                                        │
│            │                                                           │
│            └── Assign CSN=200 to oxid=100                             │
│                                                                        │
│  Transaction 2 (snapshot CSN=150):                                    │
│     │                                                                  │
│     ├── o_indices_get() called                                        │
│     │      │                                                           │
│     │      ├── Read from SYS_TREES_O_INDICES                          │
│     │      │                                                           │
│     │      ├── Check visibility:                                      │
│     │      │   if (record.csn <= snapshot.csn) // 50 <= 150 = true   │
│     │      │     return old version;                                  │
│     │      │                                                           │
│     │      └── Returns: indisvalid=true ◄── OLD VERSION               │
│     │                                                                  │
│     └── Doesn't see uncommitted change (MVCC isolation)               │
│                                                                        │
│  Transaction 3 (snapshot CSN=250):                                    │
│     │                                                                  │
│     ├── o_indices_get() called                                        │
│     │      │                                                           │
│     │      ├── Read from SYS_TREES_O_INDICES                          │
│     │      │                                                           │
│     │      ├── Check visibility:                                      │
│     │      │   if (record.csn <= snapshot.csn) // 200 <= 250 = true  │
│     │      │     return new version;                                  │
│     │      │                                                           │
│     │      └── Returns: indisvalid=false ◄── NEW VERSION              │
│     │                                                                  │
│     └── Sees committed change (CSN 200 < snapshot 250)                │
│                                                                        │
│  If Transaction 1 had rolled back:                                    │
│     │                                                                  │
│     └── ROLLBACK                                                      │
│            │                                                           │
│            ├── Replay UNDO records                                    │
│            │   ├── Read UndoRecord for oxid=100                       │
│            │   └── Restore: indisvalid=true                           │
│            │                                                           │
│            └── All future reads see indisvalid=true                   │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

## 8. Memory Layout Comparison

```
PostgreSQL Index in Memory:
┌────────────────────────────────────┐
│     IndexInfo (from pg_index)      │
├────────────────────────────────────┤
│ ii_NumIndexAttrs:  int             │
│ ii_IndexAttrNumbers: int*          │
│ ii_Predicate: List*                │
│ ii_PredicateState: ExprState*      │
│ ii_Expressions: List*              │
│ ii_ExpressionsState: ExprState*    │
│ ii_Unique: bool                    │
│ ii_ReadyForInserts: bool ◄────────┐│  From pg_index.indisready
│ ii_CheckedUnchanged: bool          ││
│ ii_BrokenHotChain: bool            ││
└────────────────────────────────────┘│
                                      │
OrioleDB Index in Memory:             │
┌────────────────────────────────────┐│
│         OIndex (in-memory)         ││
├────────────────────────────────────┤│
│ indexOids: ORelOids                ││
│ indexType: OIndexType              ││
│ tableOids: ORelOids                ││
│ bridging: bool                     ││
│ indislive: bool                    ││
│ indisready: bool ◄─────────────────┘│  Your new field!
│ indisvalid: bool                    │
│ nLeafFields: uint16                 │
│ leafFields: OTableIndexField*       │
│ predicate: List*                    │
│ expressions: List*                  │
│ ... (25+ more fields)               │
└─────────────────────────────────────┘
         ↕ serialize/deserialize
┌─────────────────────────────────────┐
│   SYS_TREES_O_INDICES (on disk)     │
├─────────────────────────────────────┤
│ Chunk 0: bytes 0-5000               │
│ Chunk 1: bytes 5001-10000           │
│ ...                                 │
└─────────────────────────────────────┘
```

## Summary

This visual guide shows:

1. **PostgreSQL vs OrioleDB**: Different storage models (heap vs B-tree)
2. **System Trees**: 23 specialized B-trees for metadata storage
3. **OIndex Structure**: Where to add your flags
4. **Lifecycle**: How flags change during index creation/build/validation
5. **Serialization**: How OIndex converts to/from binary format
6. **pg_index Problem**: Why updating pg_index doesn't work
7. **Transactions**: How MVCC and UNDO logs protect flag changes
8. **Memory Layout**: Side-by-side comparison of structures

The key insight: **OrioleDB is a parallel metadata system to PostgreSQL's catalog**. To modify OrioleDB behavior, you must modify OrioleDB's system trees, not PostgreSQL's catalog tables.
