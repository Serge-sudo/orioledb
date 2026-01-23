# OrioleDB System Trees: Comprehensive Analysis

## Executive Summary

OrioleDB uses specialized B-tree structures called "system trees" to store metadata separately from PostgreSQL's catalog tables. This document explains their implementation, necessity, and why PostgreSQL's `pg_index` changes don't work with OrioleDB trees. This analysis is critical for implementing index status flags (dead/invalid/ready) in OrioleDB.

---

## 1. What Are System Trees?

System trees are **persistent B-tree structures that store OrioleDB's own metadata** separate from PostgreSQL's system catalog. They are OrioleDB's internal metadata management system.

### Key System Trees (from `sys_trees.h`)

```c
#define SYS_TREES_SHARED_ROOT_INFO    (1)   // Root info for all trees
#define SYS_TREES_O_TABLES            (2)   // Table metadata
#define SYS_TREES_O_INDICES           (3)   // Index metadata *** KEY FOR YOUR USE CASE ***
#define SYS_TREES_OPCLASS_CACHE       (4)   // Operator class cache
#define SYS_TREES_ENUM_CACHE          (5)   // Enum type cache
#define SYS_TREES_TYPE_CACHE          (12)  // Type cache
#define SYS_TREES_EXTENTS_OFF_LEN     (9)   // Free space by offset
#define SYS_TREES_EXTENTS_LEN_OFF     (10)  // Free space by length
// ... and 13 more specialized trees
```

### Why System Trees Are Necessary

1. **Storage Model Incompatibility**
   - OrioleDB uses **index-organized tables** (data stored in B-trees)
   - PostgreSQL uses **heap-based tables**
   - Cannot use PostgreSQL's heap tables to store OrioleDB's B-tree metadata

2. **Transaction Model Differences**
   - OrioleDB: **UNDO log-based MVCC** (no VACUUM needed)
   - PostgreSQL: **Tuple versioning with VACUUM**
   - Fundamentally different transaction management requires different metadata storage

3. **Checkpoint and Recovery**
   - OrioleDB uses **copy-on-write checkpoints** with row-level WAL
   - System trees must be accessible during checkpoint/recovery **without full PostgreSQL connection**
   - Need metadata available even when PostgreSQL catalog is unavailable

4. **Performance**
   - Direct B-tree indexed access to metadata (O(log n))
   - No sequential scans of heap tables
   - Integrated with OrioleDB's page pool and caching

---

## 2. Implementation Details

### System Tree Architecture (from `sys_trees.c`)

Each system tree has a `SysTreeMeta` structure defining its behavior:

```c
typedef struct {
    int keyLength;              // Fixed or -1 for variable
    int tupleLength;            // Fixed or -1 for variable
    TupleLengthFunc tupleLengthFunc;  // For variable-length tuples
    TupleCmpFunc cmpFunc;       // Key comparison function
    OPagePoolType poolType;     // Memory pool (usually OPagePoolCatalog)
    UndoLogType undoLogType;    // Usually UndoLogSystem
    BTreeStorageType storageType; // BTreeStoragePersistence
    NeedsUndoFunc needs_undo;   // Transaction support callback
} SysTreeMeta;
```

### SYS_TREES_O_INDICES Implementation

For index metadata (your focus area):

```c
// From sys_trees.c line ~150
{  /* SYS_TREES_O_INDICES */
    .keyLength = sizeof(OIndexChunkKey),
    .tupleLength = -1,  // Variable length (uses TOAST)
    .tupleLengthFunc = o_index_chunk_length,
    .cmpFunc = o_index_chunk_cmp,
    .poolType = OPagePoolCatalog,
    .undoLogType = UndoLogSystem,
    .storageType = BTreeStoragePersistence,
    .needs_undo = o_index_chunk_needs_undo
}
```

### Data Structures

#### Index Storage Key (from `sys_trees.h`)

```c
typedef struct {
    OIndexType type;       // Primary, secondary, TOAST, bridge
    ORelOids oids;         // {datoid, reloid, relnode}
    uint32 chunknum;       // TOAST chunk number
} OIndexChunkKey;
```

#### Index Metadata (from `o_indices.h`)

```c
typedef struct {
    ORelOids indexOids;
    OIndexType indexType;
    ORelOids tableOids;
    char table_persistence;
    uint8 fillfactor;
    uint16 data_version;        // Schema evolution version
    OXid createOxid;
    NameData name;
    bool primaryIsCtid;
    bool bridging;              // *** EXISTING FLAG EXAMPLE ***
    OCompress compress;
    bool nulls_not_distinct;
    uint16 nIncludedFields;
    uint16 nLeafFields;
    uint16 nNonLeafFields;
    uint16 nUniqueFields;
    uint16 nKeyFields;
    uint16 nPrimaryFields;
    AttrNumber primaryFieldsAttnums[INDEX_MAX_KEYS];
    
    // Below fields are serialized separately
    OTableField *leafTableFields;
    OTableIndexField *leafFields;
    List *predicate;            // Serialized as string
    char *predicate_str;
    List *expressions;
    List *duplicates;
    Oid tablespace;
    MemoryContext index_mctx;
} OIndex;
```

---

## 3. How System Trees Differ from PostgreSQL Catalog

| Aspect | PostgreSQL (pg_index, pg_class) | OrioleDB (SYS_TREES_O_INDICES) |
|--------|--------------------------------|-------------------------------|
| **Storage** | Heap tables (unordered rows) | Persistent B-trees (ordered) |
| **Access Method** | Sequential/index scans via syscache | Direct B-tree lookup |
| **Transaction Model** | Tuple versioning + VACUUM | UNDO log + MVCC |
| **Update Strategy** | In-place with versioning | Copy-on-write checkpoints |
| **Keys** | OID-based (single value) | Compound ORelOids (datoid, reloid, relnode) |
| **Data Format** | Fixed row structure | Serialized chunks (TOAST-compatible) |
| **Initialization** | Requires PostgreSQL connection | Accessible during bootstrap/recovery |
| **Caching** | PostgreSQL syscache | OrioleDB B-tree cache |

---

## 4. Why pg_index Changes Don't Work with OrioleDB

### Fundamental Incompatibilities

1. **Storage Layer Mismatch**
   ```
   pg_index: Heap table → tuple at (blocknum, offset)
   OrioleDB: B-tree → key-based lookup in SYS_TREES_O_INDICES
   ```
   - `pg_index` updates write to heap pages
   - OrioleDB indexes stored in B-tree nodes
   - No direct translation between the two

2. **Metadata Structure Differences**
   
   **pg_index has:**
   ```sql
   CREATE TABLE pg_index (
       indexrelid oid,
       indrelid oid,
       indisunique bool,
       indisprimary bool,
       indisvalid bool,    -- You want to add flags like this
       indisready bool,    -- And this
       indislive bool,     -- And this
       ...
   );
   ```
   
   **OrioleDB OIndex has:**
   - 25+ additional fields not in pg_index
   - Serialized expressions and predicates
   - Complex field mappings (leafTableFields, primaryFieldsAttnums)
   - TOAST chunking for large metadata
   
3. **Transaction Isolation**
   ```
   PostgreSQL: pg_index change → syscache invalidation → all backends see it
   OrioleDB: SYS_TREES_O_INDICES change → UNDO log → MVCC visibility rules
   ```
   - OrioleDB uses its own transaction visibility system
   - pg_index changes not propagated to system trees

4. **No Synchronization Mechanism**
   - OrioleDB doesn't monitor pg_index for changes
   - When OrioleDB needs index metadata, it reads from SYS_TREES_O_INDICES
   - pg_index is essentially ignored after index creation

### Example: What Happens When You Update pg_index

```sql
-- This PostgreSQL catalog update:
UPDATE pg_catalog.pg_index SET indisready = false WHERE indexrelid = 12345;

-- Does NOT affect OrioleDB because:
-- 1. OrioleDB reads from SYS_TREES_O_INDICES, not pg_index
-- 2. No listener/hook to propagate change to system trees
-- 3. Different transaction visibility rules apply
```

---

## 5. Index Metadata Management: OrioleDB vs PostgreSQL

### PostgreSQL Flow

```
CREATE INDEX → pg_index insert → syscache invalidation
ALTER INDEX  → pg_index update → syscache invalidation
DROP INDEX   → pg_index delete → syscache invalidation
              ↓
          All backends refresh from pg_index
```

### OrioleDB Flow (from `o_indices.c`)

```
CREATE INDEX → o_indices_add()
              ↓
          serialize_o_index() → creates chunks
              ↓
          Insert into SYS_TREES_O_INDICES via TOAST API
              ↓
          UNDO log records transaction
              ↓
          Commit → CSN assigned

Access INDEX → o_indices_get()
              ↓
          Fetch chunks from SYS_TREES_O_INDICES
              ↓
          deserialize_o_index() → rebuild OIndex structure
              ↓
          Apply MVCC visibility rules (check CSN)
```

### Key Functions (from `o_indices.c`)

```c
// Add index metadata to system tree
bool o_indices_add(OTable *table, OIndexNumber ixNum, 
                   OXid oxid, CommitSeqNo csn);

// Update index metadata in system tree  
bool o_indices_update(OTable *table, OIndexNumber ixNum,
                     OXid oxid, CommitSeqNo csn);

// Delete index metadata from system tree
bool o_indices_del(OTable *table, OIndexNumber ixNum,
                   OXid oxid, CommitSeqNo csn);

// Retrieve index metadata from system tree
OIndex *o_indices_get(ORelOids oids, OIndexType type);
```

### Serialization Process (lines 638-662 in `o_indices.c`)

1. **Serialize OIndex structure** → binary format
2. **Split into TOAST chunks** (max ~5KB each)
3. **Store chunks in SYS_TREES_O_INDICES** with keys:
   ```c
   (type=oIndexPrimary, oids={1,2,3}, chunknum=0)
   (type=oIndexPrimary, oids={1,2,3}, chunknum=1)
   ...
   ```
4. **Track version** via `data_version` field

### Deserialization Process (lines 664-717)

1. **Fetch all chunks** for given (type, oids)
2. **Reassemble binary data**
3. **Deserialize fixed-size fields**
4. **Palloc and deserialize variable-size fields:**
   - `leafTableFields` array
   - `leafFields` array
   - `predicate` (parsed from string)
   - `expressions` (parsed from string)
5. **Apply MVCC visibility** (check transaction status)

---

## 6. Implementing Index Status Flags in OrioleDB

### What You Need to Do

Since pg_index changes don't work, you must:

1. **Add flag fields to OIndex structure** (in `o_indices.h`):
   ```c
   typedef struct {
       // ... existing fields ...
       bool bridging;              // Existing example
       
       // Add your new flags:
       bool indisvalid;            // Index is valid for queries
       bool indisready;            // Index is ready for inserts
       bool indislive;             // Index is live in catalog
       
       // ... rest of fields ...
   } OIndex;
   ```

2. **Update serialization** (in `o_indices.c`):
   - Add fields to `serialize_o_index()` function
   - Increment `data_version` for schema evolution
   - Add fields to `deserialize_o_index()` function

3. **Update index management functions**:
   ```c
   // When creating index
   o_indices_add() {
       oIndex->indisvalid = false;  // Not valid yet
       oIndex->indisready = false;  // Not ready yet
       oIndex->indislive = true;    // Live in catalog
       // ... serialize and store ...
   }
   
   // When building index
   o_indices_update() {
       oIndex->indisready = true;   // Ready for inserts
       // ... update system tree ...
   }
   
   // When validating index
   o_indices_update() {
       oIndex->indisvalid = true;   // Valid for queries
       // ... update system tree ...
   }
   ```

4. **Check flags in query/insert paths**:
   ```c
   // In query planning
   if (!index->indisvalid) {
       // Don't use this index for queries
   }
   
   // In insert execution
   if (index->indisready) {
       // Insert into this index
   }
   ```

5. **Handle pg_index synchronization** (if needed):
   - Update pg_index when OrioleDB flags change
   - Or ignore pg_index entirely (OrioleDB doesn't read it anyway)

### Example: Existing "bridging" Flag

The `bridging` flag is already implemented this way:

- **Declaration** in `o_indices.h`: `bool bridging;`
- **Serialized** in `serialize_o_index()`
- **Deserialized** in `deserialize_o_index()`
- **Checked** in 80+ places across codebase:
  - `if (idx->bridging) { /* special bridge logic */ }`
  - `if (primary->bridging) { /* add bridge_ctid field */ }`

Your new flags would work identically.

---

## 7. System Tree Transaction Support

System trees are **fully transactional** using OrioleDB's UNDO log system:

```c
// From sys_trees.c
.undoLogType = UndoLogSystem,
.needs_undo = o_index_chunk_needs_undo,
```

### Transaction Semantics

1. **Insert/Update/Delete** operations on SYS_TREES_O_INDICES create UNDO records
2. **Rollback** replays UNDO log to revert changes
3. **MVCC** visibility rules apply:
   - Each modification tagged with `OXid` (OrioleDB transaction ID)
   - Each commit tagged with `CommitSeqNo` (CSN)
   - Readers see versions based on their snapshot CSN

### Example Transaction

```c
// Transaction begins
OXid oxid = get_current_oxid();

// Modify index metadata
o_indices_update(table, ixNum, oxid, InvalidCommitSeqNo);
    ↓
// Stored in SYS_TREES_O_INDICES with oxid tag
// UNDO record created

// Transaction commits
CommitSeqNo csn = commit_transaction(oxid);
    ↓
// CSN assigned, UNDO records marked with CSN

// Other transactions can now see the change if their snapshot >= csn
```

---

## 8. Key Takeaways for Your Implementation

### Must Do

1. ✅ **Add flags to OIndex structure** (`o_indices.h`)
2. ✅ **Update serialize/deserialize functions** (`o_indices.c`)
3. ✅ **Increment data_version** for backward compatibility
4. ✅ **Check flags in relevant code paths** (query planner, insert executor)
5. ✅ **Use o_indices_update()** to modify flags transactionally

### Don't Do

1. ❌ **Don't rely on pg_index changes** - they won't propagate to OrioleDB
2. ❌ **Don't try to sync pg_index → SYS_TREES_O_INDICES** - no mechanism exists
3. ❌ **Don't add flags only to pg_index** - OrioleDB won't see them
4. ❌ **Don't bypass system trees** - they're the authoritative source

### Best Practices

1. **Follow the "bridging" flag pattern** - it's a proven example
2. **Test transactional behavior** - ensure flags survive rollback/commit
3. **Consider checkpoint/recovery** - flags must persist across restarts
4. **Update documentation** - explain what each flag means
5. **Handle schema evolution** - older versions without flags should work

---

## 9. Code References

### Key Files

- **`include/catalog/sys_trees.h`** - System tree definitions
- **`src/catalog/sys_trees.c`** - System tree initialization (33.7 KB)
- **`include/catalog/o_indices.h`** - OIndex structure definition
- **`src/catalog/o_indices.c`** - Index metadata management
- **`doc/architecture/overview.mdx`** - System trees overview

### Search Terms

```bash
# Find existing flag usage
grep -r "bridging" --include="*.c" --include="*.h"

# Find index metadata access
grep -r "o_indices_get\|o_indices_add\|o_indices_update" --include="*.c"

# Find serialization code
grep -r "serialize_o_index\|deserialize_o_index" --include="*.c"
```

---

## 10. Summary

**Problem:** You want to add index status flags (valid/ready/live) like PostgreSQL's pg_index, but changes to pg_index don't affect OrioleDB.

**Root Cause:** OrioleDB stores index metadata in system trees (B-trees), not PostgreSQL's heap-based catalog. System trees are the authoritative source, and pg_index is essentially ignored.

**Solution:** Add flags directly to the OIndex structure, update serialization/deserialization, and use o_indices_update() to modify flags transactionally. Follow the existing "bridging" flag pattern.

**Why This Works:** System trees are fully integrated with OrioleDB's transaction system, checkpoint/recovery, and MVCC. Flags stored in SYS_TREES_O_INDICES will be visible to all OrioleDB code paths and will survive restarts.

---

## Additional Resources

- **OrioleDB Documentation**: `/doc/architecture/overview.mdx`
- **System Trees Implementation**: `/src/catalog/sys_trees.c`
- **Index Management**: `/src/catalog/o_indices.c`
- **Existing Flag Example**: Search for `bridging` in codebase (80+ references)

**Need more help?** Examine how the `bridging` flag is declared, serialized, and used - it's the perfect template for your implementation.
