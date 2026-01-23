# Quick Reference: OrioleDB System Trees

This is a quick reference guide for understanding and working with OrioleDB system trees, particularly for implementing index status flags.

## 🔑 Key Concepts (30-Second Overview)

- **System Trees**: OrioleDB's B-tree-based metadata storage (alternative to PostgreSQL's heap-based catalog)
- **SYS_TREES_O_INDICES**: The system tree storing index metadata (tree #3 of 23)
- **OIndex**: The structure representing index metadata (similar to pg_index but much richer)
- **Why pg_index doesn't work**: OrioleDB reads from system trees, not PostgreSQL's catalog
- **Solution**: Add flags to OIndex structure and use `o_indices_update()` to persist changes

## 📋 System Trees List

| ID | Tree Name | Purpose |
|----|-----------|---------|
| 1 | SHARED_ROOT_INFO | Root page info for all trees |
| 2 | O_TABLES | Table metadata |
| **3** | **O_INDICES** | **Index metadata (your focus)** |
| 4 | OPCLASS_CACHE | Operator class cache |
| 5 | ENUM_CACHE | Enum type cache |
| 9 | EXTENTS_OFF_LEN | Free space by offset |
| 10 | EXTENTS_LEN_OFF | Free space by length |
| ... | ... | 16 more trees |

## 🏗️ OIndex Structure (Simplified)

```c
typedef struct {
    // Identity
    ORelOids indexOids;        // {datoid, reloid, relnode}
    OIndexType indexType;      // Primary/Secondary/TOAST/Bridge
    ORelOids tableOids;        // Parent table
    
    // Properties
    bool bridging;             // Existing flag example
    OCompress compress;        // Compression type
    uint16 data_version;       // Schema version
    
    // 🎯 ADD YOUR FLAGS HERE:
    bool indislive;            // Index exists in catalog
    bool indisready;           // Ready for inserts
    bool indisvalid;           // Valid for queries
    
    // Field mappings
    uint16 nKeyFields;         // Number of key fields
    OTableField *leafTableFields;  // Field definitions
    
    // Expressions
    List *predicate;           // WHERE clause
    List *expressions;         // Index expressions
} OIndex;
```

## 🔄 Key Operations

| Operation | Function | Purpose |
|-----------|----------|---------|
| **Create** | `o_indices_add()` | Add index metadata to system tree |
| **Read** | `o_indices_get()` | Fetch and deserialize index metadata |
| **Update** | `o_indices_update()` | Modify index metadata transactionally |
| **Delete** | `o_indices_del()` | Remove index metadata from system tree |

## 📝 Implementation Checklist

- [ ] Add flags to `OIndex` struct (`include/catalog/o_indices.h`)
- [ ] Increment `data_version` constant
- [ ] Update `serialize_o_index()` in `src/catalog/o_indices.c`
- [ ] Update `deserialize_o_index()` with version check
- [ ] Initialize flags in `make_o_index()` and related functions
- [ ] Set flags during index lifecycle (build, validate, drop)
- [ ] Check flags in query planner (for `indisvalid`)
- [ ] Check flags in insert execution (for `indisready`)
- [ ] Write tests for flag transitions
- [ ] Test transaction rollback behavior

## 🚫 Common Mistakes to Avoid

| ❌ Don't Do This | ✅ Do This Instead |
|-----------------|-------------------|
| `UPDATE pg_index SET indisvalid = false` | `oIndex->indisvalid = false; o_indices_update()` |
| Modify OIndex directly without persisting | Always call `o_indices_update()` after changes |
| Skip version check in deserialization | `if (data_version >= 3) { read new fields }` |
| Assume pg_index and OIndex are synced | They are independent; update both if needed |

## 🔍 Finding Things in the Codebase

```bash
# Find system tree definitions
grep -r "SYS_TREES" include/catalog/sys_trees.h

# Find OIndex usage
grep -r "o_indices_get\|o_indices_add\|o_indices_update" --include="*.c"

# Find existing flag example (bridging)
grep -rn "bridging" --include="*.c" --include="*.h" | head -20

# Find serialization code
grep -rn "serialize_o_index\|deserialize_o_index" src/catalog/o_indices.c
```

## 🧪 Testing Your Implementation

```sql
-- Test 1: Check initial state after CREATE INDEX
CREATE INDEX test_idx ON test_table(col1);
-- Expected: indislive=true, indisready=false, indisvalid=false

-- Test 2: Check state after build
-- Wait for index build to complete
-- Expected: indislive=true, indisready=true, indisvalid=false

-- Test 3: Check state after validation
-- Wait for validation to complete
-- Expected: indislive=true, indisready=true, indisvalid=true

-- Test 4: Query planner respects indisvalid
-- Set indisvalid=false somehow
EXPLAIN SELECT * FROM test_table WHERE col1 = 5;
-- Expected: Should NOT use index (seq scan instead)

-- Test 5: Transaction rollback
BEGIN;
-- Change flag
ROLLBACK;
-- Expected: Flag reverts to original value (UNDO log worked)
```

## 📊 Flag State Matrix

| State | indislive | indisready | indisvalid | Meaning |
|-------|-----------|------------|------------|---------|
| Creating | true | false | false | Index being created |
| Building | true | false | false | Initial build in progress |
| Ready | true | true | false | Accepts inserts, not for queries yet |
| Valid | true | true | true | Fully operational |
| Dropped | false | false | false | Marked for deletion |

## 🔬 Code Locations (Line Numbers Approximate)

| File | Lines | What's There |
|------|-------|--------------|
| `include/catalog/sys_trees.h` | 22-47 | System tree ID definitions |
| `include/catalog/o_indices.h` | 23-81 | OIndex structure definition |
| `src/catalog/sys_trees.c` | 130-169 | System tree metadata array |
| `src/catalog/o_indices.c` | 638-662 | `serialize_o_index()` |
| `src/catalog/o_indices.c` | 664-717 | `deserialize_o_index()` |
| `src/catalog/o_indices.c` | 1096-1121 | Index key/chunk structures |

## 🎯 Your Implementation Path

```
Step 1: Add flags to OIndex struct
   ↓
Step 2: Update serialization (write new fields)
   ↓
Step 3: Update deserialization (read new fields with version check)
   ↓
Step 4: Initialize flags in make_o_index() functions
   ↓
Step 5: Update flags during index lifecycle
   ↓
Step 6: Check flags in query/insert paths
   ↓
Step 7: Test, test, test!
```

## 💡 Pro Tips

1. **Follow the `bridging` flag pattern** - it's implemented correctly throughout the codebase
2. **Always increment `data_version`** - enables backward compatibility
3. **Test UNDO log behavior** - ensure rollback restores old flag values
4. **Test checkpoint persistence** - flags must survive restarts
5. **Use MVCC correctly** - different transactions may see different flag values

## 🆘 Need More Details?

- **Full Analysis**: See `SYSTEM_TREES_ANALYSIS.md`
- **Implementation Guide**: See `INDEX_FLAGS_IMPLEMENTATION_GUIDE.md`
- **Visual Diagrams**: See `SYSTEM_TREES_ARCHITECTURE_DIAGRAMS.md`
- **Source Code**: `src/catalog/o_indices.c` (especially lines 638-717)

## 🔗 Related PostgreSQL Concepts

| PostgreSQL | OrioleDB Equivalent |
|------------|-------------------|
| `pg_index` (heap table) | `SYS_TREES_O_INDICES` (B-tree) |
| `Form_pg_index` struct | `OIndex` struct |
| Tuple versioning | UNDO log + MVCC |
| VACUUM | Not needed (UNDO log handles old versions) |
| Syscache | Direct B-tree lookup |
| `indisvalid` flag | Add to `OIndex.indisvalid` |
| `indisready` flag | Add to `OIndex.indisready` |
| `indislive` flag | Add to `OIndex.indislive` |

## 📞 Key Functions to Remember

```c
// Create index metadata
bool o_indices_add(OTable *table, OIndexNumber ixNum, 
                   OXid oxid, CommitSeqNo csn);

// Read index metadata
OIndex *o_indices_get(ORelOids oids, OIndexType type);

// Update index metadata (USE THIS FOR FLAG CHANGES!)
bool o_indices_update(OTable *table, OIndexNumber ixNum,
                     OXid oxid, CommitSeqNo csn);

// Delete index metadata
bool o_indices_del(OTable *table, OIndexNumber ixNum,
                   OXid oxid, CommitSeqNo csn);
```

## ⏱️ Estimated Implementation Time

- Reading documentation: 2 hours
- Core implementation (steps 1-4): 4-6 hours
- Lifecycle integration (steps 5-6): 2-4 hours
- Testing and debugging: 4-6 hours
- **Total: 12-18 hours**

## 🎓 Key Takeaways

1. **OrioleDB ≠ PostgreSQL**: Different storage models require different approaches
2. **System Trees are authoritative**: pg_index is essentially ignored
3. **OIndex is rich**: 25+ fields, not just the basics in pg_index
4. **Serialization is key**: Binary format stored in system tree
5. **Transactions work**: UNDO log + MVCC protect flag changes
6. **Version matters**: Increment `data_version` for new fields
7. **Test thoroughly**: Especially transaction rollback and restart persistence

---

**Remember**: When in doubt, look at how the `bridging` flag is implemented. It's the perfect template for your implementation!

Good luck! 🚀
