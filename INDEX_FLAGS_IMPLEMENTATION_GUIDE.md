# Implementation Guide: Adding Index Status Flags to OrioleDB

This guide provides a step-by-step implementation plan for adding PostgreSQL-style index status flags (indisvalid, indisready, indislive) to OrioleDB's system trees.

## Overview

You want to implement index status tracking similar to PostgreSQL's `pg_index` columns:
- `indislive` - Index is live in catalog (exists)
- `indisready` - Index is ready for inserts (finished initial build)
- `indisvalid` - Index is valid for queries (passed validation)

Since OrioleDB stores index metadata in system trees (not `pg_index`), you must add these flags to the `OIndex` structure stored in `SYS_TREES_O_INDICES`.

## Step-by-Step Implementation

### Step 1: Add Flags to OIndex Structure

**File:** `include/catalog/o_indices.h`

```c
typedef struct
{
    ORelOids    indexOids;
    OIndexType  indexType;
    ORelOids    tableOids;
    char        table_persistence;
    uint8       fillfactor;
    uint16      data_version;
    OXid        createOxid;
    NameData    name;
    bool        primaryIsCtid;
    bool        bridging;
    OCompress   compress;
    bool        nulls_not_distinct;
    
    /* ADD YOUR NEW FLAGS HERE */
    bool        indislive;      /* Index is live in catalog */
    bool        indisready;     /* Index is ready for inserts */
    bool        indisvalid;     /* Index is valid for queries */
    
    uint16      nIncludedFields;
    /* ... rest of fields ... */
} OIndex;
```

**Important:** Add the flags in the **serialized section** (before the comment "Fields above are stored in SYS_TREES_O_INDICES").

### Step 2: Update Data Version

**File:** `include/catalog/o_indices.h` or `src/catalog/o_indices.c`

Find the current `data_version` handling and increment it:

```c
/* Current version is likely 1 or 2, increment to next version */
#define O_INDEX_DATA_VERSION_CURRENT 3  /* Added indislive/indisready/indisvalid */
```

### Step 3: Update Serialization

**File:** `src/catalog/o_indices.c`

Find the `serialize_o_index()` function (around line 638-662) and add your flags to the serialization:

```c
static Pointer
serialize_o_index(OIndex *o_index, int *size)
{
    /* ... existing serialization code ... */
    
    /* After existing bool fields (bridging, nulls_not_distinct, etc.) */
    memcpy(ptr, &o_index->indislive, sizeof(bool));
    ptr += sizeof(bool);
    
    memcpy(ptr, &o_index->indisready, sizeof(bool));
    ptr += sizeof(bool);
    
    memcpy(ptr, &o_index->indisvalid, sizeof(bool));
    ptr += sizeof(bool);
    
    /* ... rest of serialization ... */
}
```

### Step 4: Update Deserialization

**File:** `src/catalog/o_indices.c`

Find the `deserialize_o_index()` function (around line 664-717) and add corresponding deserialization:

```c
static OIndex *
deserialize_o_index(Pointer data, Size length)
{
    /* ... existing deserialization code ... */
    
    /* Handle version compatibility */
    if (oIndex->data_version >= 3)  /* Your new version */
    {
        memcpy(&oIndex->indislive, ptr, sizeof(bool));
        ptr += sizeof(bool);
        
        memcpy(&oIndex->indisready, ptr, sizeof(bool));
        ptr += sizeof(bool);
        
        memcpy(&oIndex->indisvalid, ptr, sizeof(bool));
        ptr += sizeof(bool);
    }
    else
    {
        /* Default values for older versions */
        oIndex->indislive = true;   /* Assume live if not specified */
        oIndex->indisready = true;  /* Assume ready if not specified */
        oIndex->indisvalid = true;  /* Assume valid if not specified */
    }
    
    /* ... rest of deserialization ... */
}
```

### Step 5: Initialize Flags on Index Creation

**File:** `src/catalog/o_indices.c`

Update functions that create OIndex structures:

```c
OIndex *
make_o_index(OTable *table, OIndexNumber ixNum)
{
    /* ... existing code ... */
    
    /* Initialize new flags */
    result->indislive = true;    /* Always live when created */
    result->indisready = false;  /* Not ready until build completes */
    result->indisvalid = false;  /* Not valid until validation completes */
    
    /* ... rest of function ... */
}
```

Also update other index creation functions:
- `make_ctid_o_index()` - CTID index creation
- `make_toast_o_index()` - TOAST index creation  
- `make_bridge_o_index()` - Bridge index creation

### Step 6: Update Flags During Index Build

**File:** `src/btree/build.c` or where index building completes

```c
/* When index build completes successfully */
void
o_finish_index_build(OIndex *oIndex, OTable *oTable)
{
    /* Mark index as ready for inserts */
    oIndex->indisready = true;
    
    /* Update in system tree */
    o_indices_update(oTable, ixNum, get_current_oxid(), InvalidCommitSeqNo);
}
```

### Step 7: Update Flags During Validation

**File:** `src/indexam/handler.c` or validation code

```c
/* When index validation completes successfully */
void
o_validate_index(OIndex *oIndex, OTable *oTable)
{
    /* Mark index as valid for queries */
    oIndex->indisvalid = true;
    
    /* Update in system tree */
    o_indices_update(oTable, ixNum, get_current_oxid(), InvalidCommitSeqNo);
}
```

### Step 8: Check Flags in Query Planning

**File:** `src/indexam/handler.c` or query planning code

```c
/* In index selection for queries */
bool
o_can_use_index_for_query(OIndexDescr *indexDescr)
{
    /* Don't use invalid indexes */
    if (!indexDescr->indisvalid)
        return false;
    
    /* ... other checks ... */
    return true;
}
```

### Step 9: Check Flags in Insert Execution

**File:** `src/tableam/operations.c` or insert handling

```c
/* In tuple insert routine */
void
o_insert_into_indexes(/* ... */)
{
    for (each index)
    {
        /* Only insert into ready indexes */
        if (oIndex->indisready)
        {
            /* Insert tuple into index */
        }
    }
}
```

### Step 10: Handle Index Drop

**File:** `src/catalog/ddl.c` or index drop handling

```c
/* When dropping an index */
void
o_drop_index(/* ... */)
{
    /* Mark as not live */
    oIndex->indislive = false;
    oIndex->indisready = false;
    oIndex->indisvalid = false;
    
    /* Update in system tree (or just delete) */
    o_indices_del(oTable, ixNum, get_current_oxid(), InvalidCommitSeqNo);
}
```

### Step 11: Synchronize with pg_index (Optional)

If you want to keep pg_index in sync for PostgreSQL compatibility:

**File:** `src/catalog/indices.c`

```c
/* After updating OrioleDB flags */
void
sync_pg_index_flags(Oid indexOid, OIndex *oIndex)
{
    Relation pg_index_rel;
    HeapTuple tup;
    Form_pg_index indexForm;
    
    pg_index_rel = table_open(IndexRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexOid));
    
    if (HeapTupleIsValid(tup))
    {
        indexForm = (Form_pg_index) GETSTRUCT(tup);
        
        /* Update PostgreSQL flags to match OrioleDB */
        indexForm->indislive = oIndex->indislive;
        indexForm->indisready = oIndex->indisready;
        indexForm->indisvalid = oIndex->indisvalid;
        
        CatalogTupleUpdate(pg_index_rel, &tup->t_self, tup);
        heap_freetuple(tup);
    }
    
    table_close(pg_index_rel, RowExclusiveLock);
}
```

**Note:** This is optional because OrioleDB doesn't read from pg_index anyway.

## Testing Your Implementation

### Test 1: Basic Flag Lifecycle

```sql
-- Create index (should be indislive=true, indisready=false, indisvalid=false)
CREATE INDEX test_idx ON test_table(col1);

-- Check initial state
SELECT * FROM orioledb_index_description(
    (SELECT oid FROM pg_class WHERE relname = 'test_idx')::regclass
);

-- Wait for build to complete (should set indisready=true)
-- Query the index description again

-- Validate index (should set indisvalid=true)
-- Query the index description again
```

### Test 2: Query Planner Respects Flags

```sql
-- Mark index as invalid
UPDATE orioledb_index_flags SET indisvalid = false WHERE ...;

-- Query should NOT use the invalid index
EXPLAIN SELECT * FROM test_table WHERE col1 = 5;
-- Should show sequential scan, not index scan
```

### Test 3: Insert Respects Flags

```sql
-- Mark index as not ready
UPDATE orioledb_index_flags SET indisready = false WHERE ...;

-- Insert should skip this index
INSERT INTO test_table VALUES (...);
-- Index should not contain new row
```

### Test 4: Transaction Rollback

```sql
BEGIN;
-- Update flag
UPDATE orioledb_index_flags SET indisvalid = false WHERE ...;
-- Check flag is false
ROLLBACK;
-- Check flag is back to true (UNDO log should restore it)
```

### Test 5: Restart Persistence

```sql
-- Set flags, restart PostgreSQL
-- Flags should persist (stored in system tree)
```

## Common Pitfalls to Avoid

### ❌ Don't Do This

```c
/* DON'T: Modify pg_index directly */
UPDATE pg_catalog.pg_index SET indisvalid = false WHERE ...;
/* This won't affect OrioleDB! */
```

```c
/* DON'T: Bypass o_indices_update() */
oIndex->indisvalid = true;
/* Must call o_indices_update() to persist changes! */
```

```c
/* DON'T: Forget version check in deserialization */
memcpy(&oIndex->indisvalid, ptr, sizeof(bool));  /* Segfault on old data! */
/* Must check data_version first! */
```

### ✅ Do This Instead

```c
/* DO: Update via system tree API */
oIndex->indisvalid = true;
o_indices_update(oTable, ixNum, oxid, csn);
```

```c
/* DO: Handle version compatibility */
if (oIndex->data_version >= 3)
    memcpy(&oIndex->indisvalid, ptr, sizeof(bool));
else
    oIndex->indisvalid = true;  /* Safe default */
```

## Debugging Tips

### Check Serialization Size

```c
/* Add debug logging */
elog(DEBUG1, "Serialized index size: %d bytes", size);
elog(DEBUG1, "Index flags: live=%d ready=%d valid=%d",
     oIndex->indislive, oIndex->indisready, oIndex->indisvalid);
```

### Verify System Tree Updates

```sql
-- Query system tree directly
SELECT * FROM orioledb_sys_tree_rows(3)  -- 3 = SYS_TREES_O_INDICES
WHERE ... ;
```

### Check UNDO Log

```c
/* Verify UNDO records are created */
elog(DEBUG1, "Creating UNDO record for index flag update");
```

## Files to Modify Summary

1. **`include/catalog/o_indices.h`** - Add flag fields to OIndex struct
2. **`src/catalog/o_indices.c`** - Update serialize/deserialize, increment data_version
3. **`src/catalog/o_indices.c`** - Initialize flags in make_o_index()
4. **`src/btree/build.c`** - Set indisready=true after build
5. **`src/indexam/handler.c`** - Check indisvalid in query planner
6. **`src/tableam/operations.c`** - Check indisready in insert
7. **`src/catalog/ddl.c`** - Handle flags during index drop
8. **`src/catalog/indices.c`** (optional) - Sync with pg_index

## Estimated Effort

- **Core implementation:** 4-8 hours
- **Testing:** 4-6 hours
- **Integration:** 2-4 hours
- **Total:** 10-18 hours

## References

- **Existing flag example:** `bridging` field (80+ usages in codebase)
- **System trees architecture:** `SYSTEM_TREES_ANALYSIS.md`
- **Serialization code:** `src/catalog/o_indices.c:638-717`
- **UNDO log system:** `doc/architecture/overview.mdx`

## Next Steps

1. Follow steps 1-11 above
2. Write tests for each flag transition
3. Test transactional behavior (commit/rollback)
4. Test persistence (restart)
5. Document behavior in code comments
6. Update user documentation if needed

Good luck with your implementation! The system trees architecture is well-designed, so this should be a straightforward addition following the existing patterns.
