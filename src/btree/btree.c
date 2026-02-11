/*-------------------------------------------------------------------------
 *
 * btree.c
 *		Routines for OrioleDB B-tree initilization and cleanup.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/btree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/o_tables.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/tree.h"
#include "transam/undo.h"
#include "transam/oxid.h"
#include "tuple/format.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"

LWLockPadded *unique_locks;
int			num_unique_locks;

void
o_btree_init_unique_lwlocks(void)
{
	num_unique_locks = max_procs * 4;
	unique_locks = GetNamedLWLockTranche("orioledb_unique_locks");
}

void
o_btree_init(BTreeDescr *desc)
{
	init_new_btree_page(desc, desc->rootInfo.rootPageBlkno,
						O_BTREE_FLAGS_ROOT_INIT, 0, false);
	init_page_first_chunk(desc, O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno), 0);
	unlock_page(desc->rootInfo.rootPageBlkno);
	init_meta_page(desc->rootInfo.metaPageBlkno, 1);

	/*
	 * Don't mark the root page dirty by default to skip checkpointing of the
	 * empty trees.  Except for the system trees, which are checkpointed every
	 * time.
	 */
	if (IS_SYS_TREE_OIDS(desc->oids))
		MARK_DIRTY(desc, desc->rootInfo.rootPageBlkno);
}

static bool
get_page_children(OInMemoryBlkno blkno, uint32 pageChangeCount,
				  OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS],
				  uint32 childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS],
				  int *childPagesCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	BTreePageItemLocator loc;
	int			ionum;

retry:
	lock_page(blkno);
	if (desc->ionum >= 0)
	{
		ionum = desc->ionum;
		unlock_page(blkno);

		wait_for_io_completion(ionum);
		goto retry;
	}
	*childPagesCount = 0;

	if (O_PAGE_GET_CHANGE_COUNT(p) != pageChangeCount)
	{
		/*
		 * It seems that page has been evicted concurrently.  So, nothing to
		 * do.
		 */
		unlock_page(blkno);
		return false;
	}

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			{
				ionum = DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink);
				unlock_page(blkno);

				wait_for_io_completion(ionum);
				goto retry;
			}
			else if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				childPageNumbers[*childPagesCount] = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
				childPageChangeCounts[*childPagesCount] = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);
				(*childPagesCount)++;
			}
		}
	}
	return true;
}

/*
 * Recursively sets O_BTREE_FLAG_PRE_CLEANUP to the given page and all its
 * children.
 */
static void
mark_page_pre_cleanup(OInMemoryBlkno blkno, uint32 pageChangeCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			childPagesCount;
	int			i,
				ionum;

	if (!get_page_children(blkno, pageChangeCount,
						   childPageNumbers, childPageChangeCounts,
						   &childPagesCount))
		return;

	page_block_reads(blkno);
	header->flags |= O_BTREE_FLAG_PRE_CLEANUP;
	ionum = O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum;
	unlock_page(blkno);

	if (ionum >= 0)
		wait_for_io_completion(ionum);

	for (i = 0; i < childPagesCount; i++)
		mark_page_pre_cleanup(childPageNumbers[i],
							  childPageChangeCounts[i]);
}

/*
 * Frees given page and all of its children recursively.
 */
static void
free_page(OPagePool *pool, OInMemoryBlkno blkno, uint32 pageChangeCount)
{
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			childPagesCount;
	int			i;

	if (!get_page_children(blkno, pageChangeCount,
						   childPageNumbers, childPageChangeCounts,
						   &childPagesCount))
		return;
	Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(blkno), PRE_CLEANUP));
	Assert(O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(blkno)) == pageChangeCount);
	Assert(O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum < 0);
	unlock_page(blkno);

	for (i = 0; i < childPagesCount; i++)
		free_page(pool,
				  childPageNumbers[i],
				  childPageChangeCounts[i]);

	lock_page(blkno);
	Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(blkno), PRE_CLEANUP));
	Assert(O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(blkno)) == pageChangeCount);
	Assert(O_GET_IN_MEMORY_PAGEDESC(blkno)->ionum < 0);
	page_block_reads(blkno);
	CLEAN_DIRTY(pool, blkno);
	ppool_free_page(pool, blkno, true);

}

static inline void
free_meta_page(OPagePool *pool, OInMemoryBlkno metaPageBlkno)
{
	BTreeMetaPage *meta_page;
	int			i,
				j;

	meta_page = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(metaPageBlkno);
	for (i = 0; i < 2; i++)
	{
		FREE_PAGE_IF_VALID(pool, meta_page->freeBuf.pages[i]);
		for (j = 0; j < 2; j++)
		{
			FREE_PAGE_IF_VALID(pool, meta_page->nextChkp[j].pages[i]);
			FREE_PAGE_IF_VALID(pool, meta_page->tmpBuf[j].pages[i]);
		}
	}
	ppool_free_page(pool, metaPageBlkno, NULL);
}

/*
 * Two phase algorithm for pages cleanup, which can run concurrently
 * to walk_page().
 *
 * The first phase sets O_BTREE_FLAG_PRE_CLEANUP preventing walk_page() from
 * evicting or writing these pages.
 *
 * The second phase cleans pages previously marked with
 * O_BTREE_FLAG_PRE_CLEANUP flag from bottom to top.
 *
 * Therefore walk_page() never gets in trouble trying to find parent page
 * using find_page().
 */
void
o_btree_cleanup_pages(OInMemoryBlkno rootPageBlkno, OInMemoryBlkno metaPageBlkno, uint32 rootPageChangeCount)
{
	OPagePool  *pool = get_ppool_by_blkno(rootPageBlkno);

	Assert(OInMemoryBlknoIsValid(rootPageBlkno));
	Assert(OInMemoryBlknoIsValid(metaPageBlkno));
	Assert(pool != NULL);

	mark_page_pre_cleanup(rootPageBlkno, rootPageChangeCount);
	free_page(pool, rootPageBlkno, rootPageChangeCount);

	free_meta_page(pool, metaPageBlkno);
}

void
o_btree_check_size_of_tuple(int len, char *relation_name, bool index)
{
	if (len > O_BTREE_MAX_TUPLE_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row size %d exceeds orioledb maximum %zu for %s \"%s\"",
						len,
						O_BTREE_MAX_TUPLE_SIZE,
						index ? "index" : "table",
						relation_name)));
}

ItemPointerData
btree_ctid_get_and_inc(BTreeDescr *desc)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	ItemPointerData result;
	uint64		ctid = pg_atomic_fetch_add_u64(&metaPageBlkno->ctid, 1);

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	Assert(ctid / (MaxOffsetNumber - FirstOffsetNumber) < InvalidBlockNumber);

	ItemPointerSet(&result,
				   (uint32) (ctid / (MaxOffsetNumber - FirstOffsetNumber)),
				   (OffsetNumber) (ctid % (MaxOffsetNumber - FirstOffsetNumber) + FirstOffsetNumber));
	return result;
}

void
btree_ctid_update_if_needed(BTreeDescr *desc, ItemPointerData ctid)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	uint64		old_ctid,
				new_ctid;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	new_ctid = (uint64) ItemPointerGetBlockNumber(&ctid) * (MaxOffsetNumber - FirstOffsetNumber);
	new_ctid += ctid.ip_posid - FirstOffsetNumber;
	Assert(new_ctid < (uint64) (MaxOffsetNumber - FirstOffsetNumber) * (uint64) InvalidBlockNumber);

	new_ctid++;
	do
	{
		old_ctid = pg_atomic_read_u64(&metaPageBlkno->ctid);
		if (old_ctid >= new_ctid)
			break;
	} while (!pg_atomic_compare_exchange_u64(&metaPageBlkno->ctid, &old_ctid, new_ctid));
}

ItemPointerData
btree_bridge_ctid_get_and_inc(BTreeDescr *desc, bool *overflow)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	ItemPointerData result;
	uint64		ctid = pg_atomic_fetch_add_u64(&metaPageBlkno->bridge_ctid, 1);
	BlockNumber max_block_number = MaxBlockNumber;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));

	if (BlockNumberIsValid(max_bridge_ctid_blkno))
		max_block_number = max_bridge_ctid_blkno;

	*overflow = ctid / MaxHeapTuplesPerPage >= max_block_number;

	ItemPointerSet(&result,
				   (uint32) (ctid / MaxHeapTuplesPerPage % max_block_number),
				   (OffsetNumber) (ctid % MaxHeapTuplesPerPage + FirstOffsetNumber));
	return result;
}

static inline OIndexDescr *
o_get_tree_def(BTreeDescr *desc)
{
	return desc->arg;
}

void
btree_desc_stopevent_params_internal(BTreeDescr *desc, JsonbParseState **state)
{
	jsonb_push_int8_key(state, "datoid", desc->oids.datoid);
	jsonb_push_int8_key(state, "reloid", desc->oids.reloid);
	jsonb_push_int8_key(state, "relnode", desc->oids.relnode);

	if (IS_SYS_TREE_OIDS(desc->oids))
		jsonb_push_string_key(state, "treeName", "sys_tree");
	else if (desc->type == oIndexToast)
		jsonb_push_string_key(state, "treeName", "toast");
	else
		jsonb_push_string_key(state, "treeName", o_get_tree_def(desc)->name.data);
}

void
btree_page_stopevent_params_internal(BTreeDescr *desc, Page p,
									 JsonbParseState **state)
{
	jsonb_push_int8_key(state, "level", PAGE_GET_LEVEL(p));
	jsonb_push_int8_key(state, "pageChangeCount", O_PAGE_GET_CHANGE_COUNT(p));

	jsonb_push_key(state, "hikey");
	if (!O_PAGE_IS(p, RIGHTMOST))
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, p);
		(void) o_btree_key_to_jsonb(desc, hikey, state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(state, WJB_VALUE, &jval);
	}
}

Jsonb *
btree_page_stopevent_params(BTreeDescr *desc, Page p)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	btree_page_stopevent_params_internal(desc, p, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

Jsonb *
btree_downlink_stopevent_params(BTreeDescr *desc, Page p, BTreePageItemLocator *loc)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);
	BTreeNonLeafTuphdr *internal_ptr;

	internal_ptr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, loc);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	btree_page_stopevent_params_internal(desc, p, &state);

	jsonb_push_key(&state, "downlink");
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "blkno", DOWNLINK_GET_IN_MEMORY_BLKNO(internal_ptr->downlink));
	jsonb_push_int8_key(&state, "pageChangeCount", DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(internal_ptr->downlink));
	jsonb_push_key(&state, "key");
	if (BTREE_PAGE_LOCATOR_GET_OFFSET(p, loc) > 0)
	{
		OTuple		key;

		BTREE_PAGE_READ_INTERNAL_TUPLE(key, p, loc);
		(void) o_btree_key_to_jsonb(desc, key, &state);
	}
	else
	{
		JsonbValue	jval;

		jval.type = jbvNull;
		(void) pushJsonbValue(&state, WJB_VALUE, &jval);
	}
	pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

/*
 * Set the validation boundary for concurrent index build.
 * The boundary represents the current primary key value up to which
 * validation has been completed.
 */
void
/*
 * Set the validation boundary for concurrent index builds.
 * The boundary is stored as an OBTreeKeyBound in the meta page.
 */
void
/*
 * Serialize an OBTreeKeyBound into a byte buffer.
 * Returns the number of bytes written.
 */
static int
serialize_key_bound(OBTreeKeyBound *boundary, char *buffer, int bufsize)
{
	char	   *ptr = buffer;
	int			i;
	int16		typlen;
	bool		typbyval;

	Assert(boundary != NULL);
	Assert(buffer != NULL);

	for (i = 0; i < boundary->nkeys; i++)
	{
		OBTreeValueBound *key = &boundary->keys[i];

		/* Store type info */
		if (ptr + sizeof(Oid) > buffer + bufsize)
			elog(ERROR, "validation boundary data too large");
		memcpy(ptr, &key->type, sizeof(Oid));
		ptr += sizeof(Oid);

		/* Store flags */
		if (ptr + sizeof(uint8) > buffer + bufsize)
			elog(ERROR, "validation boundary data too large");
		memcpy(ptr, &key->flags, sizeof(uint8));
		ptr += sizeof(uint8);

		/* Get type info for serialization */
		get_typlenbyval(key->type, &typlen, &typbyval);

		/* Serialize the value */
		if (key->flags & O_VALUE_BOUND_NULL)
		{
			/* NULL value - nothing to store */
		}
		else if (typbyval)
		{
			/* Pass-by-value: store the Datum directly */
			if (ptr + sizeof(Datum) > buffer + bufsize)
				elog(ERROR, "validation boundary data too large");
			memcpy(ptr, &key->value, sizeof(Datum));
			ptr += sizeof(Datum);
		}
		else if (typlen == -1)
		{
			/* Variable length type */
			int			len = VARSIZE_ANY(DatumGetPointer(key->value));

			if (ptr + sizeof(int) + len > buffer + bufsize)
				elog(ERROR, "validation boundary data too large");

			memcpy(ptr, &len, sizeof(int));
			ptr += sizeof(int);
			memcpy(ptr, DatumGetPointer(key->value), len);
			ptr += len;
		}
		else if (typlen > 0)
		{
			/* Fixed length pass-by-reference type */
			if (ptr + typlen > buffer + bufsize)
				elog(ERROR, "validation boundary data too large");

			memcpy(ptr, DatumGetPointer(key->value), typlen);
			ptr += typlen;
		}
		else
		{
			elog(ERROR, "unsupported type length: %d", typlen);
		}
	}

	return ptr - buffer;
}

/*
 * Deserialize an OBTreeKeyBound from a byte buffer.
 * Allocates memory for pass-by-reference types using palloc.
 */
static void
deserialize_key_bound(OBTreeKeyBound *boundary, int nkeys, char *buffer)
{
	char	   *ptr = buffer;
	int			i;
	int16		typlen;
	bool		typbyval;

	Assert(boundary != NULL);
	Assert(buffer != NULL);

	boundary->nkeys = nkeys;

	for (i = 0; i < nkeys; i++)
	{
		OBTreeValueBound *key = &boundary->keys[i];

		/* Read type info */
		memcpy(&key->type, ptr, sizeof(Oid));
		ptr += sizeof(Oid);

		/* Read flags */
		memcpy(&key->flags, ptr, sizeof(uint8));
		ptr += sizeof(uint8);

		/* Get type info for deserialization */
		get_typlenbyval(key->type, &typlen, &typbyval);

		/* Deserialize the value */
		if (key->flags & O_VALUE_BOUND_NULL)
		{
			/* NULL value */
			key->value = (Datum) 0;
		}
		else if (typbyval)
		{
			/* Pass-by-value: read the Datum directly */
			memcpy(&key->value, ptr, sizeof(Datum));
			ptr += sizeof(Datum);
		}
		else if (typlen == -1)
		{
			/* Variable length type - allocate and copy */
			int			len;
			void	   *data;

			memcpy(&len, ptr, sizeof(int));
			ptr += sizeof(int);

			data = palloc(len);
			memcpy(data, ptr, len);
			ptr += len;

			key->value = PointerGetDatum(data);
		}
		else if (typlen > 0)
		{
			/* Fixed length pass-by-reference type - allocate and copy */
			void	   *data = palloc(typlen);

			memcpy(data, ptr, typlen);
			ptr += typlen;

			key->value = PointerGetDatum(data);
		}
		else
		{
			elog(ERROR, "unsupported type length: %d", typlen);
		}

		/* Set comparator to NULL - will be initialized if needed */
		key->comparator = NULL;
	}
}

btree_set_validation_boundary(BTreeDescr *desc, OBTreeKeyBound *boundary)
{
	BTreeMetaPage *metaPage;
	int			serialized_len;

	Assert(desc != NULL);
	Assert(boundary != NULL);

	metaPage = BTREE_GET_META(desc);
	LWLockAcquire(&metaPage->validationBoundaryLock, LW_EXCLUSIVE);

	/* Serialize the boundary data into the meta page buffer */
	serialized_len = serialize_key_bound(boundary,
										  metaPage->validationBoundaryData,
										  sizeof(metaPage->validationBoundaryData));

	metaPage->validationBoundaryNKeys = boundary->nkeys;
	metaPage->validationBoundaryLen = serialized_len;
	metaPage->validationBoundaryValid = true;

	LWLockRelease(&metaPage->validationBoundaryLock);

#ifdef USE_ASSERT_CHECKING
	/* Debug: log when boundary is set */
	btree_print_validation_boundary(desc, boundary);
#endif
}

/*
 * Get the current validation boundary.
 * Returns true if a boundary is set, false otherwise.
 * The boundary is deserialized from shared memory into the caller-provided structure.
 */
bool
btree_get_validation_boundary(BTreeDescr *desc, OBTreeKeyBound *boundary)
{
	BTreeMetaPage *metaPage;
	bool		valid;
	int			nkeys;

	Assert(desc != NULL);
	Assert(boundary != NULL);

	metaPage = BTREE_GET_META(desc);
	LWLockAcquire(&metaPage->validationBoundaryLock, LW_SHARED);

	valid = metaPage->validationBoundaryValid;
	if (!valid)
	{
		LWLockRelease(&metaPage->validationBoundaryLock);
		return false;
	}

	/* Deserialize the boundary data from the meta page buffer */
	nkeys = metaPage->validationBoundaryNKeys;
	deserialize_key_bound(boundary, nkeys, metaPage->validationBoundaryData);

	LWLockRelease(&metaPage->validationBoundaryLock);

	return true;
}

/*
 * Clear the validation boundary, signaling that validation is complete.
 */
void
btree_clear_validation_boundary(BTreeDescr *desc)
{
	BTreeMetaPage *metaPage;

	Assert(desc != NULL);

	metaPage = BTREE_GET_META(desc);
	LWLockAcquire(&metaPage->validationBoundaryLock, LW_EXCLUSIVE);

	metaPage->validationBoundaryValid = false;

	LWLockRelease(&metaPage->validationBoundaryLock);
}

/*
 * Check if a primary key satisfies the validation boundary.
 * Returns true if:
 * - No validation is in progress (boundary not set), OR
 * - The PK is less than or equal to the boundary
 * Returns false if PK is greater than the boundary.
 */
bool
btree_pk_satisfies_validation_boundary(BTreeDescr *desc, OBTreeKeyBound *pk)
{
	OBTreeKeyBound boundary;
	int			cmp;

	Assert(desc != NULL);
	Assert(pk != NULL);

	/* Get the deserialized boundary */
	if (!btree_get_validation_boundary(desc, &boundary))
	{
		/* No validation in progress */
		return true;
	}

	/* Compare PK with boundary */
	cmp = o_btree_cmp(desc, pk, BTreeKeyBound,
					  &boundary, BTreeKeyBound);

	/* Return true if PK <= boundary */
	return (cmp <= 0);
}

/*
 * Debug function to print the validation boundary when it is set.
 * This helps with debugging concurrent index validation.
 */
void
btree_print_validation_boundary(BTreeDescr *desc, OBTreeKeyBound *boundary)
{
	StringInfoData buf;
	int			i;

	Assert(desc != NULL);
	Assert(boundary != NULL);

	initStringInfo(&buf);

	appendStringInfo(&buf, "Validation boundary set for index %u: nkeys=%d, keys=(", 
					 desc->oids.datoid, boundary->nkeys);

	/* Print the tuple key values using type-specific output functions */
	for (i = 0; i < boundary->nkeys; i++)
	{
		if (i > 0)
			appendStringInfo(&buf, ", ");
		
		/* Check if the value is NULL */
		if (boundary->keys[i].flags & O_VALUE_BOUND_NULL)
		{
			appendStringInfo(&buf, "null");
		}
		else
		{
			Oid			typoutput;
			bool		typisvarlena;
			char	   *res;

			/* Get the output function for this type */
			getTypeOutputInfo(boundary->keys[i].type, &typoutput, &typisvarlena);
			
			/* Convert the Datum to string using the type's output function */
			res = OidOutputFunctionCall(typoutput, boundary->keys[i].value);
			appendStringInfo(&buf, "'%s'", res);
		}
	}

	appendStringInfo(&buf, ")");
	
	elog(DEBUG1, "%s", buf.data);
	pfree(buf.data);
}
