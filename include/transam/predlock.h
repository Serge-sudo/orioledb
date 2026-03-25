/*-------------------------------------------------------------------------
 *
 * predlock.h
 *		Declarations for OrioleDB predicate locking.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/transam/predlock.h
 *
 * OVERVIEW
 *	  Predicate locks are lightweight read locks that track which keys have
 *	  been read (e.g. by foreign key reference checks) without writing any
 *	  undo records to the underlying B-tree pages.  This avoids marking
 *	  pages as dirty and prevents unnecessary checkpoints when no data has
 *	  actually changed.
 *
 *	  Locks are stored per-backend in shared memory.  If a backend accumulates
 *	  too many fine-grained (tuple-level) locks on the same page, they are
 *	  promoted to a single page-level lock covering the page's [lokey, hikey)
 *	  range.  Page-level locks are maintained as a monotone set of disjoint
 *	  intervals; when space is tight, neighbouring intervals are merged
 *	  instead of promoting to a tree-level lock.
 *
 *	  When another backend attempts a DELETE or UPDATE that could conflict
 *	  with a predicate lock it must wait for the lock holder to commit or
 *	  abort, just as it would wait for a regular row-level lock.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TRANSAM_PREDLOCK_H__
#define __TRANSAM_PREDLOCK_H__

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/modify.h"
#include "btree/page_contents.h"
#include "transam/oxid.h"

/*
 * Maximum number of bytes stored for a predicate lock key (both tuple key
 * and page hikey / lokey).  Keys longer than this are handled by immediately
 * promoting the lock to the page level.
 */
#define O_PRED_LOCK_MAX_KEY_SIZE	256

/*
 * Maximum number of predicate lock entries per backend.  Once this limit is
 * reached new tuple-level locks are immediately promoted to page level (and
 * page-level locks to tree level) to bound memory usage.
 */
#define O_PRED_LOCKS_MAX_ENTRIES	64

/*
 * Minimum and maximum thresholds for tuple-level lock promotion.
 * The actual threshold is calculated dynamically based on the relation's
 * tuple size to be ~75% of max tuples per page.
 */
#define O_PRED_LOCK_PAGE_PROMOTE_THRESHOLD_MIN	8
#define O_PRED_LOCK_PAGE_PROMOTE_THRESHOLD_MAX	256

/* Reserved for potential future tuning of page-level consolidation. */
#define O_PRED_LOCK_TREE_PROMOTE_THRESHOLD	8

/*
 * Granularity of a predicate lock entry.
 */
typedef enum OPredLockLevel
{
	OPredLockLevelTuple = 0,	/* exact tuple key */
	OPredLockLevelPage = 1,		/* all tuples in [loKey, hiKey) on one page */
	OPredLockLevelTree = 2,		/* entire B-tree */
} OPredLockLevel;

/*
 * Compact key representation used inside predicate lock entries.
 *
 * Keys longer than O_PRED_LOCK_MAX_KEY_SIZE cannot be stored; the caller
 * must promote to a coarser lock level in that case.
 */
typedef struct OPredLockKey
{
	bool		notNull;		/* false = key is NULL / not set */
	uint8		formatFlags;	/* OTuple formatFlags */
	uint16		len;			/* bytes in data[] */
	char		data[O_PRED_LOCK_MAX_KEY_SIZE];
} OPredLockKey;

/*
 * One predicate lock entry.  All fields are valid only when valid == true.
 */
typedef struct OPredLockEntry
{
	bool		valid;			/* slot is occupied */
	ORelOids	oids;			/* identifies the B-tree */
	OXid		oxid;			/* owning transaction */
	OPredLockLevel level;		/* lock granularity */

	/*
	 * For OPredLockLevelTuple: key is the exact tuple key.
	 * For OPredLockLevelPage:  key is the page highkey (exclusive upper
	 *                           bound); loKey is the page lokey (inclusive
	 *                           lower bound, notNull=false means leftmost).
	 * For OPredLockLevelTree:  key and loKey are unused.
	 */
	OPredLockKey key;
	OPredLockKey loKey;
} OPredLockEntry;

/*
 * Per-backend predicate lock table, stored in shared memory.
 */
typedef struct OPredLocksData
{
	LWLock		lock;			/* protects entries[] */
	int			numValid;		/* number of valid entries */
	int			trancheId;		/* LWLock tranche id */
	OPredLockEntry entries[O_PRED_LOCKS_MAX_ENTRIES];
} OPredLocksData;

/* Global array indexed by MYPROCNUMBER, allocated in shared memory. */
extern OPredLocksData *o_pred_locks;

/* Shared memory sizing / initialisation (called from orioledb.c). */
extern Size o_pred_lock_shmem_size(void);
extern void o_pred_lock_shmem_init(Pointer ptr, bool found);

/*
 * Calculate the dynamic promotion threshold for a relation.
 * Returns ~75% of the estimated max tuples per page based on O_BTREE_MAX_TUPLE_SIZE.
 */
extern int o_pred_lock_get_promote_threshold(BTreeDescr *desc);

/*
 * Acquire a predicate lock on the tuple currently pointed to by
 * pageFindContext / blkno.  Called from o_btree_modify_lock() when the
 * requested lock mode is RowLockKeyShare.
 *
 * desc      - B-tree descriptor
 * curTuple  - the leaf tuple being locked
 * page      - the B-tree leaf page containing curTuple
 * context   - find-page context (used to obtain the page lokey)
 * oxid      - transaction acquiring the lock
 */
extern void o_pred_lock_acquire(BTreeDescr *desc,
								OTuple curTuple,
								Page page,
								OBTreeFindPageContext *context,
								OXid oxid);

/*
 * Check whether any currently-held predicate lock conflicts with the
 * requested operation on key/keyType in the tree described by desc.
 *
 * Returns true if a conflicting predicate lock was found, and sets
 * *conflictOxid to the owner of that lock.  The caller is responsible for
 * waiting for conflictOxid to finish.
 *
 * lockMode   - the lock mode being requested by the caller (e.g.
 *              RowLockUpdate for DELETE, RowLockNoKeyUpdate for UPDATE).
 * myOxid     - the requesting transaction (self-locks are never conflicts).
 * conflictOxid - set when returning true.
 * lockStatus - updated with BTreeModifyWeakerLock if this transaction
 *              already holds a weaker predicate lock on the same key.
 */
extern bool o_pred_lock_check(BTreeDescr *desc,
							  OTuple key,
							  BTreeKeyType keyType,
							  RowLockMode lockMode,
							  OXid myOxid,
							  OXid *conflictOxid,
							  BTreeModifyLockStatus *lockStatus);

/*
 * Release all predicate lock entries belonging to oxid.  Called on
 * transaction commit or abort.
 */
extern void o_pred_lock_release_all(OXid oxid);

#endif							/* __TRANSAM_PREDLOCK_H__ */
