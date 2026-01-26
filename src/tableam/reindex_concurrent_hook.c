/*-------------------------------------------------------------------------
 *
 * reindex_concurrent_hook.c
 *		Hook for synchronizing orioledb system trees during REINDEX CONCURRENTLY
 *
 * PostgreSQL's REINDEX CONCURRENTLY goes through multiple stages where it
 * updates index metadata in system catalogs (pg_index). Orioledb maintains
 * its own metadata in system trees (SYS_TREES_O_INDICES) which needs to be
 * kept synchronized during these stages.
 *
 * The main stages that require synchronization are:
 * 1. Index creation (handled by o_define_index)
 * 2. Setting index state flags (indisready, indislive, indisvalid)
 * 3. Index swap (swapping relfilenodes between old and new index)
 * 4. Cleanup (removing old concurrent index structure)
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/reindex_concurrent_hook.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "transam/oxid.h"

#include "access/relation.h"
#include "catalog/index.h"
#include "utils/rel.h"

/*
 * orioledb_index_set_state_flags
 *
 * General function for setting index state flags in orioledb system trees.
 * This synchronizes orioledb's metadata with PostgreSQL's pg_index flags
 * (indislive, indisready, indisvalid).
 *
 * Parameters:
 *   heapRelation - The table relation
 *   indexRelation - The index relation to update
 *   flags - Structure specifying which flags to set and their values
 *
 * Note: This function updates orioledb's system tree metadata but does NOT
 * modify PostgreSQL's system catalogs - that is PostgreSQL's responsibility.
 * Call this AFTER updating pg_index.
 */
void
orioledb_index_set_state_flags(Relation heapRelation,
								Relation indexRelation,
								IndexStateFlags *flags)
{
	OTable	   *o_table;
	ORelOids	oids;
	OIndexNumber ix_num = InvalidIndexNumber;
	int			i;
	OXid		oxid = get_current_oxid();
	CommitSeqNo csn = COMMITSEQNO_INPROGRESS;
	StringInfoData log_msg;

	/* Only process orioledb tables */
	if (!is_orioledb_rel(heapRelation))
		return;

	ORelOidsSetFromRel(oids, heapRelation);
	o_table = o_tables_get(oids);
	if (o_table == NULL)
		return;

	/* Find the index in o_table->indices by reloid */
	for (i = 0; i < o_table->nindices; i++)
	{
		if (o_table->indices[i].oids.reloid == indexRelation->rd_rel->oid)
		{
			ix_num = i;
			break;
		}
	}

	if (ix_num == InvalidIndexNumber)
	{
		elog(DEBUG1, "orioledb_index_set_state_flags: "
			 "index reloid %u not found in table", indexRelation->rd_rel->oid);
		o_table_free(o_table);
		return;
	}

	/* Build debug log message */
	initStringInfo(&log_msg);
	appendStringInfo(&log_msg, "orioledb_index_set_state_flags: index reloid %u, setting flags:",
					 indexRelation->rd_rel->oid);
	if (flags->set_live)
		appendStringInfo(&log_msg, " indislive=%s", flags->live_value ? "true" : "false");
	if (flags->set_ready)
		appendStringInfo(&log_msg, " indisready=%s", flags->ready_value ? "true" : "false");
	if (flags->set_valid)
		appendStringInfo(&log_msg, " indisvalid=%s", flags->valid_value ? "true" : "false");
	
	elog(DEBUG1, "%s", log_msg.data);
	pfree(log_msg.data);

	/*
	 * Note: The OIndex structure in orioledb doesn't have explicit
	 * indislive/indisready/indisvalid flags. Orioledb relies on
	 * PostgreSQL's catalogs for visibility and state control.
	 * 
	 * However, we still need to call o_indices_update() to ensure
	 * the metadata is properly synchronized and any cached descriptors
	 * are invalidated.
	 */
	o_indices_update(o_table, ix_num, oxid, csn);

	o_table_free(o_table);
}

/*
 * orioledb_reindex_concurrent_swap
 *
 * Swap relfilenodes between old and new index in orioledb's system trees.
 * Call this AFTER PostgreSQL has swapped the relfilenodes in pg_class.
 *
 * After PostgreSQL swaps the relfilenodes in pg_class:
 * - oldIndex->rd_rel->relfilenode now points to the new data
 * - newIndex->rd_rel->relfilenode now points to the old data
 *
 * We need to update orioledb's system trees to reflect this swap.
 */
void
orioledb_reindex_concurrent_swap(Relation heapRelation,
								  Relation oldIndex,
								  Relation newIndex)
{
	OTable	   *o_table;
	ORelOids	oids;
	ORelOids	old_index_oids;
	ORelOids	new_index_oids;
	OIndexNumber old_ix_num = InvalidIndexNumber;
	OIndexNumber new_ix_num = InvalidIndexNumber;
	int			i;
	OXid		oxid = get_current_oxid();
	CommitSeqNo csn = COMMITSEQNO_INPROGRESS;

	/* Only process orioledb tables */
	if (!is_orioledb_rel(heapRelation))
		return;

	ORelOidsSetFromRel(oids, heapRelation);
	o_table = o_tables_get(oids);
	if (o_table == NULL)
		return;

	/*
	 * Find the old and new index structures in o_table->indices
	 * We identify them by reloid (index OID)
	 */
	for (i = 0; i < o_table->nindices; i++)
	{
		if (o_table->indices[i].oids.reloid == oldIndex->rd_rel->oid)
			old_ix_num = i;
		if (o_table->indices[i].oids.reloid == newIndex->rd_rel->oid)
			new_ix_num = i;
	}

	if (old_ix_num != InvalidIndexNumber && new_ix_num != InvalidIndexNumber)
	{
		elog(DEBUG1, "orioledb_reindex_concurrent_swap: "
			 "old index reloid %u (relfilenode %u -> %u), "
			 "new index reloid %u (relfilenode %u -> %u)",
			 o_table->indices[old_ix_num].oids.reloid,
			 o_table->indices[old_ix_num].oids.relnode,
			 oldIndex->rd_rel->relfilenode,
			 o_table->indices[new_ix_num].oids.reloid,
			 o_table->indices[new_ix_num].oids.relnode,
			 newIndex->rd_rel->relfilenode);

		/* Save the ORelOids for invalidation */
		old_index_oids = o_table->indices[old_ix_num].oids;
		new_index_oids = o_table->indices[new_ix_num].oids;

		/*
		 * Swap the relfilenodes in our structures to match what
		 * PostgreSQL just did in pg_class.
		 */
		o_table->indices[old_ix_num].oids.relnode = oldIndex->rd_rel->relfilenode;
		o_table->indices[new_ix_num].oids.relnode = newIndex->rd_rel->relfilenode;

		/* Update both indices in the system tree */
		o_indices_update(o_table, old_ix_num, oxid, csn);
		o_indices_update(o_table, new_ix_num, oxid, csn);

		/*
		 * Add invalidation undo items to ensure cached descriptors are
		 * invalidated on commit. This is critical because the relfilenode
		 * swap must persist beyond the transaction - PostgreSQL's swap
		 * in pg_class is permanent and our system tree must stay synchronized.
		 *
		 * We invalidate using the ORelOids we saved before the swap to ensure
		 * we invalidate based on the original structure identifiers.
		 */
		o_add_invalidate_undo_item(old_index_oids, O_INVALIDATE_OIDS_ON_COMMIT);
		o_add_invalidate_undo_item(new_index_oids, O_INVALIDATE_OIDS_ON_COMMIT);
	}

	o_table_free(o_table);
}

/*
 * orioledb_reindex_concurrent_set_ready
 *
 * Convenience wrapper for setting indisready flag.
 * Call this AFTER updating pg_index.
 */
void
orioledb_reindex_concurrent_set_ready(Relation heapRelation,
									   Relation indexRelation,
									   bool ready)
{
	IndexStateFlags flags = {0};
	flags.set_ready = true;
	flags.ready_value = ready;
	orioledb_index_set_state_flags(heapRelation, indexRelation, &flags);
}

/*
 * orioledb_reindex_concurrent_set_dead
 *
 * Convenience wrapper for marking index as dead (indislive=false).
 * Call this AFTER updating pg_index.
 */
void
orioledb_reindex_concurrent_set_dead(Relation heapRelation,
									  Relation oldIndex,
									  Relation newIndex)
{
	IndexStateFlags flags = {0};
	flags.set_live = true;
	flags.live_value = false;
	orioledb_index_set_state_flags(heapRelation, oldIndex, &flags);
}

/*
 * orioledb_reindex_concurrent_set_valid
 *
 * Convenience wrapper for marking index as valid (indisvalid=true).
 * Call this AFTER updating pg_index.
 */
void
orioledb_reindex_concurrent_set_valid(Relation heapRelation,
									   Relation oldIndex,
									   Relation newIndex)
{
	IndexStateFlags flags = {0};
	flags.set_valid = true;
	flags.valid_value = true;
	orioledb_index_set_state_flags(heapRelation, oldIndex, &flags);
}

/*
 * orioledb_reindex_concurrent_set_invalid
 *
 * Convenience wrapper for marking index as invalid (indisvalid=false).
 * Call this AFTER updating pg_index.
 */
void
orioledb_reindex_concurrent_set_invalid(Relation heapRelation,
										 Relation oldIndex,
										 Relation newIndex)
{
	IndexStateFlags flags = {0};
	flags.set_valid = true;
	flags.valid_value = false;
	orioledb_index_set_state_flags(heapRelation, newIndex, &flags);
}

/*
 * orioledb_reindex_concurrent_cleanup
 *
 * Final cleanup of old index structure.
 * Removes the old concurrent index from orioledb's system trees.
 */
void
orioledb_reindex_concurrent_cleanup(Relation heapRelation,
									 Relation oldIndex,
									 Relation newIndex)
{
	elog(DEBUG1, "orioledb_reindex_concurrent_cleanup: "
		 "cleaning up old index structure");
	
	/*
	 * The cleanup function expects the "new" index relation
	 * to have the updated relfilenode from the swap.
	 * After the swap, oldIndex points to what was the new data.
	 */
	orioledb_index_validate_cleanup_old_concurrent(heapRelation, oldIndex);
}
