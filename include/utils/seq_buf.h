/*-------------------------------------------------------------------------
 *
 * seq_buf.h
 *		Declarations for sequential buffered data access routines.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/seq_buf.h
 *
 * OVERVIEW
 *
 * Sequential buffers (seq bufs) provide efficient buffered sequential I/O
 * for checkpoint-related data in OrioleDB B-trees.  Rather than performing
 * random I/O or holding all checkpoint metadata in memory, seq bufs stream
 * data to and from on-disk files using two in-memory OrioleDB pages as a
 * double-buffer.  While one page is being filled (or drained), the other
 * can be read ahead from or written back to disk in the background.
 *
 * Each checkpointable B-tree descriptor (BTreeDescr) holds three kinds of
 * seq bufs:
 *
 *   freeBuf      – reads the list of free disk extents that were recorded
 *                  by the previous checkpoint.  During the current checkpoint
 *                  those extents may be reused for newly written pages.
 *
 *   nextChkp[2]  – writes the checkpoint map file for the *next* checkpoint.
 *                  Each entry records the on-disk location of a B-tree page
 *                  so that a subsequent checkpoint can find it without
 *                  scanning all tree nodes again.  The two-element array
 *                  alternates between even and odd checkpoint numbers
 *                  (index = chkpNum % 2), allowing one checkpoint to read
 *                  the map written by the previous one while the current one
 *                  builds its own.
 *
 *   tmpBuf[2]    – writes a temporary file that tracks dirty/newly placed
 *                  pages during the current checkpoint walk.  Like nextChkp,
 *                  the two-element array alternates by checkpoint number.
 *                  The file is consumed during post-processing (sorting and
 *                  hole-punching) and removed when the checkpoint finishes.
 *
 * DESCRIPTOR SPLIT: SHARED vs. PRIVATE
 *
 * State that must be visible to multiple backend processes (e.g. the
 * checkpointer and ordinary writer backends that also record page locations)
 * lives in SeqBufDescShared, which is stored inside the in-memory meta page
 * (BTreeMetaPage) of each B-tree.  Per-process state such as the open file
 * descriptor lives in SeqBufDescPrivate, which is stored in BTreeDescr and
 * is local to the owning backend.
 *
 * REMOVAL
 *
 * Seq buf in-memory pages are freed (returned to the page pool) as soon as
 * the buffer is finalised: either at the end of a successful checkpoint, when
 * the tree descriptor is evicted from the descriptor cache, or when the tree
 * is dropped.  The underlying on-disk files have a longer lifetime:
 *
 *   • The tmp file (type 't') is removed once the checkpoint that created it
 *     has finished post-processing (sorting, hole-punching).
 *   • The map file (type 'm') is kept until the *following* checkpoint has
 *     been completed and verified, because recovery may still need it.
 *   • The free-extents file is replaced atomically at the start of each
 *     checkpoint; the old file is removed once the new one is in place.
 *   • If a tree was not modified between two checkpoints (both dirtyFlag1
 *     and dirtyFlag2 are false), the freshly initialised seq buf files for
 *     that checkpoint are closed and removed immediately without writing any
 *     data.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __SEQ_BUF_H__
#define __SEQ_BUF_H__

typedef enum
{
	SeqBufPrevPageDone,
	SeqBufPrevPageInProgress,
	SeqBufPrevPageError
} SeqBufPrevPageState;

/*
 * Identifies a single seq buf file on disk.  Files are named after the
 * (datoid, relnode, num, type) tuple so that multiple checkpoints and
 * multiple buffer kinds can coexist without name collisions.
 *
 * 'type' is 'm' for checkpoint map files and 't' for temporary files.
 * 'num' is the checkpoint number the file belongs to.
 */
typedef struct
{
	Oid			datoid;
	Oid			relnode;
	uint32		num;
	char		type;
} SeqBufTag;

#define SeqBufTagEqual(l, r) ((l)->datoid == (r)->datoid && \
							  (l)->relnode == (r)->relnode && \
							  (l)->num == (r)->num && \
							  (l)->type == (r)->type)

/*
 * Shared state for a sequential buffer, stored in the B-tree meta page so
 * that it is accessible from every backend that participates in the
 * checkpoint.  A spinlock serialises updates to the fields below.
 *
 * pages[2]      – two in-memory OrioleDB pages used as a double-buffer.
 *                 While one page is being read from / written to by callers,
 *                 the other can be pre-fetched or flushed in the background.
 *                 Both slots must be allocated before the buffer is opened;
 *                 SEQ_BUF_SHARED_EXIST() tests whether pages[0] is valid.
 * curPageNum    – which of pages[0..1] is the "active" page (0 or 1).
 * filePageNum   – the logical page number within the on-disk file that is
 *                 currently loaded into the active in-memory page.
 * location      – byte offset within the active page where the next
 *                 read or write will occur.
 * freeBytesNum  – number of unread bytes remaining in the file (read mode).
 * evictOffset   – byte offset in the file at which this buffer was resumed
 *                 after the tree descriptor was evicted and reloaded.
 * tag           – identifies the on-disk file (datoid, relnode, num, type).
 * prevPageState – tracks whether an asynchronous write of the previous page
 *                 is still in progress, allowing the caller to wait if
 *                 necessary before reusing that page slot.
 */
typedef struct
{
	slock_t		lock;			/* spinlock protecting the fields below */
	OInMemoryBlkno pages[2];	/* pages with data */
	int			location;
	int			curPageNum;		/* current page in usage from previous two */
	uint32		filePageNum;	/* file page currently loaded */
	off_t		freeBytesNum;	/* how many unread bytes left in a file */
	off_t		evictOffset;
	SeqBufTag	tag;
	SeqBufPrevPageState prevPageState;
} SeqBufDescShared;

#define SEQ_BUF_SHARED_EXIST(shared_ptr) (OInMemoryBlknoIsValid((shared_ptr)->pages[0]))

/*
 * Per-backend private state for a sequential buffer.  The open file
 * descriptor and the write/read mode flag are not shared across processes
 * because each backend opens the file independently.
 *
 * shared – pointer into the B-tree meta page (BTreeMetaPage); must remain
 *           valid for the lifetime of this descriptor.
 * file   – the OS-level file descriptor (-1 when the file is not open).
 * tag    – copy of the tag used when this private descriptor was initialised;
 *           may differ from shared->tag if the buffer was replaced.
 * write  – true when the buffer is open for writing (checkpointer path),
 *           false when open for reading (recovery / free-extent reuse path).
 */
typedef struct
{
	SeqBufDescShared *shared;
	File		file;
	SeqBufTag	tag;
	bool		write;
} SeqBufDescPrivate;

/*
 * Snapshot of a seq buf's position saved when a tree descriptor is evicted
 * from the descriptor cache.  When the descriptor is later reloaded the
 * buffer can be reopened at exactly the position where it was left off.
 */
typedef struct
{
	off_t		offset;
	SeqBufTag	tag;
} EvictedSeqBufData;

typedef enum
{
	SeqBufReplaceSuccess,
	SeqBufReplaceAlready,
	SeqBufReplaceError
} SeqBufReplaceResult;

extern bool init_seq_buf(SeqBufDescPrivate *seqBufPrivate, SeqBufDescShared *shared,
						 SeqBufTag *tag, bool write, bool init_shared, int skip_len, EvictedSeqBufData *evicted);

extern bool seq_buf_write_u32(SeqBufDescPrivate *seqBufPrivate, uint32 offset);
extern bool seq_buf_read_u32(SeqBufDescPrivate *seqBufPrivate, uint32 *ptr);
extern bool seq_buf_write_file_extent(SeqBufDescPrivate *seqBufPrivate, FileExtent extent);
extern bool seq_buf_read_file_extent(SeqBufDescPrivate *seqBufPrivate, FileExtent *extent);

extern uint64 seq_buf_finalize(SeqBufDescPrivate *seqBufPrivate);
extern char *get_seq_buf_filename(SeqBufTag *tag);
extern uint64 seq_buf_get_offset(SeqBufDescPrivate *seqBufPrivate);
extern SeqBufReplaceResult seq_buf_try_replace(SeqBufDescPrivate *seqBufPrivate,
											   SeqBufTag *tag, pg_atomic_uint64 *size,
											   Size data_size);
extern bool seq_buf_file_exist(SeqBufTag *tag);
extern bool seq_buf_remove_file(SeqBufTag *tag);
extern void seq_buf_close_file(SeqBufDescPrivate *seqBufPrivate);

#endif							/* __SEQ_BUF_H__ */
