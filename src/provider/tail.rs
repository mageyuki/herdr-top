//! Incremental provider-session file tailing.

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Maximum provider bytes read from one file during one poll.
pub const MAX_TAIL_CHUNK_BYTES: usize = 256 * 1024;
/// Maximum unterminated provider record bytes retained in memory.
pub const MAX_TAIL_RECORD_BYTES: usize = 1024 * 1024;
/// Content-free diagnostic emitted when one record exceeds the retention cap.
pub const RECORD_TOO_LONG_ERROR: &str = "record_too_long";

/// Stable identity and observed size for a provider session file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSnapshot {
    /// Device containing the file.
    pub dev: u64,
    /// Inode within `dev`.
    pub inode: u64,
    /// Current length in bytes.
    pub size: u64,
}

impl FileSnapshot {
    fn from_file(file: &File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            dev: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
        })
    }

    fn identity(self) -> (u64, u64) {
        (self.dev, self.inode)
    }
}

/// Bytes read through one descriptor together with that descriptor's identity.
#[derive(Debug, Eq, PartialEq)]
pub struct ReadChunk {
    /// Snapshot of the descriptor that supplied `bytes`.
    pub snapshot: FileSnapshot,
    /// Bytes from the requested offset, bounded by [`MAX_TAIL_CHUNK_BYTES`].
    pub bytes: Vec<u8>,
}

/// Injectable filesystem boundary used by [`TailFile`].
pub trait ReadBoundary {
    /// Opens and stats a path without reading content.
    fn snapshot(&mut self, root: &Path, relative: &Path) -> io::Result<FileSnapshot>;

    /// Opens the same contained path and reads one bounded chunk from `offset`.
    fn read_from(&mut self, root: &Path, relative: &Path, offset: u64) -> io::Result<ReadChunk>;
}

/// Descriptor-relative, no-follow implementation of [`ReadBoundary`].
#[derive(Debug, Default)]
pub struct FsReadBoundary;

impl ReadBoundary for FsReadBoundary {
    fn snapshot(&mut self, root: &Path, relative: &Path) -> io::Result<FileSnapshot> {
        let file = super::open_contained_regular_file(root, relative)?;
        FileSnapshot::from_file(&file)
    }

    fn read_from(&mut self, root: &Path, relative: &Path, offset: u64) -> io::Result<ReadChunk> {
        let mut file = super::open_contained_regular_file(root, relative)?;
        let snapshot = FileSnapshot::from_file(&file)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::with_capacity(MAX_TAIL_CHUNK_BYTES);
        file.take(MAX_TAIL_CHUNK_BYTES as u64)
            .read_to_end(&mut bytes)?;
        Ok(ReadChunk { snapshot, bytes })
    }
}

/// Files treated as pre-existing by one run-scoped or fallback root baseline.
#[derive(Clone, Debug, Default)]
pub struct FirstSeenBaseline {
    existing: HashSet<PathBuf>,
}

impl FirstSeenBaseline {
    /// Creates a baseline from paths captured at the root's baseline point.
    pub fn from_existing(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            existing: paths.into_iter().collect(),
        }
    }

    /// Records one file that existed at the root's baseline point.
    pub fn record(&mut self, path: PathBuf) {
        self.existing.insert(path);
    }

    /// Reports whether a path existed at the root's baseline point.
    #[must_use]
    pub fn contained(&self, root: &Path, relative: &Path) -> bool {
        self.existing.contains(&root.join(relative))
    }

    pub(super) fn retain_existing(&mut self, root: &Path, seen: &HashSet<PathBuf>) {
        self.existing
            .retain(|path| !path.starts_with(root) || seen.contains(path));
    }
}

/// One complete provider record or content-free tail diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailRecord {
    /// Byte offset at which this record starts.
    pub offset: u64,
    /// File generation that supplied this record.
    pub generation: u64,
    /// Record bytes without the terminating newline.
    pub bytes: Vec<u8>,
    /// Content-free tail diagnostic in place of record bytes.
    pub error_code: Option<&'static str>,
}

impl TailRecord {
    /// Creates one parsed data record.
    #[must_use]
    pub fn data(offset: u64, generation: u64, bytes: Vec<u8>) -> Self {
        Self {
            offset,
            generation,
            bytes,
            error_code: None,
        }
    }

    fn malformed(offset: u64, generation: u64, error_code: &'static str) -> Self {
        Self {
            offset,
            generation,
            bytes: Vec::new(),
            error_code: Some(error_code),
        }
    }
}

/// Incremental state for one provider session file.
#[derive(Debug)]
pub struct TailFile {
    root: PathBuf,
    relative: PathBuf,
    offset: u64,
    identity: (u64, u64),
    last_size: u64,
    generation: u64,
    partial: Vec<u8>,
    partial_offset: u64,
    discarding: bool,
    read_calls: u64,
    bytes_read: u64,
}

impl TailFile {
    /// Opens one tail at EOF for a baseline file, or at byte zero for a later file.
    pub fn open(
        root: &Path,
        relative: &Path,
        baseline: &FirstSeenBaseline,
        starting_generation: u64,
        boundary: &mut impl ReadBoundary,
    ) -> io::Result<Self> {
        let snapshot = boundary.snapshot(root, relative)?;
        let offset = if baseline.contained(root, relative) {
            snapshot.size
        } else {
            0
        };
        Ok(Self {
            root: root.to_path_buf(),
            relative: relative.to_path_buf(),
            offset,
            identity: snapshot.identity(),
            last_size: snapshot.size,
            generation: starting_generation,
            partial: Vec::new(),
            partial_offset: offset,
            discarding: false,
            read_calls: 0,
            bytes_read: 0,
        })
    }

    /// Stats the file and returns only newly completed records.
    pub fn poll(&mut self, boundary: &mut impl ReadBoundary) -> io::Result<Vec<TailRecord>> {
        let snapshot = boundary.snapshot(&self.root, &self.relative)?;
        let rotated = snapshot.identity() != self.identity;
        let truncated = snapshot.size < self.last_size || snapshot.size < self.offset;
        if rotated || truncated {
            self.generation = self.generation.saturating_add(1);
            self.offset = 0;
            self.partial.clear();
            self.partial_offset = 0;
            self.discarding = false;
            self.identity = snapshot.identity();
        }
        self.last_size = snapshot.size;

        if snapshot.size <= self.offset {
            return Ok(Vec::new());
        }

        let requested_offset = self.offset;
        let chunk = boundary.read_from(&self.root, &self.relative, requested_offset)?;
        self.read_calls = self.read_calls.saturating_add(1);
        self.bytes_read = self.bytes_read.saturating_add(chunk.bytes.len() as u64);

        if chunk.snapshot.identity() != self.identity {
            self.generation = self.generation.saturating_add(1);
            self.offset = 0;
            self.partial.clear();
            self.partial_offset = 0;
            self.discarding = false;
            self.identity = chunk.snapshot.identity();
            self.last_size = chunk.snapshot.size;
            let replacement = boundary.read_from(&self.root, &self.relative, 0)?;
            self.read_calls = self.read_calls.saturating_add(1);
            self.bytes_read = self
                .bytes_read
                .saturating_add(replacement.bytes.len() as u64);
            if replacement.snapshot.identity() != self.identity {
                return Ok(Vec::new());
            }
            return Ok(self.accept_bytes(0, replacement.bytes));
        }

        self.last_size = chunk.snapshot.size;
        Ok(self.accept_bytes(requested_offset, chunk.bytes))
    }

    fn accept_bytes(&mut self, start: u64, bytes: Vec<u8>) -> Vec<TailRecord> {
        if self.partial.is_empty() && !self.discarding {
            self.partial_offset = start;
        }
        self.offset = start.saturating_add(bytes.len() as u64);
        let mut records = Vec::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            if self.discarding {
                if let Some(newline) = bytes[cursor..].iter().position(|byte| *byte == b'\n') {
                    cursor += newline + 1;
                    self.partial_offset = start.saturating_add(cursor as u64);
                    self.discarding = false;
                    continue;
                }
                break;
            }

            let remaining = &bytes[cursor..];
            match remaining.iter().position(|byte| *byte == b'\n') {
                Some(newline) => {
                    if self.partial.len().saturating_add(newline) > MAX_TAIL_RECORD_BYTES {
                        records.push(TailRecord::malformed(
                            self.partial_offset,
                            self.generation,
                            RECORD_TOO_LONG_ERROR,
                        ));
                        self.partial.clear();
                    } else {
                        self.partial.extend_from_slice(&remaining[..newline]);
                        if self.partial.last() == Some(&b'\r') {
                            self.partial.pop();
                        }
                        records.push(TailRecord::data(
                            self.partial_offset,
                            self.generation,
                            std::mem::take(&mut self.partial),
                        ));
                    }
                    cursor += newline + 1;
                    self.partial_offset = start.saturating_add(cursor as u64);
                }
                None => {
                    if self.partial.len().saturating_add(remaining.len()) > MAX_TAIL_RECORD_BYTES {
                        records.push(TailRecord::malformed(
                            self.partial_offset,
                            self.generation,
                            RECORD_TOO_LONG_ERROR,
                        ));
                        self.partial.clear();
                        self.discarding = true;
                    } else {
                        self.partial.extend_from_slice(remaining);
                    }
                    break;
                }
            }
        }
        records
    }

    /// Returns the next unread byte offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the reopen generation, incremented on rotation or truncation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the number of content reads performed by this tail.
    #[must_use]
    pub const fn read_calls(&self) -> u64 {
        self.read_calls
    }

    /// Returns the number of content bytes read by this tail.
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub(crate) fn absolute_path(&self) -> PathBuf {
        self.root.join(&self.relative)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs::{self, OpenOptions};
    use std::io::Write;

    use super::*;

    // I2a tail test list, written before implementation:
    // - appended bytes advance the offset exactly once
    // - inode rotation bumps generation and restarts at byte zero
    // - truncation bumps generation and restarts at byte zero
    // - files present at run start begin at EOF
    // - files present at run start but targeted late still begin at EOF
    // - files created after run start begin at byte zero even when targeted late
    // - incomplete records remain buffered until a newline arrives
    // - unchanged stat-only rescans perform zero read calls and zero byte reads

    fn append(path: &Path, bytes: &[u8]) {
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    #[derive(Debug)]
    struct ScriptedBoundary {
        snapshots: VecDeque<FileSnapshot>,
        reads: VecDeque<ReadChunk>,
    }

    impl ScriptedBoundary {
        fn new(snapshots: impl IntoIterator<Item = FileSnapshot>, reads: Vec<ReadChunk>) -> Self {
            Self {
                snapshots: snapshots.into_iter().collect(),
                reads: reads.into(),
            }
        }
    }

    impl ReadBoundary for ScriptedBoundary {
        fn snapshot(&mut self, _root: &Path, _relative: &Path) -> io::Result<FileSnapshot> {
            self.snapshots
                .pop_front()
                .ok_or_else(|| io::Error::other("unexpected snapshot"))
        }

        fn read_from(
            &mut self,
            _root: &Path,
            _relative: &Path,
            _offset: u64,
        ) -> io::Result<ReadChunk> {
            self.reads
                .pop_front()
                .ok_or_else(|| io::Error::other("unexpected read"))
        }
    }

    #[test]
    fn appended_bytes_advance_offset_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("session.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        append(&path, b"one\n");
        assert_eq!(
            tail.poll(&mut boundary).unwrap(),
            vec![TailRecord::data(0, 0, b"one".to_vec())]
        );
        assert_eq!(tail.offset(), 4);
        assert_eq!(tail.poll(&mut boundary).unwrap(), Vec::<TailRecord>::new());
        assert_eq!(tail.read_calls(), 1);
        assert_eq!(tail.bytes_read(), 4);
    }

    #[test]
    fn inode_rotation_bumps_generation_and_restarts_at_zero() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("session.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"old\n").unwrap();
        let baseline = FirstSeenBaseline::from_existing([path.clone()]);
        let mut boundary = FsReadBoundary;
        let mut tail =
            TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        fs::rename(&path, directory.path().join("old.jsonl")).unwrap();
        fs::write(&path, b"new\n").unwrap();

        assert_eq!(tail.poll(&mut boundary).unwrap()[0].offset, 0);
        assert_eq!(tail.generation(), 1);
        assert_eq!(tail.offset(), 4);
    }

    #[test]
    fn rotation_during_read_stamps_replacement_records_with_bumped_generation() {
        let original = FileSnapshot {
            dev: 1,
            inode: 10,
            size: 0,
        };
        let replacement = FileSnapshot {
            dev: 1,
            inode: 20,
            size: 4,
        };
        let mut boundary = ScriptedBoundary::new(
            [
                original,
                FileSnapshot {
                    size: 4,
                    ..original
                },
            ],
            vec![
                ReadChunk {
                    snapshot: replacement,
                    bytes: b"new\n".to_vec(),
                },
                ReadChunk {
                    snapshot: replacement,
                    bytes: b"new\n".to_vec(),
                },
            ],
        );
        let mut tail = TailFile::open(
            Path::new("/unused"),
            Path::new("session.jsonl"),
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        let records = tail.poll(&mut boundary).unwrap();

        assert_eq!(tail.generation(), 1);
        assert_eq!(records, [TailRecord::data(0, 1, b"new".to_vec())]);
    }

    #[test]
    fn second_rotation_during_replacement_read_defers_until_next_poll() {
        let original = FileSnapshot {
            dev: 1,
            inode: 10,
            size: 0,
        };
        let first_replacement = FileSnapshot {
            dev: 1,
            inode: 20,
            size: 6,
        };
        let second_replacement = FileSnapshot {
            dev: 1,
            inode: 30,
            size: 6,
        };
        let mut boundary = ScriptedBoundary::new(
            [
                original,
                FileSnapshot {
                    size: 6,
                    ..original
                },
                second_replacement,
            ],
            vec![
                ReadChunk {
                    snapshot: first_replacement,
                    bytes: b"first\n".to_vec(),
                },
                ReadChunk {
                    snapshot: second_replacement,
                    bytes: b"third\n".to_vec(),
                },
                ReadChunk {
                    snapshot: second_replacement,
                    bytes: b"third\n".to_vec(),
                },
            ],
        );
        let mut tail = TailFile::open(
            Path::new("/unused"),
            Path::new("session.jsonl"),
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        assert!(tail.poll(&mut boundary).unwrap().is_empty());
        assert_eq!(tail.generation(), 1);
        let records = tail.poll(&mut boundary).unwrap();
        assert_eq!(tail.generation(), 2);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].generation, 2);
    }

    #[test]
    fn truncation_bumps_generation_and_restarts_at_zero() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("session.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"old-record\n").unwrap();
        let baseline = FirstSeenBaseline::from_existing([path.clone()]);
        let mut boundary = FsReadBoundary;
        let mut tail =
            TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        fs::write(&path, b"n\n").unwrap();

        assert_eq!(tail.poll(&mut boundary).unwrap()[0].bytes, b"n");
        assert_eq!(tail.generation(), 1);
        assert_eq!(tail.offset(), 2);
    }

    #[test]
    fn preexisting_file_starts_at_eof() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("session.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"history\n").unwrap();
        let baseline = FirstSeenBaseline::from_existing([path]);
        let mut boundary = FsReadBoundary;
        let mut tail =
            TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        assert_eq!(tail.offset(), 8);
        assert!(tail.poll(&mut boundary).unwrap().is_empty());
    }

    #[test]
    fn late_targeted_preexisting_file_starts_at_eof() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("late.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"before-target\n").unwrap();
        let baseline = FirstSeenBaseline::from_existing([path]);
        append(&directory.path().join(relative), b"still-before-target\n");
        let mut boundary = FsReadBoundary;

        let tail = TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        assert_eq!(tail.offset(), 34);
    }

    #[test]
    fn file_created_after_start_reads_zero_even_when_targeted_late() {
        let directory = tempfile::tempdir().unwrap();
        let baseline = FirstSeenBaseline::default();
        let relative = Path::new("created-late.jsonl");
        fs::write(directory.path().join(relative), b"first\n").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail =
            TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        assert_eq!(tail.offset(), 0);
        assert_eq!(tail.poll(&mut boundary).unwrap()[0].bytes, b"first");
    }

    #[test]
    fn incomplete_record_is_buffered_until_newline() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("partial.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        append(&path, b"part");
        assert!(tail.poll(&mut boundary).unwrap().is_empty());
        append(&path, b"ial\nnext\n");

        assert_eq!(
            tail.poll(&mut boundary).unwrap(),
            vec![
                TailRecord::data(0, 0, b"partial".to_vec()),
                TailRecord::data(8, 0, b"next".to_vec())
            ]
        );
    }

    #[test]
    fn oversized_record_spanning_three_chunks_emits_once_then_valid_record_lands() {
        const TEST_CHUNK_BYTES: usize = 256 * 1024;
        const TEST_RECORD_BYTES: usize = 1024 * 1024;

        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("oversized.jsonl");
        let path = directory.path().join(relative);
        let mut bytes = vec![b'x'; TEST_RECORD_BYTES + TEST_CHUNK_BYTES + 1];
        bytes.extend_from_slice(b"\nvalid\n");
        fs::write(&path, bytes).unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        let mut records = Vec::new();
        for _ in 0..8 {
            records.extend(tail.poll(&mut boundary).unwrap());
            if records.iter().any(|record| record.bytes == b"valid") {
                break;
            }
        }

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].generation, 0);
        assert_eq!(records[0].error_code, Some(RECORD_TOO_LONG_ERROR));
        assert!(
            records[0].bytes.is_empty(),
            "oversized content was retained"
        );
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].bytes, b"valid");
        assert_eq!(records[1].error_code, None);
    }

    #[test]
    fn rotation_while_discarding_resets_state_and_parses_new_head() {
        const TEST_RECORD_BYTES: usize = 1024 * 1024;

        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("discarding.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, vec![b'x'; TEST_RECORD_BYTES + 1]).unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        for _ in 0..5 {
            let _ = tail.poll(&mut boundary).unwrap();
        }
        assert!(
            tail.discarding,
            "oversized partial did not enter discard mode"
        );

        fs::rename(&path, directory.path().join("old.jsonl")).unwrap();
        fs::write(&path, b"new-head\n").unwrap();
        let records = tail.poll(&mut boundary).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].bytes, b"new-head");
    }

    #[test]
    fn multi_chunk_append_advances_offsets_monotonically_across_polls() {
        const TEST_CHUNK_BYTES: usize = 256 * 1024;

        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("chunks.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();
        append(&path, &vec![b'a'; TEST_CHUNK_BYTES * 2 + 17]);

        let mut offsets = Vec::new();
        for _ in 0..3 {
            let _ = tail.poll(&mut boundary).unwrap();
            offsets.push(tail.offset());
        }

        assert_eq!(
            offsets,
            [
                TEST_CHUNK_BYTES as u64,
                (TEST_CHUNK_BYTES * 2) as u64,
                (TEST_CHUNK_BYTES * 2 + 17) as u64
            ]
        );
    }

    #[test]
    fn unchanged_stat_only_rescan_performs_zero_reads() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("idle.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"history\n").unwrap();
        let baseline = FirstSeenBaseline::from_existing([path]);
        let mut boundary = FsReadBoundary;
        let mut tail =
            TailFile::open(directory.path(), relative, &baseline, 0, &mut boundary).unwrap();

        for _ in 0..4 {
            assert!(tail.poll(&mut boundary).unwrap().is_empty());
        }

        assert_eq!(tail.read_calls(), 0);
        assert_eq!(tail.bytes_read(), 0);
    }
}
