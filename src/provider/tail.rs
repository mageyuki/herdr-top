//! Incremental provider-session file tailing.

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

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
    /// Bytes from the requested offset through the descriptor's EOF.
    pub bytes: Vec<u8>,
}

/// Injectable filesystem boundary used by [`TailFile`].
pub trait ReadBoundary {
    /// Opens and stats a path without reading content.
    fn snapshot(&mut self, root: &Path, relative: &Path) -> io::Result<FileSnapshot>;

    /// Opens the same contained path and reads from `offset` through EOF.
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
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(ReadChunk { snapshot, bytes })
    }
}

/// Files known to exist at the beginning of one collector run.
#[derive(Clone, Debug, Default)]
pub struct FirstSeenBaseline {
    existing: HashSet<PathBuf>,
}

impl FirstSeenBaseline {
    /// Creates a baseline from paths discovered at collector start.
    pub fn from_existing(paths: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            existing: paths.into_iter().collect(),
        }
    }

    /// Records one file that existed at collector start.
    pub fn record(&mut self, path: PathBuf) {
        self.existing.insert(path);
    }

    /// Reports whether a path existed at collector start.
    #[must_use]
    pub fn contained(&self, root: &Path, relative: &Path) -> bool {
        self.existing.contains(&root.join(relative))
    }
}

/// One complete newline-terminated provider record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailRecord {
    /// Byte offset at which this record starts.
    pub offset: u64,
    /// Record bytes without the terminating newline.
    pub bytes: Vec<u8>,
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
    read_calls: u64,
    bytes_read: u64,
}

impl TailFile {
    /// Opens one tail at EOF for a run-start file, or at byte zero for a later file.
    pub fn open(
        root: &Path,
        relative: &Path,
        baseline: &FirstSeenBaseline,
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
            generation: 0,
            partial: Vec::new(),
            partial_offset: offset,
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
            self.identity = chunk.snapshot.identity();
            self.last_size = chunk.snapshot.size;
            let replacement = boundary.read_from(&self.root, &self.relative, 0)?;
            self.read_calls = self.read_calls.saturating_add(1);
            self.bytes_read = self
                .bytes_read
                .saturating_add(replacement.bytes.len() as u64);
            return Ok(self.accept_bytes(0, replacement.bytes));
        }

        self.last_size = chunk.snapshot.size;
        Ok(self.accept_bytes(requested_offset, chunk.bytes))
    }

    fn accept_bytes(&mut self, start: u64, bytes: Vec<u8>) -> Vec<TailRecord> {
        if self.partial.is_empty() {
            self.partial_offset = start;
        }
        self.offset = start.saturating_add(bytes.len() as u64);
        self.partial.extend(bytes);

        let mut records = Vec::new();
        while let Some(newline) = self.partial.iter().position(|byte| *byte == b'\n') {
            let mut record = self.partial.drain(..=newline).collect::<Vec<_>>();
            record.pop();
            if record.last() == Some(&b'\r') {
                record.pop();
            }
            let record_offset = self.partial_offset;
            self.partial_offset = self
                .partial_offset
                .saturating_add((newline as u64).saturating_add(1));
            records.push(TailRecord {
                offset: record_offset,
                bytes: record,
            });
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
}

#[cfg(test)]
mod tests {
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
            &mut boundary,
        )
        .unwrap();

        append(&path, b"one\n");
        assert_eq!(
            tail.poll(&mut boundary).unwrap(),
            vec![TailRecord {
                offset: 0,
                bytes: b"one".to_vec()
            }]
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
            TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

        fs::rename(&path, directory.path().join("old.jsonl")).unwrap();
        fs::write(&path, b"new\n").unwrap();

        assert_eq!(tail.poll(&mut boundary).unwrap()[0].offset, 0);
        assert_eq!(tail.generation(), 1);
        assert_eq!(tail.offset(), 4);
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
            TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

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
            TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

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

        let tail = TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

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
            TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

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
            &mut boundary,
        )
        .unwrap();

        append(&path, b"part");
        assert!(tail.poll(&mut boundary).unwrap().is_empty());
        append(&path, b"ial\nnext\n");

        assert_eq!(
            tail.poll(&mut boundary).unwrap(),
            vec![
                TailRecord {
                    offset: 0,
                    bytes: b"partial".to_vec()
                },
                TailRecord {
                    offset: 8,
                    bytes: b"next".to_vec()
                }
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
            TailFile::open(directory.path(), relative, &baseline, &mut boundary).unwrap();

        for _ in 0..4 {
            assert!(tail.poll(&mut boundary).unwrap().is_empty());
        }

        assert_eq!(tail.read_calls(), 0);
        assert_eq!(tail.bytes_read(), 0);
    }
}
