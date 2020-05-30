// #![cfg_attr(not(any(test, feature = "std")), no_std)] // aoslab todo: add this restriction after debugging
// Current working on this LFS file

extern crate alloc;
#[macro_use]
extern crate log;

use alloc::{
    string::String,
    vec,
    collections::BTreeMap,
    sync::{Arc, Weak},
    vec::Vec,
};
use core::any::Any;
use core::mem; // for size_of
use core::fmt::{Debug, Error, Formatter};
// MaybeUninit is used, to notify compiler not to transform inner struct since it may not be initilized and causes undefined behavior.
use core::mem::MaybeUninit;

use spin::RwLock;

use rcore_fs::dev::Device;
use rcore_fs::dirty::Dirty;
use rcore_fs::util::*;
use rcore_fs::vfs::{self, FileSystem, FsError, MMapArea, INode, Timespec};

pub use self::structs::*;

mod structs;

trait DeviceExt: Device {
    fn read_block(&self, id: BlockId, offset: usize, buf: &mut [u8]) -> vfs::Result<()> {
        // debug!("read block offset {} len {}", id * BLKSIZE + offset, buf.len());
        debug_assert!(offset + buf.len() <= BLKSIZE);
        info!("offset\t{}\tlen\t{}\t0", id * BLKSIZE + offset, buf.len());
        match self.read_at(id * BLKSIZE + offset, buf) {
            Ok(len) if len == buf.len() => Ok(()),
            _ => panic!("cannot read block {} offset {} from device", id, offset),
        }
    }
    fn write_block(&self, id: BlockId, offset: usize, buf: &[u8]) -> vfs::Result<()> {
        debug_assert!(offset + buf.len() <= BLKSIZE);
        info!("offset\t{}\tlen\t{}\t1", id * BLKSIZE + offset, buf.len());
        match self.write_at(id * BLKSIZE + offset, buf) {
            Ok(len) if len == buf.len() => Ok(()),
            _ => panic!("cannot write block {} offset {} to device", id, offset),
        }
    }
    /// Load struct `T` from given block in device
    fn load_struct<T: AsBuf>(&self, id: BlockId) -> vfs::Result<T> {
        let mut s: T = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_block(id, 0, s.as_buf_mut())?;
        Ok(s)
    }
}

impl DeviceExt for dyn Device {}

/// INode for LFS
pub struct INodeImpl {
    /// INode number (usize type)
    id: INodeId,
    blk_id: BlockId,
    /// On-disk INode instance
    disk_inode: RwLock<Dirty<DiskINode>>,
    /// Reference to LFS, used by almost all operations
    fs: Arc<LogFileSystem>,
    /// Char/block device id (major, minor)
    /// e.g. crw-rw-rw- 1 root wheel 3, 2 May 13 16:40 /dev/null
    device_inode_id: usize,
}

impl Debug for INodeImpl {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "INode {{ id: {}, disk: {:?} }}",
            self.id, self.disk_inode
        )
    }
}

impl INodeImpl {
    /// Map file id to disk block id
    fn get_disk_block_id(&self, file_id: BlockId) -> vfs::Result<BlockId> {
        let disk_inode = self.disk_inode.read();
        match file_id {
            id if id >= disk_inode.blocks as BlockId => Err(FsError::InvalidParam),
            id if id < MAX_NBLOCK_DIRECT => {
                // debug!("get disk id {} -> {}", id, disk_inode.direct[id]);
                Ok(disk_inode.direct[id] as BlockId)
            }
            id if id < MAX_NBLOCK_INDIRECT => {
                let mut disk_block_id: u32 = 0;
                self.fs.device.read_block(
                    disk_inode.indirect as usize,
                    ENTRY_SIZE * (id - NDIRECT),
                    disk_block_id.as_buf_mut(),
                )?;
                // debug!("get disk id {} -> {}", id, disk_block_id);
                Ok(disk_block_id as BlockId)
            }
            _ => unimplemented!("double indirect blocks is not supported"),
        }
    }
    fn set_disk_block_id(&self, file_id: BlockId, disk_block_id: BlockId) -> vfs::Result<()> {
        // debug!("inode blocks {}", self.disk_inode.read().blocks);
        match file_id {
            id if id >= self.disk_inode.read().blocks as BlockId => Err(FsError::InvalidParam),
            id if id < MAX_NBLOCK_DIRECT => {
                debug!("set disk id {} -> {}", id, disk_block_id);
                self.disk_inode.write().direct[id] = disk_block_id as u32;
                Ok(())
            }
            id if id < MAX_NBLOCK_INDIRECT => {
                let disk_block_id = disk_block_id as u32;
                self.fs.device.write_block(
                    self.disk_inode.read().indirect as usize,
                    ENTRY_SIZE * (id - NDIRECT),
                    disk_block_id.as_buf(),
                )?;
                // debug!("set disk id {} -> {}", id, disk_block_id);
                Ok(())
            }
            _ => unimplemented!("double indirect blocks is not supported"),
        }
    }
    /// Only for Dir
    fn get_file_inode_and_entry_id(&self, name: &str) -> Option<(INodeId, usize)> {
        let inode = self.disk_inode.read();
        // debug!("D {}", self.id);
        // for i in 0..(inode.size as usize / DIRENT_SIZE) {
            // let dir_i = self.read_direntry(i as usize).unwrap();
            // debug!("D i {} {:?}", i, dir_i.name);
        // }
        (0..inode.size as usize / DIRENT_SIZE)
            .map(|i| (self.read_direntry(i as usize).unwrap(), i))
            .find(|(entry, _)| entry.name.as_ref() == name)
            .map(|(entry, id)| (entry.id as INodeId, id as usize))
    }
    fn get_file_inode_id(&self, name: &str) -> Option<INodeId> {
        self.get_file_inode_and_entry_id(name)
            .map(|(inode_id, _)| inode_id)
    }
    /// Init dir content. Insert 2 init entries.
    /// This do not init nlinks, please modify the nlinks in the invoker.
    fn init_direntry(&self, parent: INodeId) -> vfs::Result<()> {
        // Insert entries: '.' '..'
        self._resize(DIRENT_SIZE * 2)?;
        self.write_direntry(
            0,
            &DiskEntry {
                id: self.id as u32,
                name: Str256::from("."),
            },
        )?;
        self.write_direntry(
            1,
            &DiskEntry {
                id: parent as u32,
                name: Str256::from(".."),
            },
        )?;
        Ok(())
    }
    fn read_direntry(&self, id: usize) -> vfs::Result<DiskEntry> {
        let mut direntry: DiskEntry = unsafe { MaybeUninit::uninit().assume_init() };
        self._read_at(DIRENT_SIZE * id, direntry.as_buf_mut())?;
        Ok(direntry)
    }
    fn write_direntry(&self, id: usize, direntry: &DiskEntry) -> vfs::Result<()> {
        self._write_at(DIRENT_SIZE * id, direntry.as_buf())?;
        Ok(())
    }
    fn append_direntry(&self, direntry: &DiskEntry) -> vfs::Result<()> {
        let size = self.disk_inode.read().size as usize;
        let dirent_count = size / DIRENT_SIZE;
        self._resize(size + DIRENT_SIZE)?;
        self.write_direntry(dirent_count, direntry)?;
        // debug!("D write directory to {:?}", direntry.name);
        Ok(())
    }
    /// remove a direntry in middle of file and insert the last one here, useful for direntry remove
    /// should be only used in unlink
    fn remove_direntry(&self, id: usize) -> vfs::Result<()> {
        let size = self.disk_inode.read().size as usize;
        let dirent_count = size / DIRENT_SIZE;
        debug_assert!(id < dirent_count);
        println!("remove_dentry id {} count {}", id, dirent_count);
        let last_dirent = self.read_direntry(dirent_count - 1)?;
        self.write_direntry(id, &last_dirent)?;
        self.disk_inode.write().size -= DIRENT_SIZE as u32;
        Ok(())
    }
    /// Resize content size, no matter what type it is.
    fn _resize(&self, len: usize) -> vfs::Result<()> {
        if len > MAX_FILE_SIZE {
            return Err(FsError::InvalidParam);
        }
        let blocks = ((len + BLKSIZE - 1) / BLKSIZE) as u32;
        if blocks > MAX_NBLOCK_DOUBLE_INDIRECT as u32 {
            return Err(FsError::InvalidParam);
        }
        use core::cmp::Ordering;
        let mut disk_inode = self.disk_inode.write();
        let old_blocks = disk_inode.blocks;
        debug!("_resize: id {} dirty {} stale {}", self.id, disk_inode.dirty(), disk_inode.stale());
        match blocks.cmp(&old_blocks) {
            Ordering::Equal => {
                disk_inode.size = len as u32;
            }
            Ordering::Greater => {
                disk_inode.blocks = blocks;
                // debug!("disk inode old_blocks {} blocks {}", old_blocks, blocks);
                if blocks >= MAX_NBLOCK_INDIRECT as u32 {
                    unimplemented!("not support double indirect");
                }
                if old_blocks < MAX_NBLOCK_DIRECT as u32 && blocks >= MAX_NBLOCK_DIRECT as u32 {
                    disk_inode.indirect = self.fs.alloc_block().expect("no space") as u32;
                    self.fs._record_block_summary(self.id, disk_inode.indirect as usize, -1); // -1 indicates it is a special indirect ptr block
                }
                drop(disk_inode);
                // allocate extra blocks
                for i in old_blocks..blocks {
                    let disk_block_id = self.fs.alloc_block().expect("no space");
                    self.fs._record_block_summary(self.id, disk_block_id, i as isize);
                    // debug!("in_resize disk inode blocks i {} {}", i, disk_block_id);
                    self.set_disk_block_id(i as usize, disk_block_id)?;
                }
                // debug!("set_disk_block_id finish");
                // clean up
                let mut disk_inode = self.disk_inode.write();
                disk_inode.size = len as u32;
                // debug!("disk inode size {}", len);
                drop(disk_inode);
            }
            Ordering::Less => {
                // Not support space reduction!
            }
        }
        // debug!("resize finish");
        Ok(())
    }
    // Note: the _\w*_at method always return begin>size?0:begin<end?0:(min(size,end)-begin) when success
    /// Read/Write content, no matter what type it is
    fn _io_at<F>(&self, begin: usize, end: usize, mut f: F, iswrite: bool) -> vfs::Result<usize>
    where
        F: FnMut(&Arc<dyn Device>, &BlockRange, usize) -> vfs::Result<()>,
    {
        let size = self.disk_inode.read().size as usize;
        let iter = BlockIter {
            begin: size.min(begin),
            end: size.min(end),
            block_size_log2: BLKSIZE_LOG2,
        };

        if iswrite && self.disk_inode.read().dirty() && (self.disk_inode.read().stale()) {
            // back up stale data
            let mut buf: [u8; BLKSIZE] = unsafe { MaybeUninit::uninit().assume_init() };
            let begin_offset_align = begin/BLKSIZE * BLKSIZE;
            let begin_entryid = begin/BLKSIZE;
            let end_entryid = (end + BLKSIZE - 1) / BLKSIZE;
            let end_offset_align = end_entryid * BLKSIZE;
            // debug!("DDD offbegin {}, offbegin_align {}, offend {}, offend_align {}, dirty {}, stale {}", begin, begin_offset_align, end, end_offset_align, self.disk_inode.read().dirty(), self.disk_inode.read().stale());
            let old_begin_blkid = self.get_disk_block_id(begin_entryid)?;

            &self.fs.device.read_block(
                old_begin_blkid,
                0,
                &mut buf[0..begin - begin_offset_align],
            );

            for i in begin_entryid..end_entryid {
                let disk_block_id = self.fs.alloc_block().expect("no space");
                self.fs._record_block_summary(self.id, disk_block_id, i as isize);
                self.set_disk_block_id(i as usize, disk_block_id)?;
            }

            if begin_offset_align < begin {
                let new_begin_blkid = self.get_disk_block_id(begin_entryid)?;
                &self.fs.device.write_block(
                    new_begin_blkid,
                    0,
                    &mut buf[0..begin - begin_offset_align],
                );
            }

            if end < end_offset_align {
                let new_end_blkid = self.get_disk_block_id(end_entryid - 1)?;
                &self.fs.device.write_block(
                    new_end_blkid,
                    end,
                    &mut buf[end..end_offset_align],
                );
            }
        }

        let mut buf_offset = 0usize;
        for mut range in iter {
            range.block = self.get_disk_block_id(range.block)?;
            if iswrite {
                // debug!("write to blk {} offset {}", range.block, range.begin);
            }
            f(&self.fs.device, &range, buf_offset)?;
            buf_offset += range.len();
        }
        Ok(buf_offset)
    }
    /// Read content, no matter what type it is
    fn _read_at(&self, offset: usize, buf: &mut [u8]) -> vfs::Result<usize> {
        self._io_at(offset, offset + buf.len(), |device, range, offset| {
            device.read_block(
                range.block,
                range.begin,
                &mut buf[offset..offset + range.len()],
            )
        }, false)
    }
    /// Write content, no matter what type it is
    fn _write_at(&self, offset: usize, buf: &[u8]) -> vfs::Result<usize> {
        self.disk_inode.write().turn_dirty();
        let res = self._io_at(offset, offset + buf.len(), |device, range, offset| {
            device.write_block(range.block, range.begin, &buf[offset..offset + range.len()])
        }, true);
        self.disk_inode.write().clear_stale(); // aoslab DEBUG: comment it to allow in-place metadata writing or not.
        res
    }
    fn nlinks_inc(&self) {
        self.disk_inode.write().nlinks += 1;
    }
    fn nlinks_dec(&self) {
        let mut disk_inode = self.disk_inode.write();
        assert!(disk_inode.nlinks > 0);
        disk_inode.nlinks -= 1;
    }
    fn _free_all_block(&self) -> vfs::Result<()> {
        let old_blocks = self.disk_inode.read().blocks;
        for i in 0..old_blocks {
            let disk_block_id = self.get_disk_block_id(i as usize)?;
            self.fs.free_block(disk_block_id);
        }
        let mut disk_inode = self.disk_inode.write();
        // free indirect block if needed
        if old_blocks >= MAX_NBLOCK_DIRECT as u32
        {
            self.fs.free_block(disk_inode.indirect as usize);
            disk_inode.indirect = 0;
        }
        disk_inode.blocks = 0;
        disk_inode.size = 0 as u32;
        drop(disk_inode);
        self.sync_data()?;
        Ok(())
    }
}

impl vfs::INode for INodeImpl {
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> vfs::Result<usize> {
        let inode = self.disk_inode.read();
        match inode.type_ {
            FileType::File => self._read_at(offset, buf),
            FileType::SymLink => self._read_at(offset, buf),
            FileType::CharDevice => {
                let device_inodes = self.fs.device_inodes.read();
                let device_inode = device_inodes.get(&self.device_inode_id);
                match device_inode {
                    Some(device) => device.read_at(offset, buf),
                    None => Err(FsError::DeviceError),
                }
            }
            _ => Err(FsError::NotFile),
        }
    }
    fn write_at(&self, offset: usize, buf: &[u8]) -> vfs::Result<usize> {
        let DiskINode { type_, size, .. } = **self.disk_inode.read();
        match type_ {
            FileType::File | FileType::SymLink => {
                let end_offset = offset + buf.len();
                if (size as usize) < end_offset {
                    debug!("Need resize for alignment");
                    self._resize(end_offset)?;
                }
                self._write_at(offset, buf)
            }
            FileType::CharDevice => {
                let device_inodes = self.fs.device_inodes.write();
                let device_inode = device_inodes.get(&self.device_inode_id);
                match device_inode {
                    Some(device) => device.write_at(offset, buf),
                    None => Err(FsError::DeviceError),
                }
            }
            _ => Err(FsError::NotFile),
        }
    }
    fn poll(&self) -> vfs::Result<vfs::PollStatus> {
        Err(FsError::NotSupported)
    }
    /// the size returned here is logical size(entry num for directory), not the disk space used.
    fn metadata(&self) -> vfs::Result<vfs::Metadata> {
        let disk_inode = self.disk_inode.read();
        // debug!("name {} type {:?}", self.id, disk_inode.type_);
        Ok(vfs::Metadata {
            dev: 0,
            inode: self.id,
            size: match disk_inode.type_ {
                FileType::File | FileType::SymLink => disk_inode.size as usize,
                FileType::Dir => disk_inode.size as usize,
                FileType::CharDevice => 0,
                FileType::BlockDevice => 0,
                _ => panic!("Unknown file type"),
            },
            mode: 0o777,
            type_: vfs::FileType::from(disk_inode.type_.clone()),
            blocks: disk_inode.blocks as usize,
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            nlinks: disk_inode.nlinks as usize,
            uid: 0,
            gid: 0,
            blk_size: BLKSIZE,
            rdev: self.device_inode_id,
        })
    }
    fn set_metadata(&self, _metadata: &vfs::Metadata) -> vfs::Result<()> {
        // No-op for lfs
        Ok(())
    }
    fn sync_all(&self) -> vfs::Result<()> {
        let mut disk_inode = self.disk_inode.write();
        debug!("sync_all: id {} dirty {} stale {}", self.id, disk_inode.dirty(), disk_inode.stale());
        if disk_inode.dirty() {
            // allocate a new block and append write to it
            let new_blk_id = if disk_inode.stale() {
                self.fs.alloc_block().unwrap()
            } else {
                self.blk_id
            };
            self.fs._record_block_summary(self.id, new_blk_id, -1);
            // update imaps
            let mut imaps = self.fs.imaps.write();
            if let Some(x) = imaps.get_mut(&self.id) {
                *x = new_blk_id;
            }
            self.fs
                .device
                .write_block(new_blk_id, 0, disk_inode.as_buf())?;
            disk_inode.sync();
        }
        Ok(())
    }
    fn sync_data(&self) -> vfs::Result<()> {
        self.sync_all()
    }
    fn resize(&self, len: usize) -> vfs::Result<()> {
        if self.disk_inode.read().type_ != FileType::File
            && self.disk_inode.read().type_ != FileType::SymLink
        {
            return Err(FsError::NotFile);
        }
        self._resize(len)
    }
    fn create2(
        &self,
        name: &str,
        type_: vfs::FileType,
        _mode: u32,
        data: usize,
    ) -> vfs::Result<Arc<dyn vfs::INode>> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }

        // Ensure the name is not exist
        if !self.get_file_inode_id(name).is_none() {
            return Err(FsError::EntryExist);
        }

        // Create new INode
        let inode = match type_ {
            vfs::FileType::File => self.fs.new_inode_file()?,
            vfs::FileType::SymLink => self.fs.new_inode_symlink()?,
            vfs::FileType::Dir => self.fs.new_inode_dir(self.id)?,
            vfs::FileType::CharDevice => self.fs.new_inode_chardevice(data)?,
            _ => return Err(vfs::FsError::InvalidParam),
        };

        // Write new entry
        self.append_direntry(&DiskEntry {
            id: inode.id as u32,
            name: Str256::from(name),
        })?;
        inode.nlinks_inc();
        if type_ == vfs::FileType::Dir {
            inode.nlinks_inc(); //for .
            self.nlinks_inc(); //for ..
        }
        debug!("create2: {} created ino:{} blkid:{}", name, inode.id, inode.blk_id);
        Ok(inode)
    }
    fn link(&self, name: &str, other: &Arc<dyn INode>) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if !self.get_file_inode_id(name).is_none() {
            return Err(FsError::EntryExist);
        }
        let child = other
            .downcast_ref::<INodeImpl>()
            .ok_or(FsError::NotSameFs)?;
        if !Arc::ptr_eq(&self.fs, &child.fs) {
            return Err(FsError::NotSameFs);
        }
        if child.metadata()?.type_ == vfs::FileType::Dir {
            return Err(FsError::IsDir);
        }
        self.append_direntry(&DiskEntry {
            id: child.id as u32,
            name: Str256::from(name),
        })?;
        child.nlinks_inc();
        Ok(())
    }
    fn unlink(&self, name: &str) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks <= 0 {
            return Err(FsError::DirRemoved);
        }
        if name == "." {
            return Err(FsError::IsDir);
        }
        if name == ".." {
            return Err(FsError::IsDir);
        }

        let (inode_id, entry_id) = self
            .get_file_inode_and_entry_id(name)
            .ok_or(FsError::EntryNotFound)?;
        let inode = self.fs.get_inode(inode_id);

        let type_ = inode.disk_inode.read().type_;
        if type_ == FileType::Dir {
            // only . and ..
            if inode.disk_inode.read().size as usize / DIRENT_SIZE > 2 {
                return Err(FsError::DirNotEmpty);
            }
        }
        inode.nlinks_dec();
        println!("inode links {}", inode.disk_inode.read().nlinks);
        if type_ == FileType::Dir {
            inode.nlinks_dec(); //for .
            self.nlinks_dec(); //for ..
        }
        self.remove_direntry(entry_id)?;
        Ok(())
    }
    fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> vfs::Result<()> {
        Err(FsError::NotSupported)
    }
    fn find(&self, name: &str) -> vfs::Result<Arc<dyn vfs::INode>> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        let inode_id = self.get_file_inode_id(name).ok_or(FsError::EntryNotFound)?;
        debug!("find name:{} myid:{} id:{}", name, self.id, inode_id);
        Ok(self.fs.get_inode(inode_id))
    }
    fn get_entry(&self, id: usize) -> vfs::Result<String> {
        if self.disk_inode.read().type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if id >= self.disk_inode.read().size as usize / DIRENT_SIZE {
            return Err(FsError::EntryNotFound);
        };
        let entry = self.read_direntry(id)?;
        Ok(String::from(entry.name.as_ref()))
    }
    fn io_control(&self, _cmd: u32, _data: usize) -> vfs::Result<()> {
        Err(FsError::NotSupported)
    }
    fn mmap(&self, _area: MMapArea) -> vfs::Result<()> {
        Err(FsError::NotSupported)
    }
    fn fs(&self) -> Arc<dyn vfs::FileSystem> {
        self.fs.clone()
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl Drop for INodeImpl {
    /// Auto sync when drop
    fn drop(&mut self) {
        if self.disk_inode.read().nlinks <= 0 {
            let mut disk_inode = self.disk_inode.write();
            // clean data block and inode itself
            disk_inode.sync();
            let mut segments = self.fs.segments.write();
            let cur_seg = segments.get_mut(&(self.fs.super_block.read().current_seg_id as usize)).unwrap();
            cur_seg.seg_imap.write().insert(self.id, 0); // indicate this inode is being dumped
            drop(cur_seg);
            drop(segments);
            drop(disk_inode);
            self._free_all_block().unwrap();
            self.fs.free_block(self.blk_id);
        } else {
            self.sync_all()
                .expect("Failed to sync when dropping the LogStructureFileSystem Inode");
        }
    }
}

/// filesystem for lfs
///
/// ## 内部可变性
/// 为了方便协调外部及INode对LFS的访问，并为日后并行化做准备，
/// 将LFS设置为内部可变，即对外接口全部是&self，struct的全部field用RwLock包起来
/// 这样其内部各field均可独立访问
pub struct LogFileSystem {
    /// on-disk superblock
    super_block: RwLock<Dirty<SuperBlock>>,
    imaps: RwLock<Dirty<IMapTable>>,
    check_region: RwLock<Dirty<CheckRegion>>,
    inodes: RwLock<BTreeMap<INodeId, Weak<INodeImpl>>>, // should be in Segment struct
    segments: RwLock<BTreeMap<SegmentId, Segment>>,
    /// device
    device: Arc<dyn Device>,
    /// Pointer to self, used by INodes
    self_ptr: Weak<LogFileSystem>,
    /// device inode
    device_inodes: RwLock<BTreeMap<usize, Arc<DeviceINode>>>, // aoslab don't know the use
}

impl LogFileSystem {
    /// Load LFS from device
    pub fn open(device: Arc<dyn Device>) -> vfs::Result<Arc<Self>> {
        let super_block = device.load_struct::<SuperBlock>(BLKN_SUPER)?;
        let check_region = device.load_struct::<CheckRegion>(BLKN_CR)?;
        let mut imaps = BTreeMap::new();
        let inodes_num: u32 = check_region.inodes_num;
        // device.read_block(BLKN_CR, 0, imaps_blkid.as_buf_mut())?;
        // device.read_block(BLKN_CR, 4, inodes_num.as_buf_mut())?;
        debug!("sb size: {} info {:?}", mem::size_of::<SuperBlock>(), super_block.info);
        debug!("imaps inonum {}", inodes_num);
        let current_segment_id = super_block.current_seg_id as usize;
        let mut segments = BTreeMap::new();
        for i in 1..(current_segment_id+1) {
            let seg_meta: SegmentMeta = device.load_struct::<SegmentMeta>(i * SEGMENT_SIZE / BLKSIZE)?;
            // device.read_block(i * SEGMENT_SIZE / BLKSIZE, 0, seg_size.as_buf_mut())?;
            // device.read_block(i * SEGMENT_SIZE / BLKSIZE, 4, seg_inodes_num.as_buf_mut())?;
            // device.read_block(i * SEGMENT_SIZE / BLKSIZE, 8, seg_unused.as_buf_mut())?;

            let mut seg_imap = BTreeMap::new();
            let mut seg_summary = BTreeMap::new();
            for ino_i in 0..seg_meta.inodes_num as usize {
                let mut blk_id: u32 = 0;
                let mut inode_id: u32 = 0;
                // debug!("read device {}", (i * SEGMENT_SIZE + SEGMENT_META_SIZE) / BLKSIZE + ino_i * 8);
                device.read_block((i * SEGMENT_SIZE + SEGMENT_META_SIZE) / BLKSIZE, ino_i * 8, inode_id.as_buf_mut())?;
                device.read_block((i * SEGMENT_SIZE + SEGMENT_META_SIZE) / BLKSIZE, ino_i * 8 + 4, blk_id.as_buf_mut())?;
                if blk_id != 0 {
                    imaps.insert(inode_id as usize, blk_id as usize);
                    seg_imap.insert(inode_id as usize, blk_id as usize);
                    debug!("load ino {} blkid {}", inode_id, blk_id);
                }
            }
            let blk_id_begin = (SEGMENT_META_SIZE + IMAP_PER_SEGMENT_SIZE + SS_PER_SEGMENT_SIZE) / BLKSIZE;
            for blk_i in blk_id_begin..(seg_meta.size as usize)/BLKSIZE {
                let mut entry_i: SummaryEntry = unsafe { MaybeUninit::uninit().assume_init() };
                device.read_block((i * SEGMENT_SIZE + SEGMENT_META_SIZE + SS_PER_SEGMENT_SIZE) / BLKSIZE, blk_i * mem::size_of::<SummaryEntry>(), entry_i.as_buf_mut())?;
                seg_summary.insert(blk_i, entry_i);
            }

            let segment_i = Segment {
                meta: seg_meta,
                seg_imap: RwLock::new(Dirty::new(seg_imap)),
                summary_map: RwLock::new(Dirty::new(seg_summary)),
            };

            segments.insert(i, segment_i);
        }
        debug!("finish loading imaps and segment meta...");

        if !super_block.check() {
            return Err(FsError::WrongFs);
        }

        Ok(LogFileSystem {
            super_block: RwLock::new(Dirty::new(super_block)),
            imaps: RwLock::new(Dirty::new(imaps)),
            check_region: RwLock::new(Dirty::new(check_region)),
            inodes: RwLock::new(BTreeMap::new()),
            segments: RwLock::new(BTreeMap::new()),
            device,
            self_ptr: Weak::default(),
            device_inodes: RwLock::new(BTreeMap::new()),
        }
        .wrap())
    }
    /// Create a new LFS on blank disk
    pub fn create(device: Arc<dyn Device>, space: usize) -> vfs::Result<Arc<Self>> {
        let blocks = space / BLKSIZE;
        let current_seg_id_: usize = 1; // segment 0 is reserved for superblock
        let n_segment = space / SEGMENT_SIZE;
        let unused_blocks_ = ((n_segment - current_seg_id_) + SEGMENT_SIZE) * SEGMENT_SIZE / BLKSIZE;
        assert!(blocks >= 16, "space too small");
        let super_block = SuperBlock {
            magic: MAGIC,
            blocks: blocks as u32,
            unused_blocks: unused_blocks_ as u32,
            info: Str32::from(DEFAULT_INFO),
            current_seg_id: current_seg_id_ as u32,
            next_ino_number: INO_ROOT as u32,
            n_segment: n_segment as u32,
        };

        let check_region = CheckRegion {
            inodes_num: 0,
        };

        let lfs = LogFileSystem {
            super_block: RwLock::new(Dirty::new_dirty(super_block)),
            imaps: RwLock::new(Dirty::new_dirty(BTreeMap::new())),
            check_region: RwLock::new(Dirty::new_dirty(check_region)),
            inodes: RwLock::new(BTreeMap::new()),
            segments: RwLock::new(BTreeMap::new()),
            device,
            self_ptr: Weak::default(),
            device_inodes: RwLock::new(BTreeMap::new()),
        }
        .wrap();
        debug!("alloc segment...");

        // Insert segment1
        lfs.alloc_segment(1);
        debug!("init root inode...");
        // Init root INode
        let root_blkid = lfs.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        // debug!("init current_segment_size:{}", lfs.super_block.read().current_seg_size);
        let root_inode = lfs._new_inode(root_blkid, Dirty::new_dirty(DiskINode::new_dir()));
        lfs._record_block_summary(root_inode.id, root_blkid, -1);
        root_inode.init_direntry(root_blkid)?;
        root_inode.nlinks_inc(); //for .
        root_inode.nlinks_inc(); //for ..(root's parent is itself)
        debug!("syncing root inode...");
        root_inode.sync_all()?;
        debug!("create lfs done");
        debug!("rootnode type {:?}", root_inode.disk_inode.read().type_);
        Ok(lfs)
    }
    /// Wrap pure LogFileSystem with Arc
    /// Used in constructors
    fn wrap(self) -> Arc<Self> {
        // Create an Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let fs = Arc::new(self);
        let weak = Arc::downgrade(&fs);
        let ptr = Arc::into_raw(fs) as *mut Self;
        unsafe {
            (*ptr).self_ptr = weak;
        }
        unsafe { Arc::from_raw(ptr) }
    }

    fn alloc_segment(&self, seg_id: usize) {
        assert!(!self.segments.read().contains_key(&seg_id));
        assert!(seg_id < self.super_block.read().n_segment as usize);
        let segment = Segment {
            meta: SegmentMeta {
                size: SEGMENT_META_SIZE + IMAP_PER_SEGMENT_SIZE + SS_PER_SEGMENT_SIZE,
                inodes_num: 0,
                unused: 0,
            },
            seg_imap: RwLock::new(Dirty::new_dirty(BTreeMap::new())),
            summary_map: RwLock::new(Dirty::new_dirty(BTreeMap::new())),
        };
        self.segments.write().insert(seg_id, segment);
    }

    /// Allocate a block, return block id
    fn alloc_block(&self) -> Option<usize> {
        let current_seg_id = self.super_block.read().current_seg_id as usize;
        let mut sb = self.super_block.write();
        sb.unused_blocks -= 1;
        let cur_seg_id = sb.current_seg_id as usize;
        let mut current_seg_size = self.segments.read().get(&cur_seg_id).unwrap().meta.size;
        let new_blk_id = (current_seg_size + current_seg_id * SEGMENT_SIZE) / BLKSIZE;
        // debug!("seg size {} {}", current_seg_id, current_seg_size);
        if current_seg_size > SEGMENT_SIZE - BLKSIZE {
            return None;
        } else {
            current_seg_size += BLKSIZE;
            self.segments.write().get_mut(&cur_seg_id).unwrap().meta.size = current_seg_size;
            if current_seg_size == SEGMENT_SIZE {
                let new_seg_id = current_seg_id + 1;
                sb.current_seg_id = new_seg_id as u32;
                drop(sb);
                self.alloc_segment(new_seg_id as usize);
                debug!("allocate blkid: {} at segment: {} seg_size: {}", new_blk_id, current_seg_id, current_seg_size);
            }
        }
        Some(new_blk_id)
    }

    /// Free a block
    fn free_block(&self, block_id: usize) {
        let mut segments = self.segments.write();
        let cur_seg = segments.get_mut(&(self.super_block.read().current_seg_id as usize)).unwrap();
        cur_seg.summary_map.write().insert(block_id, SummaryEntry {
            entry_id: -2,
            inode_id: -1, // indicate an invalid (unused) block
        });
        self.super_block.write().unused_blocks += 1;
        debug!("free block {:#x}", block_id);
    }

    pub fn new_device_inode(&self, device_inode_id: usize, device_inode: Arc<DeviceINode>) {
        self.device_inodes
            .write()
            .insert(device_inode_id, device_inode);
    }

    /// Create a new INode struct, then insert it to self.inodes
    /// Private used for load or create INode
    fn _new_inode(&self, blk_id: BlockId, disk_inode: Dirty<DiskINode>) -> Arc<INodeImpl> {
        let device_inode_id = disk_inode.device_inode_id;
        let mut cr = self.check_region.write();
        let ino_id = cr.inodes_num as usize;
        let inode = Arc::new(INodeImpl {
            id: ino_id,
            blk_id: blk_id,
            disk_inode: RwLock::new(disk_inode),
            fs: self.self_ptr.upgrade().unwrap(),
            device_inode_id: device_inode_id,
        });
        cr.inodes_num += 1;
        let cur_seg_id = self.super_block.read().current_seg_id as usize;
        let mut segments = self.segments.write();
        let cur_seg = segments.get_mut(&cur_seg_id).unwrap();
        cur_seg.meta.inodes_num += 1;
        cur_seg.seg_imap.write().insert(ino_id, blk_id);
        self.imaps.write().insert(ino_id, blk_id);
        self.inodes.write().insert(ino_id, Arc::downgrade(&inode));
        println!("add inode {} -> {}  segid {}", ino_id, blk_id, cur_seg_id);
        inode
    }

    fn _record_block_summary(&self, ino_id: INodeId, blk_id: BlockId, entry_id: isize) {
        let mut segments = self.segments.write();
        let cur_seg = segments.get_mut(&((self.super_block.read().current_seg_id as usize))).unwrap();
        cur_seg.summary_map.write().insert(blk_id as usize, SummaryEntry{
            inode_id: ino_id as i32,
            entry_id: entry_id as i32,
        });
    }

    // map an inode to a existing block
    fn _map_inode(&self, ino_id: INodeId, blk_id: BlockId, disk_inode: Dirty<DiskINode>) -> Arc<INodeImpl> {
        let device_inode_id = disk_inode.device_inode_id;
        let inode = Arc::new(INodeImpl {
            id: ino_id,
            blk_id: blk_id,
            disk_inode: RwLock::new(disk_inode),
            fs: self.self_ptr.upgrade().unwrap(),
            device_inode_id: device_inode_id,
        });
        self.inodes.write().insert(ino_id, Arc::downgrade(&inode));
        inode
    }

    /// Get inode by id. Load if not in memory.
    /// ** Must ensure it's a valid INode **
    fn get_inode(&self, id: INodeId) -> Arc<INodeImpl> {
        // debug!("in get_inode contains {}", self.imaps.read().contains_key(&id))
        assert!(self.imaps.read().contains_key(&id));
        let imaps_ptr = self.imaps.read();
        debug!("get_inode: id={}", id);
        let blk_ptr = imaps_ptr.get(&id).unwrap();
        debug!("get_inode: blkid={}", blk_ptr);
        // In the BTreeSet and not weak.
        if let Some(inode) = self.inodes.read().get(&id) {
            if let Some(inode) = inode.upgrade() {
                return inode;
            }
        }
        let blk = *blk_ptr;
        // Load if not in set, or is weak ref.
        let mut disk_inode = Dirty::new(self.device.load_struct::<DiskINode>(blk).unwrap());
        // debug!("TTT id {} turn_stale", id);
        disk_inode.turn_stale();
        self._map_inode(id, blk, disk_inode)
    }
    /// Create a new INode file
    fn new_inode_file(&self) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_file());
        let new_inode = self._new_inode(id, disk_inode);
        self._record_block_summary(new_inode.id, new_inode.blk_id, -1); // -1 indicates it is inode itself
        Ok(new_inode)
    }
    /// Create a new INode symlink
    fn new_inode_symlink(&self) -> vfs::Result<Arc<INodeImpl>> {
        Err(FsError::NotSupported)
    }
    /// Create a new INode dir
    fn new_inode_dir(&self, parent: INodeId) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let disk_inode = Dirty::new_dirty(DiskINode::new_dir());
        let inode = self._new_inode(id, disk_inode);
        inode.init_direntry(parent)?;
        self._record_block_summary(inode.id, inode.blk_id, -1); // -1 indicates it is inode itself
        Ok(inode)
    }
    /// Create a new INode chardevice
    pub fn new_inode_chardevice(&self, device_inode_id: usize) -> vfs::Result<Arc<INodeImpl>> {
        Err(FsError::NotSupported)
    }
    fn flush_weak_inodes(&self) {
        let mut inodes = self.inodes.write();
        let remove_ids: Vec<_> = inodes
            .iter()
            .filter(|(_, inode)| inode.upgrade().is_none())
            .map(|(&id, _)| id)
            .collect();
        for id in remove_ids.iter() {
            inodes.remove(&id);
        }
    }

    fn _detect_garbage_segment(&self) {
        for seg_i in 0..self.super_block.read().n_segment as usize {
            let mut segments = self.segments.write();
            let seg = segments.get_mut(&seg_i).unwrap();
            let mut cleanable = true;
            if seg.meta.size == SEGMENT_SIZE {
                // only search for full segments, which can be full of stale blocks
                for (blkid, entry_i) in seg.summary_map.write().iter() {
                    let ino_id = entry_i.inode_id as usize;
                    let alive;
                    if entry_i.entry_id == -2 {
                        alive = false;
                    }
                    else if entry_i.entry_id == -1 {
                        // if it is an inode/indirect block, it is alive only when it exists in imaps
                        if self.imaps.read().contains_key(&ino_id) && (*self.imaps.read().get(&ino_id).unwrap())>0 {
                            alive = true;
                        } else {
                            alive = false;
                        }
                    } else {
                        let inode = self.get_inode(ino_id);
                        let latest_blk_id = inode.get_disk_block_id(entry_i.entry_id as usize).unwrap();
                        alive = (*blkid) == latest_blk_id;
                    }
                    if alive {
                        // there is still at least one live block in one segment, skip it.
                        cleanable = false;
                        break
                    }
                }
            }
            if cleanable {
                seg.meta.unused = 1;
            }
        }
    }
}

impl vfs::FileSystem for LogFileSystem {
    /// Write back super block if dirty
    fn sync(&self) -> vfs::Result<()> {
        let mut super_block = self.super_block.write();
        if super_block.dirty() {
            self.device
                .write_at(BLKSIZE * BLKN_SUPER, super_block.as_buf())?;
            super_block.sync();
        }
        let mut imaps = self.imaps.write();
        let mut cr = self.check_region.write();
        if imaps.dirty() {
            cr.inodes_num = imaps.len() as u32;
            // debug!("writeback imaps offset {} len:{}", BLKN_CR * BLKSIZE, cr.inodes_num);
            self.device.write_at(BLKN_CR * BLKSIZE, cr.as_buf())?;
            cr.sync();
            imaps.sync();
        }
        for (seg_id, segment) in self.segments.read().iter() {
            // let seg_size = segment.meta.size as u32;
            // let seg_inodes_num = segment.meta.inodes_num as u32;
            // let seg_unused = segment.meta.unused as u32;
            assert!(*seg_id > 0);
            // sync metadata
            self.device.write_at(seg_id * SEGMENT_SIZE, segment.meta.as_buf())?;
            // self.device.write_at(seg_id * SEGMENT_SIZE, seg_size.as_buf())?;
            // self.device.write_at(seg_id * SEGMENT_SIZE + 4, seg_inodes_num.as_buf())?;
            // self.device.write_at(seg_id * SEGMENT_SIZE + 8, seg_unused.as_buf())?;
            // sync imaps per seg
            let mut seg_imaps = segment.seg_imap.write();
            if seg_imaps.dirty() {
                let mut idx = 0;
                for (ino_i, blkid_i) in seg_imaps.iter() {
                    self.device.write_at(seg_id * SEGMENT_SIZE + SEGMENT_META_SIZE + idx * 8, (*ino_i as u32).as_buf())?;
                    self.device.write_at(seg_id * SEGMENT_SIZE + SEGMENT_META_SIZE + idx * 8 + 4, (*blkid_i as u32).as_buf())?;
                    debug!("sync ino {} blkid {}", ino_i, blkid_i);
                    idx += 1;
                }
                seg_imaps.sync();
            }
            // sync segment summary
            let mut seg_summary = segment.summary_map.write();
            if seg_summary.dirty() {
                // let mut idx = 0;
                for (blk_id, entry_i) in seg_summary.iter() {
                    debug!("sync blkid {} offset {}", blk_id, seg_id * SEGMENT_SIZE + SEGMENT_META_SIZE + IMAP_PER_SEGMENT_SIZE + blk_id * mem::size_of::<SummaryEntry>());
                    self.device.write_at(seg_id * SEGMENT_SIZE + SEGMENT_META_SIZE + IMAP_PER_SEGMENT_SIZE + blk_id * mem::size_of::<SummaryEntry>(), entry_i.as_buf())?;
                    // idx += 1;
                }
                seg_summary.sync();
            }
        }

        self.flush_weak_inodes();
        for inode in self.inodes.read().values() {
            if let Some(inode) = inode.upgrade() {
                inode.sync_all()?;
            }
        }
        self.device.sync()?;
        Ok(())
    }

    fn root_inode(&self) -> Arc<dyn vfs::INode> {
        debug!("get root inode");
        return self.get_inode(INO_ROOT);
    }

    fn info(&self) -> vfs::FsInfo {
        let sb = self.super_block.read();
        vfs::FsInfo {
            bsize: BLKSIZE,
            frsize: BLKSIZE,
            blocks: sb.blocks as usize,
            bfree: sb.unused_blocks as usize,
            bavail: sb.unused_blocks as usize,
            files: sb.blocks as usize,        // inaccurate
            ffree: sb.unused_blocks as usize, // inaccurate
            namemax: MAX_FNAME_LEN,
        }
    }
}

impl Drop for LogFileSystem {
    /// Auto sync when drop
    fn drop(&mut self) {
        self.sync()
            .expect("Failed to sync when dropping the LogFileSystem");
    }
}

trait BitsetAlloc {
    fn alloc(&mut self) -> Option<usize>;
}

impl AsBuf for [u8; BLKSIZE] {}

impl From<FileType> for vfs::FileType {
    fn from(t: FileType) -> Self {
        match t {
            FileType::File => vfs::FileType::File,
            FileType::SymLink => vfs::FileType::SymLink,
            FileType::Dir => vfs::FileType::Dir,
            FileType::CharDevice => vfs::FileType::CharDevice,
            FileType::BlockDevice => vfs::FileType::BlockDevice,
            _ => panic!("unknown file type"),
        }
    }
}
