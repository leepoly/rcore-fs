use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::mem::MaybeUninit;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::str;
use std::sync::Arc;

use rcore_fs::vfs::{FileSystem, FileType, INode};

const DEFAULT_MODE: u32 = 0o664;
const BUF_SIZE: usize = 0x1000;

pub fn zip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    println!("into zip dir:{}", path.display());
    let dir = fs::read_dir(path)?;
    for entry in dir {
        let entry = entry?;
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap();
        let type_ = entry.file_type()?;
        if type_.is_file() {
            let mut file = fs::File::open(entry.path())?;
            println!("processing file {:?} len: {}", entry.path(), file.metadata()?.len());
            let inode = inode.create(name, FileType::File, DEFAULT_MODE)?;
            inode.resize(file.metadata()?.len() as usize)?;
            let mut buf: [u8; BUF_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
            let mut offset = 0usize;
            let mut len = BUF_SIZE;
            while len == BUF_SIZE {
                len = file.read(&mut buf)?;
                inode.write_at(offset, &buf[..len])?;
                offset += len;
            }
            println!("processing {} done", name);
        } else if type_.is_dir() {
            println!("processing dir {}", name);
            let inode = inode.create(name, FileType::Dir, DEFAULT_MODE)?;
            zip_dir(entry.path().as_path(), inode)?;
        } else if type_.is_symlink() {
            let target = fs::read_link(entry.path())?;
            let inode = inode.create(name, FileType::SymLink, DEFAULT_MODE)?;
            #[cfg(unix)]
            let data = target.as_os_str().as_bytes();
            #[cfg(windows)]
            let data = target.to_str().unwrap().as_bytes();
            inode.resize(data.len())?;
            inode.write_at(0, data)?;
        }
    }

    Ok(())
}

pub fn zip_dir2(path: &Path, inode: Arc<dyn INode>, depth: usize) -> Result<(), Box<dyn Error>> {
    println!("fuse: finish creating img");
    inode.ls();
    // let files = inode.list()?;
    // for name in files.iter() {
    //     println!("file {}", name);
    // }
    let name_i = "hello_world";
    println!();
    println!("start lookup!");
    let inode = inode.lookup(name_i)?;

    println!("size {}", inode.metadata()?.size);
    println!("modify hello_world...");
    let mut file = fs::File::open("build/disk/test/temp123")?;
    let mut buf: [u8; BUF_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut offset = 0usize;
    let mut len = BUF_SIZE;
    while len == BUF_SIZE {
        len = file.read(&mut buf)?;
        inode.write_at(offset, &buf[..len])?;
        offset += len;
    }

    println!("size {}", inode.metadata()?.size);
    Ok(())
}

/// 为 [`INode`] 类型添加的扩展功能
pub trait INodeExt {
    /// 打印当前目录的文件
    fn ls(&self);

}

impl INodeExt for dyn INode {
    fn ls(&self) {
        let mut id = 0;
        while let Ok(name) = self.get_entry(id) {
            println!("{}", name);
            id += 1;
        }
    }

}

pub fn unzip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    println!("into unzip dir:{}", path.display());
    let files = inode.list()?;
    inode.ls();
    for name in files.iter().skip(2) {
        println!("processing file {}", name);
        let inode = inode.lookup(name.as_str())?;
        let mut path = path.to_path_buf();
        path.push(name);
        let info = inode.metadata()?;
        match info.type_ {
            FileType::File => {
                let mut file = fs::File::create(&path)?;
                let mut buf: [u8; BUF_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
                let mut offset = 0usize;
                let mut len = BUF_SIZE;
                while len == BUF_SIZE {
                    len = inode.read_at(offset, buf.as_mut())?;
                    file.write(&buf[..len])?;
                    offset += len;
                }
            }
            FileType::Dir => {
                fs::create_dir(&path)?;
                unzip_dir(path.as_path(), inode)?;
            }
            FileType::SymLink => {
                let mut buf: [u8; BUF_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
                let len = inode.read_at(0, buf.as_mut())?;
                #[cfg(unix)]
                std::os::unix::fs::symlink(str::from_utf8(&buf[..len]).unwrap(), path)?;
                #[cfg(windows)]
                std::os::windows::fs::symlink_file(str::from_utf8(&buf[..len]).unwrap(), path)?;
            }
            _ => panic!("unsupported file type"),
        }
    }
    Ok(())
}
