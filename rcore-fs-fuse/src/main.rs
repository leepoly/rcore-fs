use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use structopt::StructOpt;

use rcore_fs::dev::std_impl::StdTimeProvider;
use rcore_fs::vfs::FileSystem;
#[cfg(feature = "use_fuse")]
use rcore_fs_fuse::fuse::VfsFuse;
use rcore_fs_fuse::zip::{unzip_dir, zip_dir, zip_dir2};
use rcore_fs_sfs as sfs;
use rcore_fs_lfs as lfs;

use git_version::git_version;

#[derive(Debug, StructOpt)]
struct Opt {
    /// Command
    #[structopt(subcommand)]
    cmd: Cmd,

    /// Image file
    #[structopt(parse(from_os_str))]
    image: PathBuf,

    /// Target directory
    #[structopt(parse(from_os_str))]
    dir: PathBuf,

    /// File system: [sfs | sefs | ramfs]
    #[structopt(short = "f", long = "fs", default_value = "sfs")]
    fs: String,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    /// Create a new <image> for <dir>
    #[structopt(name = "zip")]
    Zip,

    /// Unzip data from given <image> to <dir>
    #[structopt(name = "unzip")]
    Unzip,

    /// Mount <image> to <dir>
    #[cfg(feature = "use_fuse")]
    #[structopt(name = "mount")]
    Mount,

    #[structopt(name = "git-version")]
    GitVersion,
}

fn main() {
    println!("modified in aoslab, supporting lfs");
    env_logger::init().unwrap();
    let opt = Opt::from_args();

    // open or create
    let create = match opt.cmd {
        #[cfg(feature = "use_fuse")]
        Cmd::Mount => !opt.image.is_dir() && !opt.image.is_file(),
        Cmd::Zip => true,
        Cmd::Unzip => false,
        Cmd::GitVersion => {
            println!("{}", git_version!());
            return;
        }
    };

    let fs: Arc<dyn FileSystem> = match opt.fs.as_str() {
        "sfs" => {
            let file = OpenOptions::new()
                .read(true)
                .write(create)
                .create(create)
                .truncate(create)
                .open(&opt.image)
                .expect("failed to open image");
            let device = Mutex::new(file);
            const MAX_SPACE: usize = 0x1000 * 0x1000 * 1024; // 1G
            match create {
                true => sfs::SimpleFileSystem::create(Arc::new(device), MAX_SPACE)
                    .expect("failed to create sfs"),
                false => sfs::SimpleFileSystem::open(Arc::new(device)).expect("failed to open sfs"),
            }
        }
        "lfs" => {
            let file = OpenOptions::new()
                .read(true)
                .write(create)
                .create(create)
                .truncate(create)
                .open(&opt.image)
                .expect("failed to open image");
            let device = Mutex::new(file);
            const MAX_SPACE: usize = 0x8000 * 1024; // 8M
            match create {
                true => lfs::LogFileSystem::create(Arc::new(device), MAX_SPACE)
                    .expect("failed to create lfs"),
                false => lfs::LogFileSystem::open(Arc::new(device)).expect("failed to open lfs"),
            }
        }
        _ => panic!("unsupported file system"),
    };
    match opt.cmd {
        #[cfg(feature = "use_fuse")]
        Cmd::Mount => {
            fuse::mount(VfsFuse::new(fs), &opt.dir, &[]).expect("failed to mount fs");
        }
        Cmd::Zip => {
            println!("fuse ready to zip");
            zip_dir(&opt.dir, fs.root_inode()).expect("failed to zip fs");
            // zip_dir2(&opt.dir, fs.root_inode(), 0).expect("failed to zip fs");
            println!("fuse zip done");
        }
        Cmd::Unzip => {
            std::fs::create_dir(&opt.dir).expect("failed to create dir");
            unzip_dir(&opt.dir, fs.root_inode()).expect("failed to unzip fs");
            println!("fuse unzip done");
        }
        Cmd::GitVersion => unreachable!(),
    }
    println!("fuse all done");
}
