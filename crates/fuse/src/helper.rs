use common::types::InodeInfo;
use fuse3::raw::reply::FileAttr;
use std::time::SystemTime;
use crate::convert::to_fuse_type;

pub fn to_file_attr(info: &InodeInfo) -> FileAttr {
    let kind = to_fuse_type(info.kind);
    FileAttr {
        ino: info.ino,
        size: info.size,
        blocks: (info.size + 511) / 512,
        atime: SystemTime::now().into(),
        mtime: SystemTime::now().into(),
        ctime: SystemTime::now().into(),
        #[cfg(target_os = "macos")]
        crtime: SystemTime::now().into(),
        kind,
        perm: info.mode,
        nlink: if kind == fuse3::FileType::Directory { 2 } else { 1 },
        uid: 0,
        gid: 0,
        rdev: 0,
        #[cfg(target_os = "macos")]
        flags: 0,
        blksize: 4096,
    }
}