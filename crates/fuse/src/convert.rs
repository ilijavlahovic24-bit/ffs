use common::types::FileType as CommonFileType;
use fuse3::FileType as FuseFileType;

pub fn to_fuse_type(k: CommonFileType) -> FuseFileType {
    match k {
        CommonFileType::File      => FuseFileType::RegularFile,
        CommonFileType::Directory => FuseFileType::Directory,
        CommonFileType::Symlink   => FuseFileType::Symlink,
    }
}

pub fn to_common_type(k: FuseFileType) -> CommonFileType {
    match k {
        FuseFileType::RegularFile => CommonFileType::File,
        FuseFileType::Directory   => CommonFileType::Directory,
        FuseFileType::Symlink     => CommonFileType::Symlink,
        _ => CommonFileType::File, // fallback za char/block/fifo/socket
    }
}