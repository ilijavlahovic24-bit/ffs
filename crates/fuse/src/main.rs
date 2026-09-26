use std::env;
use std::path::PathBuf;

use tracing::Level;

use fuse::mount::mount;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    log_init();

    // CLI: ffs <mount_point> [wal_path] [tmp_dir] [data_dir]
    let mut args = env::args_os().skip(1);

    let mount_point = match args.next() {
        Some(p) => PathBuf::from(p),
        None => {
            eprintln!("usage: ffs <mount_point> [wal_path] [tmp_dir] [data_dir]");
            std::process::exit(1);
        }
    };

    let wal_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/ferumfs.wal"));
    let tmp_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/ferumfs-tmp"));
    let data_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/ferumfs-data"));

    for d in [&tmp_dir, &data_dir] {
        if let Err(e) = std::fs::create_dir_all(d) {
            eprintln!("failed to create {}: {e}", d.display());
            std::process::exit(1);
        }
    }

    if let Err(e) = mount(mount_point, wal_path, tmp_dir, data_dir).await {
        eprintln!("mount failed: {e}");
        std::process::exit(1);
    }
}

fn log_init() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}