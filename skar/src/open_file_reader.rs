use std::{fs::File, io::BufReader, path::Path};

// #[cfg(target_os = "linux")]
// use std::os::unix::fs::OpenOptionsExt;

use anyhow::{Context, Result};

pub fn open_file_reader(path: &Path) -> Result<BufReader<File>> {
    let file = open_file(path).context("open file")?;

    Ok(BufReader::new(file))
}

pub fn open_file(path: &Path) -> std::io::Result<File> {
    // #[cfg(target_os = "linux")]
    // let res = File::options()
    //     .read(true)
    //     .custom_flags(libc::O_DIRECT)
    //     .open(path);

    //#[cfg(not(target_os = "linux"))]
    let res = File::options().read(true).open(path);

    res
}
