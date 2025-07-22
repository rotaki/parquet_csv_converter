use libc::{c_void, fstat, off_t, pread, pwrite};
use std::fs::File;
use std::io;
use std::os::unix::io::RawFd;
use std::path::Path;

/// Direct I/O alignment requirement (typically 512 bytes on Linux)
pub const DIRECT_IO_ALIGNMENT: usize = 512;

/// Default buffer size for aligned I/O operations (64KB)
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Page size for memory operations (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Calculate the aligned size by rounding up to the next alignment boundary
///
/// # Arguments
/// * `size` - The unaligned size
/// * `alignment` - The alignment requirement (must be a power of 2)
///
/// # Returns
/// The size rounded up to the next alignment boundary
#[inline]
pub fn align_up(size: usize, alignment: usize) -> usize {
    debug_assert!(
        alignment.is_power_of_two(),
        "Alignment must be a power of 2"
    );
    (size + alignment - 1) & !(alignment - 1)
}

/// Calculate the aligned offset by rounding down to the previous alignment boundary
///
/// # Arguments
/// * `offset` - The unaligned offset
/// * `alignment` - The alignment requirement (must be a power of 2)
///
/// # Returns
/// The offset rounded down to the previous alignment boundary
#[inline]
pub fn align_down(offset: u64, alignment: u64) -> u64 {
    debug_assert!(
        alignment.is_power_of_two(),
        "Alignment must be a power of 2"
    );
    offset & !(alignment - 1)
}

/// Calculate the offset within an aligned block
///
/// # Arguments
/// * `offset` - The unaligned offset
/// * `alignment` - The alignment requirement (must be a power of 2)
///
/// # Returns
/// The offset within the aligned block (0 to alignment-1)
#[inline]
pub fn offset_within_block(offset: u64, alignment: u64) -> usize {
    debug_assert!(
        alignment.is_power_of_two(),
        "Alignment must be a power of 2"
    );
    (offset & (alignment - 1)) as usize
}

#[inline]
pub fn open_file_with_direct_io(path: &Path) -> std::io::Result<File> {
    // If mac OS, do not use direct I/O
    #[cfg(target_os = "macos")]
    {
        use std::{fs::OpenOptions, os::fd::AsRawFd};

        use libc::fcntl;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let fd = file.as_raw_fd();
        let ret = unsafe { fcntl(fd, libc::F_NOCACHE, 1) };
        if ret == -1 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(file)
    }
    #[cfg(not(target_os = "macos"))]
    {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
    }
}

/// Get the size of a file using its raw file descriptor
pub fn file_size_fd(fd: RawFd) -> io::Result<u64> {
    let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };

    let result = unsafe { fstat(fd, &mut stat_buf) };

    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(stat_buf.st_size as u64)
    }
}

/// Perform pread using raw file descriptor
///
/// This function reads data from a file at a specific offset without changing
/// the file position. It's thread-safe and doesn't require synchronization.
pub fn pread_fd(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let result = unsafe {
        pread(
            fd,
            buf.as_mut_ptr() as *mut c_void,
            buf.len(),
            offset as off_t,
        )
    };

    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

/// Perform pwrite using raw file descriptor
///
/// This function writes data to a file at a specific offset without changing
/// the file position. It's thread-safe and doesn't require synchronization.
pub fn pwrite_fd(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize> {
    // Ensure buffer is aligned
    let buf_addr = buf.as_ptr() as usize;
    if buf_addr % DIRECT_IO_ALIGNMENT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Buffer is not properly aligned for Direct I/O",
        ));
    }

    // Ensure offset is aligned
    if offset % DIRECT_IO_ALIGNMENT as u64 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Offset is not properly aligned for Direct I/O",
        ));
    }

    // Ensure length is aligned
    if buf.len() % DIRECT_IO_ALIGNMENT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Buffer length is not properly aligned for Direct I/O",
        ));
    }

    let result = unsafe {
        pwrite(
            fd,
            buf.as_ptr() as *const c_void,
            buf.len(),
            offset as off_t,
        )
    };

    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_up() {
        // Test with 512-byte alignment
        assert_eq!(align_up(0, 512), 0);
        assert_eq!(align_up(1, 512), 512);
        assert_eq!(align_up(511, 512), 512);
        assert_eq!(align_up(512, 512), 512);
        assert_eq!(align_up(513, 512), 1024);
        assert_eq!(align_up(1000, 512), 1024);

        // Test with 4KB alignment
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4095, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn test_align_down() {
        // Test with 512-byte alignment
        assert_eq!(align_down(0, 512), 0);
        assert_eq!(align_down(1, 512), 0);
        assert_eq!(align_down(511, 512), 0);
        assert_eq!(align_down(512, 512), 512);
        assert_eq!(align_down(513, 512), 512);
        assert_eq!(align_down(1000, 512), 512);
        assert_eq!(align_down(1024, 512), 1024);

        // Test with 4KB alignment
        assert_eq!(align_down(0, 4096), 0);
        assert_eq!(align_down(1, 4096), 0);
        assert_eq!(align_down(4095, 4096), 0);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(4097, 4096), 4096);
        assert_eq!(align_down(8192, 4096), 8192);
    }

    #[test]
    fn test_offset_within_block() {
        // Test with 512-byte alignment
        assert_eq!(offset_within_block(0, 512), 0);
        assert_eq!(offset_within_block(1, 512), 1);
        assert_eq!(offset_within_block(511, 512), 511);
        assert_eq!(offset_within_block(512, 512), 0);
        assert_eq!(offset_within_block(513, 512), 1);
        assert_eq!(offset_within_block(1000, 512), 488);

        // Test with 4KB alignment
        assert_eq!(offset_within_block(0, 4096), 0);
        assert_eq!(offset_within_block(1, 4096), 1);
        assert_eq!(offset_within_block(4095, 4096), 4095);
        assert_eq!(offset_within_block(4096, 4096), 0);
        assert_eq!(offset_within_block(4097, 4096), 1);
    }

    #[test]
    #[should_panic]
    fn test_align_up_non_power_of_two() {
        align_up(100, 513); // 513 is not a power of 2
    }

    #[test]
    #[should_panic]
    fn test_align_down_non_power_of_two() {
        align_down(100, 513); // 513 is not a power of 2
    }
}
