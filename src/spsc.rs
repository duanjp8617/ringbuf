#![feature(alloc)]

use std::sync::atomic::AtomicUsize;

#[repr(align(64))]
struct SyncRingBuf<T> {
    buf: *mut T,
    cap: usize,
    read_index: CacheLine,
    write_index: CacheLine,
}
#[repr(align(64))]
struct CacheLine {
    inner: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use std::mem::{size_of, size_of_val};

    use super::*;

    #[test]
    fn align_size() {
        assert_eq!(size_of::<SyncRingBuf<i32>>(), 192);
        assert_eq!(size_of::<CacheLine>(), 64);
        let mut v = 5;
        let empty_val = SyncRingBuf {
            buf: &mut v as *mut i32,
            cap: 0,
            read_index: CacheLine {
                inner: AtomicUsize::new(0),
            },
            write_index: CacheLine {
                inner: AtomicUsize::new(0),
            },
        };

        let empty_val_addr = &empty_val as *const SyncRingBuf<i32> as usize;
        let read_index_addr = &empty_val.read_index as *const CacheLine as usize;
        let write_index_addr = &empty_val.write_index as *const CacheLine as usize;
        let buf_addr = &empty_val.buf as *const *mut i32 as usize;
        let cap = &empty_val.cap as *const usize as usize;

        println!(
            "{}, {}, {}, {}",
            read_index_addr - empty_val_addr,
            write_index_addr - empty_val_addr,
            buf_addr - empty_val_addr,
            cap - empty_val_addr
        );
        // assert_eq!((read_index_addr as usize) - (empty_val_addr as usize), 64);
        // assert_eq!((write_index_addr as usize) - (empty_val_addr as usize), 128);
        assert_eq!(size_of_val(&empty_val), 192);
    }
}
