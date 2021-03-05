#![feature(alloc)]

use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(C)]
struct SyncRingBuf<T> {
    buf: *mut T,
    cap: usize,
    read_idx: CacheLine,
    write_idx: CacheLine,
}
#[repr(align(64))]
struct CacheLine {
    inner: AtomicUsize,
}

impl CacheLine {
    fn new(n: usize) -> Self {
        Self {
            inner: AtomicUsize::new(n)
        }
    }
}

unsafe impl<T: Sync> Sync for SyncRingBuf<T> {}

impl<T> SyncRingBuf<T> {
    pub fn with_capacity(mut cap: usize) -> Self {
        assert!(cap > 0 && cap < usize::MAX, "invalid capacity size");
        cap += 1;

        let mut v = Vec::with_capacity(cap);
        let buf = v.as_mut_ptr();
        std::mem::forget(v);

        Self {
            buf,
            cap,
            read_idx: CacheLine::new(0),
            write_idx: CacheLine::new(0),
        }
    }

    pub fn try_push(&self, t: T) -> Option<T> {
        // load the write index
        let curr_write_idx = self.write_idx.inner.load(Ordering::Relaxed);

        // compute the next write index
        let mut next_write_idx = curr_write_idx + 1;
        if next_write_idx >= self.cap {
            next_write_idx = 0;
        }

        // load up the read index
        let curr_read_idx = self.read_idx.inner.load(Ordering::Acquire);

        // do the actual push
        if next_write_idx == curr_read_idx {
            Some(t)
        } else {
            unsafe {
                std::ptr::write(self.buf.add(curr_write_idx), t);
            }
            self.write_idx.inner.store(next_write_idx, Ordering::Release);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    fn align_size() {
        assert_eq!(size_of::<SyncRingBuf<i32>>(), 192);
        assert_eq!(size_of::<CacheLine>(), 64);
        let mut v = 5;
        let empty_val = SyncRingBuf {
            buf: &mut v as *mut i32,
            cap: 0,
            read_idx: CacheLine {
                inner: AtomicUsize::new(0),
            },
            write_idx: CacheLine {
                inner: AtomicUsize::new(0),
            },
        };

        let empty_val_addr = &empty_val as *const SyncRingBuf<i32> as usize;
        let read_index_addr = &empty_val.read_idx as *const CacheLine as usize;
        let write_index_addr = &empty_val.write_idx as *const CacheLine as usize;
        let buf_addr = &empty_val.buf as *const *mut i32 as usize;
        let cap_addr = &empty_val.cap as *const usize as usize;

        assert_eq!(buf_addr - empty_val_addr, 0);
        assert_eq!(cap_addr - empty_val_addr, 8);
        assert_eq!(read_index_addr - empty_val_addr, 64);
        assert_eq!(write_index_addr - empty_val_addr, 128);
    }
}
