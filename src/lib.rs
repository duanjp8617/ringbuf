mod bootstrap;
use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

pub use bootstrap::RingBuf;

mod spsc;

#[repr(C)]
pub struct SyncRingBuf<T> {
    // first cacheline
    buf: *mut T,
    buf_len: usize,
    cap: usize,
    padding1: [usize; 5],
    // second cacheline
    read_idx: AtomicUsize,
    local_write_idx: Cell<usize>,
    padding2: [usize; 6],
    // third cacheline
    write_idx: AtomicUsize,
    local_read_idx: Cell<usize>,
    padding3: [usize; 6],
}

// make SyncRingBuf<T> sync if T is sync
unsafe impl<T: Sync> Sync for SyncRingBuf<T> {}

impl<T> SyncRingBuf<T> {
    pub fn with_capacity_at_least(cap_at_least: usize) -> Self {
        assert!(cap_at_least > 1, "invalid capacity size");

        let cap_power_of_two = cap_at_least.next_power_of_two();
        let buf_len = if cap_at_least == cap_power_of_two {
            (cap_at_least + 1).next_power_of_two()
        } else {
            cap_power_of_two
        };

        // allocate the buffer via a vector
        let mut v = Vec::with_capacity(buf_len);
        let buf = v.as_mut_ptr();
        std::mem::forget(v);

        Self {
            // first cacheline
            buf,
            buf_len,
            cap: buf_len - 1,
            padding1: [0; 5],
            // second cacheline
            read_idx: AtomicUsize::new(0),
            local_write_idx: Cell::new(0),
            padding2: [0; 6],
            // third cacheline
            write_idx: AtomicUsize::new(0),
            local_read_idx: Cell::new(0),
            padding3: [0; 6],
        }
    }

    pub fn try_push(&self, t: T) -> Option<T> {
        let curr_write_idx = self.write_idx.load(Ordering::Relaxed);
        let next_write_idx = curr_write_idx + 1 & self.cap;

        if next_write_idx == self.local_read_idx.get() {
            self.local_read_idx
                .set(self.read_idx.load(Ordering::Acquire));
            if next_write_idx == self.local_read_idx.get() {
                return Some(t);
            }
        }

        unsafe {
            std::ptr::write(self.buf.add(curr_write_idx), t);
        }
        self.write_idx.store(next_write_idx, Ordering::Release);
        None
    }

    pub fn try_pop(&self) -> Option<T> {
        let curr_read_idx = self.read_idx.load(Ordering::Relaxed);

        if curr_read_idx == self.local_write_idx.get() {
            self.local_write_idx
                .set(self.write_idx.load(Ordering::Acquire));
            if curr_read_idx == self.local_write_idx.get() {
                return None;
            }
        }

        let t = unsafe { std::ptr::read(self.buf.add(curr_read_idx)) };
        self.read_idx
            .store((curr_read_idx + 1) & self.cap, Ordering::Release);
        Some(t)
    }
}

impl<T> Drop for SyncRingBuf<T> {
    fn drop(&mut self) {
        while let Some(_) = self.try_pop() {}

        unsafe {
            Vec::from_raw_parts(self.buf, 0, self.buf_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    #[should_panic]
    fn super_large_capacity() {
        let _b = SyncRingBuf::<i32>::with_capacity_at_least((2 as usize).pow(63));
    }

    #[test]
    #[should_panic]
    fn capacity_of_one() {
        let _b = SyncRingBuf::<i32>::with_capacity_at_least(1);
    }

    #[test]
    fn cache_aligned() {
        assert_eq!(size_of::<SyncRingBuf<i32>>(), 192);
        let empty_val = SyncRingBuf::<i32>::with_capacity_at_least(20);

        let base_addr = &empty_val as *const SyncRingBuf<i32> as usize;
        let read_index_addr = &empty_val.read_idx as *const AtomicUsize as usize;
        let write_index_addr = &empty_val.write_idx as *const AtomicUsize as usize;

        assert_eq!(read_index_addr - base_addr, 64);
        assert_eq!(write_index_addr - base_addr, 128);
    }
}
