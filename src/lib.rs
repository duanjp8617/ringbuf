mod bootstrap;
use std::{cell::Cell, sync::atomic::AtomicUsize};

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn super_large_capacity() {
        let power = std::mem::size_of::<usize>() * 8 - 1;
        let _b = SyncRingBuf::<i32>::with_capacity_at_least(
            (2 as usize).pow(power as u32),
        );
    }

    #[test]
    #[should_panic]
    fn capacity_of_one() {
        let _b = SyncRingBuf::<i32>::with_capacity_at_least(1);
    }
}
