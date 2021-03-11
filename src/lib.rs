use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{cell::Cell, cmp::min};

#[repr(C)]
struct SyncRingBuf<T> {
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
    fn with_capacity_at_least(cap_at_least: usize) -> Self {
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

    fn try_push(&self, t: T) -> Option<T> {
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

    fn try_pop(&self) -> Option<T> {
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

    #[inline]
    fn vacant_write_size(&self, curr_write_idx: usize) -> usize {
        let local_read_idx = self.local_read_idx.get();

        if local_read_idx <= curr_write_idx {
            self.cap - curr_write_idx + local_read_idx
        } else {
            local_read_idx - curr_write_idx - 1
        }
    }

    unsafe fn push_batch(&self, batch_ptr: *const T, batch_len: usize) -> usize {
        let curr_write_idx = self.write_idx.load(Ordering::Relaxed);
        let mut vacant_size = self.vacant_write_size(curr_write_idx);
        if vacant_size < batch_len {
            self.local_read_idx
                .set(self.read_idx.load(Ordering::Acquire));
            vacant_size = self.vacant_write_size(curr_write_idx);
            if vacant_size == 0 {
                return 0;
            }
        }
        let batch_size = std::cmp::min(vacant_size, batch_len);

        let next_write_idx = (curr_write_idx + batch_size) & self.cap;
        let second_half = if curr_write_idx + batch_size < self.buf_len {
            0
        } else {
            next_write_idx
        };
        let first_half = batch_size - second_half;

        std::ptr::copy(batch_ptr, self.buf.add(curr_write_idx), first_half);
        std::ptr::copy(batch_ptr.add(first_half), self.buf, second_half);

        self.write_idx.store(next_write_idx, Ordering::Release);

        batch_size
    }

    #[inline]
    fn available_read_size(&self, curr_read_idx: usize) -> usize {
        let local_write_idx = self.local_write_idx.get();
        if curr_read_idx <= local_write_idx {
            local_write_idx - curr_read_idx
        } else {
            self.buf_len - curr_read_idx + local_write_idx
        }
    }

    unsafe fn pop_batch(&self, batch_ptr: *mut T, batch_cap: usize) -> usize {
        let curr_read_idx = self.read_idx.load(Ordering::Relaxed);
        let mut available_size = self.available_read_size(curr_read_idx);
        if available_size < batch_cap {
            self.local_write_idx
                .set(self.write_idx.load(Ordering::Acquire));
            available_size = self.available_read_size(curr_read_idx);
            if available_size == 0 {
                return 0;
            }
        }
        let batch_size = std::cmp::min(available_size, batch_cap);

        let next_read_idx = (curr_read_idx + batch_size) & self.cap;
        let second_half = if curr_read_idx + batch_size < self.buf_len {
            0
        } else {
            next_read_idx
        };
        let first_half = batch_size - second_half;

        std::ptr::copy(self.buf.add(curr_read_idx), batch_ptr, first_half);
        std::ptr::copy(self.buf, batch_ptr.add(first_half), second_half);

        self.read_idx.store(next_read_idx, Ordering::Release);

        batch_size
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

pub struct Producer<T> {
    inner: Arc<SyncRingBuf<T>>,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Producer<T> {
    pub fn try_push(&mut self, t: T) -> Option<T> {
        self.inner.try_push(t)
    }
}

pub struct Consumer<T> {
    inner: Arc<SyncRingBuf<T>>,
}

unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub fn try_pop(&mut self) -> Option<T> {
        self.inner.try_pop()
    }
}

pub fn with_capacity_at_least<T>(cap_at_least: usize) -> (Producer<T>, Consumer<T>) {
    let rb = Arc::new(SyncRingBuf::with_capacity_at_least(cap_at_least));
    let p = Producer { inner: rb.clone() };
    let c = Consumer { inner: rb };
    (p, c)
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

    #[test]
    fn two_threaded() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        let jh = std::thread::spawn(move || {
            for i in 0..n {
                while let Some(_) = p.try_push(i) {}
            }
        });

        for i in 0..n {
            let res = loop {
                if let Some(res) = c.try_pop() {
                    break res;
                }
            };
            assert_eq!(res, i);
        }

        jh.join().unwrap();
    }

    #[test]
    fn fixed_producer() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        std::thread::spawn(move || {
            for i in 0..n {
                while let Some(_) = p.try_push(i) {}
            }
        });

        let total_threads = 4;
        let count = n / total_threads;

        let closure = move |tid: i32, c: &mut Consumer<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                let res = loop {
                    if let Some(res) = c.try_pop() {
                        break res;
                    }
                };
                assert_eq!(res, i);
            }
        };

        let jh = std::thread::spawn(move || {
            closure(0, &mut c);
            let jh = std::thread::spawn(move || {
                closure(1, &mut c);
                let jh = std::thread::spawn(move || {
                    closure(2, &mut c);
                    let jh = std::thread::spawn(move || {
                        closure(3, &mut c);
                    });
                    jh.join().unwrap();
                });
                jh.join().unwrap();
            });
            jh.join().unwrap();
        });

        jh.join().unwrap();
    }
}
