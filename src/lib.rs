use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

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
        (*self.inner).try_push(t)
    }
}

pub struct Consumer<T> {
    inner: Arc<SyncRingBuf<T>>,
}

unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    pub fn try_pop(&mut self) -> Option<T> {
        (*self.inner).try_pop()
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
    use std::{mem::size_of, thread::spawn};

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
    fn test_threaded() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        std::thread::spawn(move || {
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
            let jh = std::thread::spawn(move|| {
                closure(1, &mut c);
                let jh = std::thread::spawn(move|| {
                    closure(2, &mut c);
                    let jh = std::thread::spawn(move|| {
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
