use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

    #[inline]
    fn remaining_at_least(&self) -> usize {
        let curr_write_idx = self.write_idx.load(Ordering::Relaxed);
        let vacant_size = self.vacant_write_size(curr_write_idx);
        if vacant_size == 0 {
            self.local_read_idx
                .set(self.read_idx.load(Ordering::Acquire));
            self.vacant_write_size(curr_write_idx)
        } else {
            vacant_size
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
    fn len_at_least(&self) -> usize {
        let curr_read_idx = self.read_idx.load(Ordering::Relaxed);
        let available_size = self.available_read_size(curr_read_idx);
        if available_size == 0 {
            self.local_write_idx
                .set(self.write_idx.load(Ordering::Acquire));
            self.available_read_size(curr_read_idx)
        } else {
            available_size
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

    #[inline]
    fn vacant_write_size(&self, curr_write_idx: usize) -> usize {
        let local_read_idx = self.local_read_idx.get();

        if local_read_idx <= curr_write_idx {
            self.cap - curr_write_idx + local_read_idx
        } else {
            local_read_idx - curr_write_idx - 1
        }
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

    pub fn remaining_at_least(&self) -> usize {
        self.inner.remaining_at_least()
    }

    pub fn push_batch(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_len = batch.len();
        let n_pushed = unsafe { self.inner.push_batch(batch.as_ptr(), batch_len) };
        if n_pushed == 0 {
            return 0;
        }

        unsafe {
            if n_pushed < batch_len {
                let src = batch.as_ptr().add(n_pushed);
                let dst = batch.as_mut_ptr();
                std::ptr::copy(src, dst, batch_len - n_pushed);
            }
            batch.set_len(batch_len - n_pushed);
        }

        n_pushed
    }

    pub fn push_batch_slow(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_size = std::cmp::min(self.remaining_at_least(), batch.len());
        let drain = batch.drain(0..batch_size);
        for t in drain {
            assert!(self.try_push(t).is_none() == true, "this should not happen");
        }
        batch_size
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

    pub fn len_at_least(&self) -> usize {
        self.inner.len_at_least()
    }

    pub fn pop_batch(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_len = batch.len();
        let batch_cap = batch.capacity();
        let n_popped = unsafe {
            self.inner
                .pop_batch(batch.as_mut_ptr().add(batch_len), batch_cap - batch_len)
        };

        if n_popped == 0 {
            0
        } else {
            unsafe {
                batch.set_len(batch_len + n_popped);
            }
            n_popped
        }
    }

    pub fn pop_batch_slow(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_size = std::cmp::min(self.len_at_least(), batch.capacity() - batch.len());
        for _ in 0..batch_size {
            let t = self.try_pop().expect("this should not happen");
            batch.push(t);
        }
        batch_size
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
    use std::{convert::TryInto, mem::size_of};

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

    #[inline]
    fn push(p: &mut Producer<i32>, i: i32) {
        while let Some(_) = p.try_push(i) {}
    }

    #[inline]
    fn pop(c: &mut Consumer<i32>) -> i32 {
        loop {
            if let Some(res) = c.try_pop() {
                break res;
            }
        }
    }

    #[test]
    fn two_threaded() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 100000000;
        let jh = std::thread::spawn(move || {
            for i in 0..n {
                push(&mut p, i);
            }
        });

        for i in 0..n {
            let res = pop(&mut c);
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
                push(&mut p, i);
            }
        });

        let total_threads = 4;
        let count = n / total_threads;

        let closure = move |tid: i32, c: &mut Consumer<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                let res = pop(c);
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

    #[test]
    fn fixed_consumer() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        std::thread::spawn(move || {
            for i in 0..n {
                let res = pop(&mut c);
                assert_eq!(res, i);
            }
        });

        let total_threads = 4;
        let count = n / total_threads;

        let closure = move |tid: i32, p: &mut Producer<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                push(p, i);
            }
        };

        let jh = std::thread::spawn(move || {
            closure(0, &mut p);
            let jh = std::thread::spawn(move || {
                closure(1, &mut p);
                let jh = std::thread::spawn(move || {
                    closure(2, &mut p);
                    let jh = std::thread::spawn(move || {
                        closure(3, &mut p);
                    });
                    jh.join().unwrap();
                });
                jh.join().unwrap();
            });
            jh.join().unwrap();
        });

        jh.join().unwrap();
    }

    #[test]
    fn fix_nothing() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;

        let c_total_threads = 4;
        let count = n / c_total_threads;

        let closure = move |tid: i32, c: &mut Consumer<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                let res = pop(c);
                assert_eq!(res, i);
            }
        };

        let c_jh = std::thread::spawn(move || {
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

        let p_total_threads = 5;
        let count = n / p_total_threads;

        let closure = move |tid: i32, p: &mut Producer<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                push(p, i);
            }
        };

        let p_jh = std::thread::spawn(move || {
            closure(0, &mut p);
            let jh = std::thread::spawn(move || {
                closure(1, &mut p);
                let jh = std::thread::spawn(move || {
                    closure(2, &mut p);
                    let jh = std::thread::spawn(move || {
                        closure(3, &mut p);
                        let jh = std::thread::spawn(move || {
                            closure(4, &mut p);
                        });
                        jh.join().unwrap();
                    });
                    jh.join().unwrap();
                });
                jh.join().unwrap();
            });
            jh.join().unwrap();
        });

        p_jh.join().unwrap();
        c_jh.join().unwrap();
    }

    #[test]
    fn batched_pop() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        let jh = std::thread::spawn(move || {
            for i in 0..n {
                push(&mut p, i);
            }
        });

        let mut v = Vec::with_capacity(32);
        let mut counter = 0;
        loop {
            let n_popped = c.pop_batch(&mut v);
            for i in v.drain(0..n_popped) {
                assert_eq!(i, counter);
                counter += 1;
            }
            if counter == n {
                break;
            }
        }
        jh.join().unwrap();
    }

    #[test]
    fn batched_push() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 10000000;
        let jh = std::thread::spawn(move || {
            let mut v = Vec::with_capacity(32);
            let mut counter = 0;
            while counter < n {
                if v.len() < 32 {
                    v.push(counter);
                    counter += 1;
                } else {
                    p.push_batch(&mut v);
                }
            }
            while v.len() > 0 {
                p.push_batch(&mut v);
            }
        });

        for i in 0..n {
            let res = pop(&mut c);
            assert_eq!(res, i);
        }

        jh.join().unwrap();
    }

    #[test]
    fn batched_push_pop() {
        let (mut p, mut c) = with_capacity_at_least(500);
        let n = 100000000;

        let jh = std::thread::spawn(move || {
            let mut v = Vec::with_capacity(32);
            let mut counter = 0;
            while counter < n {
                if v.len() < 32 {
                    v.push(counter);
                    counter += 1;
                } else {
                    p.push_batch(&mut v);
                }
            }
            while v.len() > 0 {
                p.push_batch(&mut v);
            }
        });

        let mut v = Vec::with_capacity(32);
        let mut counter = 0;
        loop {
            let n_popped = c.pop_batch(&mut v);
            for i in v.drain(0..n_popped) {
                assert_eq!(i, counter);
                counter += 1;
            }
            if counter == n {
                break;
            }
        }
        jh.join().unwrap();
    }
}
