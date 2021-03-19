use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CACHELINE_SIZE: usize = 64;
const POINTER_SIZE: usize = 8;

macro_rules! total_size {
    ( $t: ty $(, $ts: ty)* $(,)? ) => {
          std::mem::size_of::<$t>()
          $( + std::mem::size_of::<$ts>() )*
    }
}

#[repr(C)]
struct SyncRingBuf<T> {
    // first cacheline
    buf: *mut T,
    buf_len: usize,
    cap: usize,
    padding1: [u8; CACHELINE_SIZE - total_size!(usize, usize) - POINTER_SIZE],
    // second cacheline
    read_idx: AtomicUsize,
    local_write_idx: Cell<usize>,
    padding2: [u8; CACHELINE_SIZE - total_size!(AtomicUsize, Cell<usize>)],
    // third cacheline
    write_idx: AtomicUsize,
    local_read_idx: Cell<usize>,
    padding3: [u8; CACHELINE_SIZE - total_size!(AtomicUsize, Cell<usize>)],
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
            padding1: [0; 40],
            // second cacheline
            read_idx: AtomicUsize::new(0),
            local_write_idx: Cell::new(0),
            padding2: [0; 48],
            // third cacheline
            write_idx: AtomicUsize::new(0),
            local_read_idx: Cell::new(0),
            padding3: [0; 48],
        }
    }

    fn try_send(&self, t: T) -> Option<T> {
        let curr_write_idx = self.write_idx.load(Ordering::Relaxed);
        let next_write_idx = (curr_write_idx + 1) & self.cap;

        if next_write_idx == self.local_read_idx.get() {
            self.local_read_idx
                .set(self.read_idx.load(Ordering::Relaxed));
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

    unsafe fn send_batch(&self, batch_ptr: *const T, batch_len: usize) -> usize {
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

    fn try_recv(&self) -> Option<T> {
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

    unsafe fn recv_batch(&self, batch_ptr: *mut T, batch_cap: usize) -> usize {
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

    #[inline]
    fn capacity(&self) -> usize {
        self.cap
    }
}

impl<T> Drop for SyncRingBuf<T> {
    fn drop(&mut self) {
        while let Some(_) = self.try_recv() {}

        unsafe {
            Vec::from_raw_parts(self.buf, 0, self.buf_len);
        }
    }
}

pub struct Sender<T> {
    inner: Arc<SyncRingBuf<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}

impl<T> Sender<T> {
    pub fn try_send(&mut self, t: T) -> Option<T> {
        self.inner.try_send(t)
    }

    pub fn remaining_at_least(&self) -> usize {
        self.inner.remaining_at_least()
    }

    pub fn send_batch(&mut self, batch: &mut Vec<T>) -> usize {
        let n_pushed = unsafe { self.inner.send_batch(batch.as_ptr(), batch.len()) };
        if n_pushed == 0 {
            return 0;
        }

        unsafe {
            if n_pushed < batch.len() {
                let src = batch.as_ptr().add(n_pushed);
                let dst = batch.as_mut_ptr();
                std::ptr::copy(src, dst, batch.len() - n_pushed);
            }
            batch.set_len(batch.len() - n_pushed);
        }

        n_pushed
    }

    pub fn send_batch_slow(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_size = std::cmp::min(self.remaining_at_least(), batch.len());
        let drain = batch.drain(0..batch_size);
        for t in drain {
            assert!(self.try_send(t).is_none() == true, "this should not happen");
        }
        batch_size
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

pub struct Receiver<T> {
    inner: Arc<SyncRingBuf<T>>,
}

unsafe impl<T: Send> Send for Receiver<T> {}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self) -> Option<T> {
        self.inner.try_recv()
    }

    pub fn len_at_least(&self) -> usize {
        self.inner.len_at_least()
    }

    pub fn recv_batch(&mut self, batch: &mut Vec<T>) -> usize {
        let n_popped = unsafe {
            self.inner.recv_batch(
                batch.as_mut_ptr().add(batch.len()),
                batch.capacity() - batch.len(),
            )
        };

        if n_popped == 0 {
            0
        } else {
            unsafe {
                batch.set_len(batch.len() + n_popped);
            }
            n_popped
        }
    }

    pub fn recv_batch_slow(&mut self, batch: &mut Vec<T>) -> usize {
        let batch_size = std::cmp::min(self.len_at_least(), batch.capacity() - batch.len());
        for _ in 0..batch_size {
            let t = self.try_recv().expect("this should not happen");
            batch.push(t);
        }
        batch_size
    }
}

pub fn with_capacity_at_least<T>(cap_at_least: usize) -> (Sender<T>, Receiver<T>) {
    let rb = Arc::new(SyncRingBuf::with_capacity_at_least(cap_at_least));
    let p = Sender { inner: rb.clone() };
    let c = Receiver { inner: rb };
    (p, c)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, mem::size_of, rc::Rc};

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
    fn capacity() {
        let b = SyncRingBuf::<i32>::with_capacity_at_least(2);
        assert_eq!(b.capacity(), 3);

        let b = SyncRingBuf::<i32>::with_capacity_at_least(512);
        assert_eq!(b.capacity(), 1023);
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
    fn send<T>(s: &mut Sender<T>, mut i: T) {
        while let Some(res) = s.try_send(i) {
            i = res;
        }
    }

    #[inline]
    fn recv<T>(r: &mut Receiver<T>) -> T {
        loop {
            if let Some(res) = r.try_recv() {
                break res;
            }
        }
    }

    #[test]
    fn two_threads() {
        let (mut s, mut r) = with_capacity_at_least(500);
        let n = 10000000;
        let jh = std::thread::spawn(move || {
            for i in 0..n {
                send(&mut s, i);
            }
        });

        for i in 0..n {
            let res = recv(&mut r);
            assert_eq!(res, i);
        }

        jh.join().unwrap();
    }

    #[test]
    fn send_around() {
        let (mut s, mut r) = with_capacity_at_least(500);
        let n = 10000000;

        let r_total_threads = 4;
        let count = n / r_total_threads;

        let closure = move |tid: i32, r: &mut Receiver<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                let res = recv(r);
                assert_eq!(res, i);
            }
        };

        let r_jh = std::thread::spawn(move || {
            closure(0, &mut r);
            let jh = std::thread::spawn(move || {
                closure(1, &mut r);
                let jh = std::thread::spawn(move || {
                    closure(2, &mut r);
                    let jh = std::thread::spawn(move || {
                        closure(3, &mut r);
                    });
                    jh.join().unwrap();
                });
                jh.join().unwrap();
            });
            jh.join().unwrap();
        });

        let s_total_threads = 5;
        let count = n / s_total_threads;

        let closure = move |tid: i32, s: &mut Sender<i32>| {
            for i in (tid * count)..((tid + 1) * count) {
                send(s, i);
            }
        };

        let s_jh = std::thread::spawn(move || {
            closure(0, &mut s);
            let jh = std::thread::spawn(move || {
                closure(1, &mut s);
                let jh = std::thread::spawn(move || {
                    closure(2, &mut s);
                    let jh = std::thread::spawn(move || {
                        closure(3, &mut s);
                        let jh = std::thread::spawn(move || {
                            closure(4, &mut s);
                        });
                        jh.join().unwrap();
                    });
                    jh.join().unwrap();
                });
                jh.join().unwrap();
            });
            jh.join().unwrap();
        });

        r_jh.join().unwrap();
        s_jh.join().unwrap();
    }

    #[test]
    fn batched_two_threads() {
        let (mut s, mut r) = with_capacity_at_least(500);
        let n = 10000000;

        let jh = std::thread::spawn(move || {
            let mut v = Vec::with_capacity(32);
            let mut counter = 0;
            while counter < n {
                if v.len() < 32 {
                    v.push(counter);
                    counter += 1;
                } else {
                    s.send_batch(&mut v);
                }
            }
            while v.len() > 0 {
                s.send_batch(&mut v);
            }
        });

        let mut v = Vec::with_capacity(32);
        let mut counter = 0;
        loop {
            let n_popped = r.recv_batch(&mut v);
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
    fn batched_small_queue() {
        let (mut s, mut r) = with_capacity_at_least(10);

        let jh = std::thread::spawn(move || {
            let mut v = Vec::<i32>::with_capacity(500);
            let mut curr_len = v.len();
            while curr_len < 500 {
                let n_recved = r.recv_batch(&mut v);
                assert_eq!(curr_len + n_recved, v.len());
                curr_len = v.len();
            }
            for i in 0..500 {
                assert_eq!(i as i32, v[i]);
            }
        });

        let mut v = Vec::with_capacity(500);
        for i in 0..500 {
            v.push(i);
        }

        let mut curr_len = v.len();
        while curr_len > 0 {
            let n_send = s.send_batch(&mut v);
            assert_eq!(n_send + v.len(), curr_len);
            curr_len = v.len();
        }

        jh.join().unwrap();
    }

    #[test]
    fn drop() {
        struct Share {
            inner: Rc<RefCell<i32>>,
        }

        impl Share {
            fn new(share: Rc<RefCell<i32>>) -> Self {
                *share.borrow_mut() += 1;
                Self { inner: share }
            }
        }

        impl Drop for Share {
            fn drop(&mut self) {
                *self.inner.borrow_mut() -= 1;
            }
        }

        let share = Rc::new(RefCell::new(0));
        let (mut s, mut r) = with_capacity_at_least(5);

        assert_eq!(s.remaining_at_least(), 7);
        assert_eq!(r.len_at_least(), 0);

        s.try_send(Share::new(share.clone()));
        s.try_send(Share::new(share.clone()));
        s.try_send(Share::new(share.clone()));
        s.try_send(Share::new(share.clone()));
        s.try_send(Share::new(share.clone()));
        assert_eq!(*share.borrow(), 5);

        r.try_recv();
        r.try_recv();
        assert_eq!(*share.borrow(), 3);

        let mut v = Vec::with_capacity(5);
        for _ in 0..5 {
            v.push(Share::new(share.clone()));
        }
        assert_eq!(*share.borrow(), 8);

        let n_send = s.send_batch(&mut v);
        assert_eq!(n_send, 4);
        assert_eq!(v.len(), 1);
        assert_eq!(*share.borrow(), 8);

        let n_recv = r.recv_batch(&mut v);
        assert_eq!(n_recv, 4);
        assert_eq!(v.len(), 5);
        assert_eq!(*share.borrow(), 8);

        v.clear();
        assert_eq!(*share.borrow(), 3);

        let n_recv = r.recv_batch(&mut v);
        assert_eq!(n_recv, 3);

        let n_send = s.send_batch(&mut v);
        assert_eq!(n_send, 3);

        assert_eq!(*share.borrow(), 3);
        std::mem::drop(r);
        std::mem::drop(s);
        assert_eq!(*share.borrow(), 0);
    }

    #[test]
    fn zero_sized_batch() {
        let (mut s, mut r) = with_capacity_at_least(2);
        let n = 100;

        let jh = std::thread::spawn(move || {
            let mut v = Vec::new();
            for _ in 0..n {
                assert_eq!(s.send_batch(&mut v), 0);
            }
            send(&mut s, 5);
            send(&mut s, 6);
        });

        let mut v = Vec::with_capacity(2);
        v.push(5);
        v.push(6);
        for _ in 0..n {
            assert_eq!(r.recv_batch(&mut v), 0);
        }
        assert_eq!(recv(&mut r), 5);
        assert_eq!(recv(&mut r), 6);
        
        jh.join().unwrap();
    }

    #[test]
    fn zero_sized_object() {
        let (mut s, mut r) = with_capacity_at_least::<()>(500);
        let n = 10000000;
        let jh = std::thread::spawn(move || {
            for _ in 0..n {
                send(&mut s, ());
            }
        });

        for _ in 0..n {
            let res = recv(&mut r);
            assert_eq!(res, ());
        }

        jh.join().unwrap();
        assert_eq!(r.try_recv(), None);

        let (mut s, mut r) = with_capacity_at_least::<()>(500);
        let n = 10000000;

        let jh = std::thread::spawn(move || {
            let mut v = Vec::with_capacity(32);
            let mut counter = 0;
            while counter < n {
                if v.len() < 32 {
                    v.push(());
                    counter += 1;
                } else {
                    s.send_batch(&mut v);
                }
            }
            while v.len() > 0 {
                s.send_batch(&mut v);
            }
        });

        let mut v = Vec::with_capacity(32);
        let mut counter = 0;
        loop {
            let n_popped = r.recv_batch(&mut v);
            for i in v.drain(0..n_popped) {
                assert_eq!(i, ());
                counter += 1;
            }
            if counter == n {
                break;
            }
        }

        jh.join().unwrap();
    }
}
