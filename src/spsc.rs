#![feature(alloc)]

use std::{
    intrinsics::drop_in_place,
    sync::atomic::{AtomicUsize, Ordering},
};

use std::sync::Arc;

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
            inner: AtomicUsize::new(n),
        }
    }
}

unsafe impl<T: Sync> Sync for SyncRingBuf<T> {}
unsafe impl<T: Send> Send for SyncRingBuf<T> {}

impl<T> SyncRingBuf<T> {
    fn with_capacity(mut cap: usize) -> Self {
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

    fn try_push(&self, t: T) -> Option<T> {
        let curr_write_idx = self.write_idx.inner.load(Ordering::Relaxed);

        let mut next_write_idx = curr_write_idx + 1;
        if next_write_idx >= self.cap {
            next_write_idx = 0;
        }

        if next_write_idx == self.read_idx.inner.load(Ordering::Acquire) {
            Some(t)
        } else {
            unsafe {
                std::ptr::write(self.buf.add(curr_write_idx), t);
            }
            self.write_idx
                .inner
                .store(next_write_idx, Ordering::Release);
            None
        }
    }

    fn try_pop(&self) -> Option<T> {
        let curr_read_idx = self.read_idx.inner.load(Ordering::Relaxed);

        if curr_read_idx == self.write_idx.inner.load(Ordering::Acquire) {
            None
        } else {
            let t = unsafe { std::ptr::read(self.buf.add(curr_read_idx) as *const T) };

            let mut next_read_idx = curr_read_idx + 1;
            if next_read_idx >= self.cap {
                next_read_idx = 0
            }

            self.read_idx.inner.store(next_read_idx, Ordering::Release);
            Some(t)
        }
    }
}

impl<T> Drop for SyncRingBuf<T> {
    fn drop(&mut self) {
        let curr_read_idx = self.read_idx.inner.load(Ordering::Relaxed);
        let curr_write_idx = self.write_idx.inner.load(Ordering::Relaxed);

        if curr_read_idx <= curr_write_idx {
            for i in curr_read_idx..curr_write_idx {
                unsafe {
                    drop_in_place(self.buf.add(i));
                }
            }
        } else {
            for i in curr_read_idx..self.cap {
                unsafe {
                    drop_in_place(self.buf.add(i));
                }
            }
            for i in 0..curr_write_idx {
                unsafe {
                    drop_in_place(self.buf.add(i));
                }
            }
        }

        unsafe {
            Vec::from_raw_parts(self.buf, 0, self.cap);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, mem::size_of, rc::Rc};
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref RB: SyncRingBuf<i32> = {
            let rb = SyncRingBuf::with_capacity(100);
            rb
        };
    }

    #[test]
    fn cache_aligned() {
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

    #[test]
    fn push_pop() {
        let rb = SyncRingBuf::<i32>::with_capacity(5);
        assert_eq!(rb.try_pop(), None);

        assert_eq!(rb.try_push(0), None);
        assert_eq!(rb.try_pop(), Some(0));

        assert_eq!(rb.try_push(1), None);
        assert_eq!(rb.try_pop(), Some(1));

        assert_eq!(rb.try_push(2), None);
        assert_eq!(rb.try_pop(), Some(2));

        for i in 0..5 {
            assert_eq!(rb.try_push(i), None);
        }
        assert_eq!(rb.try_push(5), Some(5));
        assert_eq!(rb.try_push(6), Some(6));

        for i in 0..5 {
            assert_eq!(rb.try_pop(), Some(i));
        }
        assert_eq!(rb.try_pop(), None);
    }

    #[test]
    fn drop_test() {
        struct Tester {
            common: Rc<RefCell<i32>>,
        }

        impl Tester {
            fn new(common: Rc<RefCell<i32>>) -> Self {
                *common.borrow_mut() += 1;
                Self { common }
            }
        }

        impl Drop for Tester {
            fn drop(&mut self) {
                *self.common.borrow_mut() -= 1;
            }
        }

        let common = Rc::new(RefCell::new(0));
        let rb = SyncRingBuf::with_capacity(50);
        for _ in 0..40 {
            rb.try_push(Tester::new(common.clone()));
        }
        for _ in 0..10 {
            rb.try_pop();
        }
        assert_eq!(*common.borrow(), 30);
        drop(rb);
        assert_eq!(*common.borrow(), 0);

        let rb = SyncRingBuf::with_capacity(50);
        for _ in 0..40 {
            rb.try_push(Tester::new(common.clone()));
        }
        for _ in 0..20 {
            rb.try_pop();
        }
        for _ in 0..20 {
            rb.try_push(Tester::new(common.clone()));
        }
        assert_eq!(*common.borrow(), 40);
        drop(rb);
        assert_eq!(*common.borrow(), 0);
    }

    #[test]
    fn test_send() {
        let rb = SyncRingBuf::<i32>::with_capacity(10);
        for i in 0..10 {
            rb.try_push(i);
        }
        let tj = std::thread::spawn(move || {
            let new_rb = rb;
            assert_eq!(new_rb.try_pop(), Some(0));
        });

        assert_eq!(tj.join().is_ok(), true);
    }

    #[test]
    fn test_sync() {
        let rb_ref = &RB;
        let n = 10000000;

        let tj = std::thread::spawn(move ||{
            let mut counter = 0;
            loop {
                if let Some(v) = rb_ref.try_pop() {
                    assert_eq!(v, counter);
                    counter += 1;
                }

                if counter == n {
                    break;
                }
            }
        });

        for i in 0..n {
            while let Some(_) = RB.try_push(i) {}
        }

        assert_eq!(tj.join().is_ok(), true);
    }
}
