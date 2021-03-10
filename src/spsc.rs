use std::{
    intrinsics::drop_in_place,
    sync::atomic::{AtomicUsize, Ordering},
};

use std::sync::Arc;

#[repr(C)]
struct SyncRingBuf<T> {
    read_idx: CacheLine,
    write_idx: CacheLine,
    buf: *mut T,
    cap: usize,
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
        let curr_read_idx = self.read_idx.inner.load(Ordering::Acquire);
        let curr_write_idx = self.write_idx.inner.load(Ordering::Acquire);

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

pub fn with_capacity<T>(cap: usize) -> (Producer<T>, Consumer<T>) {
    let rb = Arc::new(SyncRingBuf::with_capacity(cap));
    let p = Producer { inner: rb.clone() };
    let c = Consumer { inner: rb };
    (p, c)
}

// #[cfg(test)]
// #[ignore]
// mod tests {
//     use lazy_static::lazy_static;
//     use std::{cell::RefCell, mem::size_of, rc::Rc};

//     use super::*;

//     lazy_static! {
//         static ref RB: SyncRingBuf<i32> = {
//             let rb = SyncRingBuf::with_capacity(500);
//             rb
//         };
//     }

//     #[test]
//     fn cache_aligned() {
//         assert_eq!(size_of::<SyncRingBuf<i32>>(), 192);
//         assert_eq!(size_of::<CacheLine>(), 64);
//         let mut v = 5;
//         let empty_val = SyncRingBuf {
//             buf: &mut v as *mut i32,
//             cap: 0,
//             // mask: 0,
//             read_idx: CacheLine {
//                 inner: AtomicUsize::new(0),
//             },
//             write_idx: CacheLine {
//                 inner: AtomicUsize::new(0),
//             },
//         };

//         let empty_val_addr = &empty_val as *const SyncRingBuf<i32> as usize;
//         let read_index_addr = &empty_val.read_idx as *const CacheLine as usize;
//         let write_index_addr = &empty_val.write_idx as *const CacheLine as usize;
//         let buf_addr = &empty_val.buf as *const *mut i32 as usize;
//         let cap_addr = &empty_val.cap as *const usize as usize;
//         // let mask_addr = &empty_val.mask as *const usize as usize;

//         assert_eq!(buf_addr - empty_val_addr, 0);
//         assert_eq!(cap_addr - empty_val_addr, 8);
//         // assert_eq!(mask_addr - empty_val_addr, 16);
//         assert_eq!(read_index_addr - empty_val_addr, 64);
//         assert_eq!(write_index_addr - empty_val_addr, 128);
//     }

//     #[test]
//     fn single_thread_push_pop() {
//         let (mut sender, mut receiver) = with_capacity(4);
//         assert_eq!(receiver.try_pop(), None);

//         assert_eq!(sender.try_push(0), None);
//         assert_eq!(receiver.try_pop(), Some(0));

//         assert_eq!(sender.try_push(1), None);
//         assert_eq!(receiver.try_pop(), Some(1));

//         assert_eq!(sender.try_push(2), None);
//         assert_eq!(receiver.try_pop(), Some(2));

//         for i in 0..7 {
//             assert_eq!(sender.try_push(i), None);
//         }
//         assert_eq!(sender.try_push(7), Some(7));
//         assert_eq!(sender.try_push(8), Some(8));

//         for i in 0..7 {
//             assert_eq!(receiver.try_pop(), Some(i));
//         }
//         assert_eq!(receiver.try_pop(), None);
//     }

//     #[test]
//     fn single_thread_drop() {
//         struct Tester {
//             common: Rc<RefCell<i32>>,
//         }

//         impl Tester {
//             fn new(common: Rc<RefCell<i32>>) -> Self {
//                 *common.borrow_mut() += 1;
//                 Self { common }
//             }
//         }

//         impl Drop for Tester {
//             fn drop(&mut self) {
//                 *self.common.borrow_mut() -= 1;
//             }
//         }

//         let common = Rc::new(RefCell::new(0));
//         let (mut sender, mut receiver) = with_capacity(50);
//         for _ in 0..50 {
//             sender.try_push(Tester::new(common.clone()));
//         }
//         for _ in 0..10 {
//             receiver.try_pop();
//         }
//         assert_eq!(*common.borrow(), 40);

//         drop(sender);
//         drop(receiver);
//         assert_eq!(*common.borrow(), 0);

//         let (mut sender, mut receiver) = with_capacity(50);
//         for _ in 0..50 {
//             sender.try_push(Tester::new(common.clone()));
//         }
//         for _ in 0..20 {
//             receiver.try_pop();
//         }
//         for _ in 0..25 {
//             sender.try_push(Tester::new(common.clone()));
//         }
//         assert_eq!(*common.borrow(), 55);

//         drop(sender);
//         drop(receiver);
//         assert_eq!(*common.borrow(), 0);
//     }

//     // #[test]
//     // fn test_send() {
//     //     // SyncRingBuf is not inherently sendable because it contains
//     //     // a *mut T
//     //     let rb = SyncRingBuf::<i32>::with_capacity(10);
//     //     for i in 0..10 {
//     //         rb.try_push(i);
//     //     }
//     //     let tj = std::thread::spawn(move || {
//     //         let new_rb = rb;
//     //         assert_eq!(new_rb.try_pop(), Some(0));
//     //     });

//     //     assert_eq!(tj.join().is_ok(), true);
//     // }

//     #[test]
//     fn test_sync() {
//         let rb_ref = &RB;
//         let n = 10000000;

//         let _tj = std::thread::spawn(move || {
//             let mut counter = 0;
//             loop {
//                 if let Some(v) = rb_ref.try_pop() {
//                     assert_eq!(v, counter);
//                     counter += 1;

//                     if counter == n {
//                         break;
//                     }
//                 }
//             }
//         });

//         for i in 0..n {
//             while let Some(_) = RB.try_push(i) {}
//         }

//         // assert_eq!(tj.join().is_ok(), true);
//     }

//     #[test]
//     fn test_threaded() {
//         let (mut p, mut c) = with_capacity(10);
//         let n = 1000;
//         std::thread::spawn(move || {
//             for i in 0..n {
//                 while let None = p.try_push(i) {}
//             }
//         });

//         for i in 0..n {
//             while let Some(t) = c.try_pop() {
//                 assert_eq!(t, i);
//                 break
//             }
//         }
//     }
// }
