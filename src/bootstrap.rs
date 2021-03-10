// A regular ring buffer implentation to boot strap a SPSC ring buffer
pub struct RingBuf<T> {
    buf: *mut T, // what's the different
    cap: usize,
    write_idx: usize,
    read_idx: usize,
}

unsafe impl<T: Send> Send for RingBuf<T> {}

impl<T> RingBuf<T> {
    pub fn new(mut cap: usize) -> Self {
        assert!(cap > 0 && cap < usize::MAX, "invalid capacity");

        // the size of the alloated buffer should be one element
        // larger than the input capacity
        cap += 1;

        // allocate the buffer via a vector
        let mut v = Vec::with_capacity(cap);
        let buf = v.as_mut_ptr();
        std::mem::forget(v);

        Self {
            buf,
            cap,
            write_idx: 0,
            read_idx: 0,
        }
    }

    pub fn len(&self) -> usize {
        let curr_read_idx = self.read_idx;
        let curr_write_idx = self.write_idx;

        if curr_read_idx <= curr_write_idx {
            curr_write_idx - curr_read_idx
        } else {
            self.cap - curr_read_idx + curr_write_idx
        }
    }

    pub fn max_cap(&self) -> usize {
        self.cap - 1
    }

    pub fn push(&mut self, t: T) -> Option<T> {
        // load the write index
        let curr_write_idx = self.write_idx;

        // compute the next write index
        let mut next_write_idx = curr_write_idx + 1;
        if next_write_idx >= self.cap {
            next_write_idx = 0;
        }

        // load up the read index
        let curr_read_idx = self.read_idx;

        // do the actual push
        if next_write_idx == curr_read_idx {
            Some(t)
        } else {
            unsafe {
                std::ptr::write(self.buf.add(self.write_idx), t);
            }
            self.write_idx = next_write_idx;
            None
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len() == 0 {
            None
        } else {
            // load the element from the buffer
            let curr_read_idx = self.read_idx;
            let t = unsafe { std::ptr::read(self.buf.add(curr_read_idx) as *const T) };

            // compute the next read index
            let mut next_read_idx = curr_read_idx + 1;
            if next_read_idx >= self.cap {
                next_read_idx = 0
            }

            // adjust the read index
            self.read_idx = next_read_idx;

            Some(t)
        }
    }
}

impl<T> Drop for RingBuf<T> {
    fn drop(&mut self) {
        for _ in 0..self.len() {
            self.pop();
        }

        unsafe {
            // restore the vector that generate the buffer
            Vec::from_raw_parts(self.buf, 0, self.cap);
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{cell::RefCell, rc::Rc};

//     use super::*;

//     #[test]
//     fn push_pop() {
//         let mut rb = RingBuf::<i32>::new(5);
//         assert_eq!(rb.pop(), None);

//         assert_eq!(rb.push(0), None);
//         assert_eq!(rb.pop(), Some(0));

//         assert_eq!(rb.push(1), None);
//         assert_eq!(rb.pop(), Some(1));

//         assert_eq!(rb.push(2), None);
//         assert_eq!(rb.pop(), Some(2));

//         assert_eq!(rb.len(), 0);

//         for i in 0..5 {
//             assert_eq!(rb.push(i), None);
//             assert_eq!(rb.len(), i as usize + 1);
//         }
//         assert_eq!(rb.push(5), Some(5));
//         assert_eq!(rb.push(6), Some(6));

//         for i in 0..5 {
//             assert_eq!(rb.pop(), Some(i));
//             assert_eq!(rb.len(), 4 - i as usize);
//         }
//         assert_eq!(rb.pop(), None);
//     }

//     #[test]
//     fn drop_test() {
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
//         let mut rb = RingBuf::new(100);
//         for _ in 0..100 {
//             rb.push(Tester::new(common.clone()));
//         }
//         assert_eq!(*common.borrow(), 100);
//         rb.pop();
//         assert_eq!(*common.borrow(), 99);
//         drop(rb);
//         assert_eq!(*common.borrow(), 0);
//     }
// }
