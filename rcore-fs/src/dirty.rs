use core::fmt::{Debug, Error, Formatter};
use core::ops::{Deref, DerefMut};

/// Dirty wraps a value of type T with functions similiar to that of a Read/Write
/// lock but simply sets a dirty flag on write(), reset on read()
pub struct Dirty<T> {
    value: T,
    dirty: bool,
    stale: bool,
}

impl<T> Dirty<T> {
    /// Create a new Dirty
    pub fn new(val: T) -> Dirty<T> {
        Dirty {
            value: val,
            dirty: false, // dirty: this block needs to be written back to disk
            stale: false, // stale: this block is modified. We should allocate a new block for it.
        }
    }

    /// Create a new Dirty with dirty set
    pub fn new_dirty(val: T) -> Dirty<T> {
        Dirty {
            value: val,
            dirty: true,
            stale: false,
        }
    }

    /// Returns true if dirty, false otherwise
    #[allow(dead_code)]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn stale(&self) -> bool {
        self.stale
    }

    pub fn turn_stale(&mut self) {
        self.stale = true;
    }

    /// Reset dirty
    pub fn sync(&mut self) {
        self.dirty = false;
        self.stale = false;
    }
}

impl<T> Deref for Dirty<T> {
    type Target = T;

    /// Read the value
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for Dirty<T> {
    /// Writable value return, sets the dirty flag
    fn deref_mut(&mut self) -> &mut T {
        self.dirty = true;
        &mut self.value
    }
}

impl<T> Drop for Dirty<T> {
    /// Guard it is not dirty when dropping
    fn drop(&mut self) {
        assert!(!self.dirty, "data dirty when dropping");
    }
}

impl<T: Debug> Debug for Dirty<T> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let tag = if self.dirty { "Dirty" } else { "Clean" };
        write!(f, "[{}] {:?}", tag, self.value)
    }
}
