// We put these things in submodule in case we need to change the impls easily
pub use self::parking_lot::*;

mod parking_lot {
    use std::sync::{LockResult, PoisonError, TryLockError, TryLockResult};

    // We just reexport std type as parking_lot doesn't have any improved version of these
    pub use std::sync::atomic;
    pub use std::sync::Arc;
    // TODO: maybe use crossbeam_channel?
    pub use std::sync::mpsc;

    // renaming allows us to switch impl if needed
    use parking_lot::Mutex as MutexImpl;
    use parking_lot::MutexGuard as MutexGuardImpl;

    struct MutexData<T> {
        poisoned: bool,
        value: T,
    }

    /// Mutex from parking_lot with poisoning added on top
    pub struct Mutex<T>(MutexImpl<MutexData<T>>);

    impl<T> Mutex<T> {
        pub fn new(value: T) -> Self {
            Mutex(MutexImpl::new(MutexData {
                poisoned: false,
                value,
            }))
        }
    }

    impl<T> Mutex<T> {
        pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
            let guard = self.0.lock();
            if guard.poisoned {
                Err(PoisonError::new(MutexGuard(guard)))
            } else {
                Ok(MutexGuard(guard))
            }
        }

        pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
            let guard = self.0.try_lock();
            match guard {
                Some(guard) if guard.poisoned => {
                    Err(TryLockError::Poisoned(PoisonError::new(MutexGuard(guard))))
                }
                Some(guard) => Ok(MutexGuard(guard)),
                None => Err(TryLockError::WouldBlock),
            }
        }
    }

    pub struct MutexGuard<'a, T>(MutexGuardImpl<'a, MutexData<T>>);

    impl<'a, T> std::ops::Deref for MutexGuard<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0.value
        }
    }

    impl<'a, T> std::ops::DerefMut for MutexGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut (*self.0).value
        }
    }

    impl<'a, T> Drop for MutexGuard<'a, T> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                (*self.0).poisoned = true;
            }
        }
    }

    use parking_lot::RwLock as RwLockImpl;
    use parking_lot::RwLockReadGuard as RwLockReadGuardImpl;
    use parking_lot::RwLockWriteGuard as RwLockWriteGuardImpl;

    struct RwLockData<T> {
        poisoned: bool,
        value: T,
    }

    /// RwLock from parking_lot with poisoning added on top
    pub struct RwLock<T>(RwLockImpl<RwLockData<T>>);

    impl<T> RwLock<T> {
        pub fn new(value: T) -> Self {
            RwLock(RwLockImpl::new(RwLockData {
                poisoned: false,
                value,
            }))
        }
    }

    impl<T> RwLock<T> {
        pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
            let guard = self.0.read();
            if guard.poisoned {
                Err(PoisonError::new(RwLockReadGuard(guard)))
            } else {
                Ok(RwLockReadGuard(guard))
            }
        }

        pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
            let guard = self.0.write();
            if guard.poisoned {
                Err(PoisonError::new(RwLockWriteGuard(guard)))
            } else {
                Ok(RwLockWriteGuard(guard))
            }
        }

        pub fn try_read(&self) -> TryLockResult<RwLockReadGuard<'_, T>> {
            let guard = self.0.try_read();
            match guard {
                Some(guard) if guard.poisoned => Err(TryLockError::Poisoned(PoisonError::new(
                    RwLockReadGuard(guard),
                ))),
                Some(guard) => Ok(RwLockReadGuard(guard)),
                None => Err(TryLockError::WouldBlock),
            }
        }

        pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
            let guard = self.0.try_write();
            match guard {
                Some(guard) if guard.poisoned => Err(TryLockError::Poisoned(PoisonError::new(
                    RwLockWriteGuard(guard),
                ))),
                Some(guard) => Ok(RwLockWriteGuard(guard)),
                None => Err(TryLockError::WouldBlock),
            }
        }
    }

    pub struct RwLockReadGuard<'a, T>(RwLockReadGuardImpl<'a, RwLockData<T>>);

    impl<'a, T> std::ops::Deref for RwLockReadGuard<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0.value
        }
    }

    // No Drop for RwLockReadGuard because readers can't change data/violate invariants

    pub struct RwLockWriteGuard<'a, T>(RwLockWriteGuardImpl<'a, RwLockData<T>>);

    impl<'a, T> std::ops::Deref for RwLockWriteGuard<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0.value
        }
    }

    impl<'a, T> std::ops::DerefMut for RwLockWriteGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut (*self.0).value
        }
    }

    impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                (*self.0).poisoned = true;
            }
        }
    }
}
