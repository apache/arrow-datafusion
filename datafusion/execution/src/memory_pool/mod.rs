// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`MemoryPool`] for memory management during query execution, [`proxy]` for
//! help with allocation accounting.

use datafusion_common::Result;
use std::{cmp::Ordering, sync::Arc};

mod pool;
pub mod proxy {
    pub use datafusion_common::utils::proxy::{RawTableAllocExt, VecAllocExt};
}

pub use pool::*;

/// Tracks and potentially limits memory use across operators during execution.
///
/// # Memory Management Overview
///
/// DataFusion is a streaming query engine, processing most queries without
/// buffering the entire input. Most operators require a fixed amount of memory
/// based on the schema and target batch size. However, certain operations such
/// as sorting and grouping/joining, require buffering intermediate results,
/// which can require memory proportional to the number of input rows.
///
/// Rather than tracking all allocations, DataFusion takes a pragmatic approach:
/// Intermediate memory used as data streams through the system is not accounted
/// (it assumed to be "small") but the large consumers of memory must register
/// and constrain their use. This design trades off the additional code
/// complexity of memory tracking with limiting resource usage.
///
/// When limiting memory with a `MemoryPool` you should typically reserve some
/// overhead (e.g. 10%) for the "small" memory allocations that are not tracked.
///
/// # Memory Management Design
///
/// As explained above, DataFusion's design ONLY limits operators that require
/// "large" amounts of memory (proportional to number of input rows), such as
/// `GroupByHashExec`. It does NOT track and limit memory used internally by
/// other operators such as `ParquetExec` or the `RecordBatch`es that flow
/// between operators.
///
/// In order to avoid allocating memory until the OS or the container system
/// kills the process, DataFusion `ExecutionPlan`s (operators) that consume
/// large amounts of memory must first request their desired allocation from a
/// [`MemoryPool`] before allocating more.  The request is typically managed via
/// a  [`MemoryReservation`] and [`MemoryConsumer`].
///
/// If the allocation is successful, the operator should proceed and allocate
/// the desired memory. If the allocation fails, the operator must either first
/// free memory (e.g. by spilling to local disk) and try again, or error.
///
/// Note that a `MemoryPool` can be shared by concurrently executing plans,
/// which can be used to control memory usage in a multi-tenant system.
///
/// # Implementing `MemoryPool`
///
/// You can implement a custom allocation policy by implementing the
/// [`MemoryPool`] trait and configuring a `SessionContext` appropriately.
/// However, mDataFusion comes with the following simple memory pool implementations that
/// handle many common cases:
///
/// * [`UnboundedMemoryPool`]: no memory limits (the default)
///
/// * [`GreedyMemoryPool`]: Limits memory usage to a fixed size using a "first
/// come first served" policy
///
/// * [`FairSpillPool`]: Limits memory usage to a fixed size, allocating memory
/// to all spilling operators fairly
pub trait MemoryPool: Send + Sync + std::fmt::Debug {
    /// Registers a new [`MemoryConsumer`]
    ///
    /// Note: Subsequent calls to [`Self::grow`] must be made to reserve memory
    fn register(&self, _consumer: &MemoryConsumer) {}

    /// Records the destruction of a [`MemoryReservation`] with [`MemoryConsumer`]
    ///
    /// Note: Prior calls to [`Self::shrink`] must be made to free any reserved memory
    fn unregister(&self, _consumer: &MemoryConsumer) {}

    /// Infallibly grow the provided `reservation` by `additional` bytes
    ///
    /// This must always succeed
    fn grow(&self, reservation: &MemoryReservation, additional: usize);

    /// Infallibly shrink the provided `reservation` by `shrink` bytes
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize);

    /// Attempt to grow the provided `reservation` by `additional` bytes
    ///
    /// On error the `allocation` will not be increased in size
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()>;

    /// Return the total amount of memory reserved
    fn reserved(&self) -> usize;
}

/// A memory consumer is a named allocation traced by a particular
/// [`MemoryReservation`] in a [`MemoryPool`]. All allocations are registered to
/// a particular `MemoryConsumer`;
///
/// For help with allocation accounting, see the [proxy] module.
///
/// [proxy]: crate::memory_pool::proxy
#[derive(Debug)]
pub struct MemoryConsumer {
    name: String,
    can_spill: bool,
}

impl MemoryConsumer {
    /// Create a new empty [`MemoryConsumer`] that can be grown using [`MemoryReservation`]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            can_spill: false,
        }
    }

    /// Set whether this allocation can be spilled to disk
    pub fn with_can_spill(self, can_spill: bool) -> Self {
        Self { can_spill, ..self }
    }

    /// Returns true if this allocation can spill to disk
    pub fn can_spill(&self) -> bool {
        self.can_spill
    }

    /// Returns the name associated with this allocation
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registers this [`MemoryConsumer`] with the provided [`MemoryPool`] returning
    /// a [`MemoryReservation`] that can be used to grow or shrink the memory reservation
    pub fn register(self, pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
        pool.register(&self);
        MemoryReservation {
            registration: Arc::new(SharedRegistration {
                pool: Arc::clone(pool),
                consumer: self,
            }),
            size: 0,
        }
    }
}

/// A registration of a [`MemoryConsumer`] with a [`MemoryPool`].
///
/// Calls [`MemoryPool::unregister`] on drop to return any memory to
/// the underlying pool.
#[derive(Debug)]
struct SharedRegistration {
    pool: Arc<dyn MemoryPool>,
    consumer: MemoryConsumer,
}

impl Drop for SharedRegistration {
    fn drop(&mut self) {
        self.pool.unregister(&self.consumer);
    }
}

/// A [`MemoryReservation`] tracks an individual reservation of a
/// number of bytes of memory in a [`MemoryPool`] that is freed back
/// to the pool on drop.
///
/// The reservation can be grown or shrunk over time.
#[derive(Debug)]
pub struct MemoryReservation {
    registration: Arc<SharedRegistration>,
    size: usize,
}

impl MemoryReservation {
    /// Returns the size of this reservation in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns [MemoryConsumer] for this [MemoryReservation]
    pub fn consumer(&self) -> &MemoryConsumer {
        &self.registration.consumer
    }

    /// Frees all bytes from this reservation back to the underlying
    /// pool, returning the number of bytes freed.
    pub fn free(&mut self) -> usize {
        let size = self.size;
        if size != 0 {
            self.shrink(size)
        }
        size
    }

    /// Frees `capacity` bytes from this reservation
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds [`Self::size`]
    pub fn shrink(&mut self, capacity: usize) {
        let new_size = self.size.checked_sub(capacity).unwrap();
        self.registration.pool.shrink(self, capacity);
        self.size = new_size
    }

    /// Sets the size of this reservation to `capacity`
    pub fn resize(&mut self, capacity: usize) {
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.grow(capacity - self.size),
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        }
    }

    /// Try to set the size of this reservation to `capacity`
    pub fn try_resize(&mut self, capacity: usize) -> Result<()> {
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.try_grow(capacity - self.size)?,
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        };
        Ok(())
    }

    /// Increase the size of this reservation by `capacity` bytes
    pub fn grow(&mut self, capacity: usize) {
        self.registration.pool.grow(self, capacity);
        self.size += capacity;
    }

    /// Try to increase the size of this reservation by `capacity`
    /// bytes, returning error if there is insufficient capacity left
    /// in the pool.
    pub fn try_grow(&mut self, capacity: usize) -> Result<()> {
        self.registration.pool.try_grow(self, capacity)?;
        self.size += capacity;
        Ok(())
    }

    /// Splits off `capacity` bytes from this [`MemoryReservation`]
    /// into a new [`MemoryReservation`] with the same
    /// [`MemoryConsumer`].
    ///
    /// This can be useful to free part of this reservation with RAAI
    /// style dropping
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds [`Self::size`]
    pub fn split(&mut self, capacity: usize) -> MemoryReservation {
        self.size = self.size.checked_sub(capacity).unwrap();
        Self {
            size: capacity,
            registration: Arc::clone(&self.registration),
        }
    }

    /// Returns a new empty [`MemoryReservation`] with the same [`MemoryConsumer`]
    pub fn new_empty(&self) -> Self {
        Self {
            size: 0,
            registration: Arc::clone(&self.registration),
        }
    }

    /// Splits off all the bytes from this [`MemoryReservation`] into
    /// a new [`MemoryReservation`] with the same [`MemoryConsumer`]
    pub fn take(&mut self) -> MemoryReservation {
        self.split(self.size)
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.free();
    }
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

/// Present size in human readable form
pub fn human_readable_size(size: usize) -> String {
    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{value:.1} {unit}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_underflow() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut a1 = MemoryConsumer::new("a1").register(&pool);
        assert_eq!(pool.reserved(), 0);

        a1.grow(100);
        assert_eq!(pool.reserved(), 100);

        assert_eq!(a1.free(), 100);
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(100).unwrap_err();
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(30).unwrap();
        assert_eq!(pool.reserved(), 30);

        let mut a2 = MemoryConsumer::new("a2").register(&pool);
        a2.try_grow(25).unwrap_err();
        assert_eq!(pool.reserved(), 30);

        drop(a1);
        assert_eq!(pool.reserved(), 0);

        a2.try_grow(25).unwrap();
        assert_eq!(pool.reserved(), 25);
    }

    #[test]
    fn test_split() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut r1 = MemoryConsumer::new("r1").register(&pool);

        r1.try_grow(20).unwrap();
        assert_eq!(r1.size(), 20);
        assert_eq!(pool.reserved(), 20);

        // take 5 from r1, should still have same reservation split
        let r2 = r1.split(5);
        assert_eq!(r1.size(), 15);
        assert_eq!(r2.size(), 5);
        assert_eq!(pool.reserved(), 20);

        // dropping r1 frees 15 but retains 5 as they have the same consumer
        drop(r1);
        assert_eq!(r2.size(), 5);
        assert_eq!(pool.reserved(), 5);
    }

    #[test]
    fn test_new_empty() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut r1 = MemoryConsumer::new("r1").register(&pool);

        r1.try_grow(20).unwrap();
        let mut r2 = r1.new_empty();
        r2.try_grow(5).unwrap();

        assert_eq!(r1.size(), 20);
        assert_eq!(r2.size(), 5);
        assert_eq!(pool.reserved(), 25);
    }

    #[test]
    fn test_take() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut r1 = MemoryConsumer::new("r1").register(&pool);

        r1.try_grow(20).unwrap();
        let mut r2 = r1.take();
        r2.try_grow(5).unwrap();

        assert_eq!(r1.size(), 0);
        assert_eq!(r2.size(), 25);
        assert_eq!(pool.reserved(), 25);

        // r1 can still grow again
        r1.try_grow(3).unwrap();
        assert_eq!(r1.size(), 3);
        assert_eq!(r2.size(), 25);
        assert_eq!(pool.reserved(), 28);
    }
}
