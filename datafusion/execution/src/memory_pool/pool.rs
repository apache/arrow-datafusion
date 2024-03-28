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

use crate::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{DataFusionError, Result};
use hashbrown::HashMap;
use log::debug;
use parking_lot::Mutex;
use std::{
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
};

/// A [`MemoryPool`] that enforces no limit
#[derive(Debug, Default)]
pub struct UnboundedMemoryPool {
    used: AtomicUsize,
}

impl MemoryPool for UnboundedMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// A [`MemoryPool`] that implements a greedy first-come first-serve limit.
///
/// This pool works well for queries that do not need to spill or have
/// a single spillable operator. See [`FairSpillPool`] if there are
/// multiple spillable operators that all will spill.
#[derive(Debug)]
pub struct GreedyMemoryPool {
    pool_size: usize,
    meta: Mutex<SimplePool>,
}

// Simple struct that tracks memory usage across multiple consumers.
// Can be used in composition with other fields for providing more sophisticated memory pool
// policies
#[derive(Debug, Default)]
struct SimplePool {
    // Maps consumer names to the amount of memory they use.
    pool_members: HashMap<String, usize>,
    // The total memory usage of all consumers.
    used: usize,
}

impl SimplePool {
    fn unregister(&mut self, consumer_name: &str) -> usize {
        if let Some(used) = self.pool_members.remove(consumer_name) {
            self.used = self.used.checked_sub(used).unwrap();
            return used;
        };

        0
    }

    fn grow(&mut self, consumer_name: &str, additional: usize) {
        let used = self.pool_members.entry_ref(consumer_name).or_insert(0);
        *used = used.saturating_add(additional);

        self.used = self.used.saturating_add(additional);
    }

    fn shrink(&mut self, consumer_name: &str, shrink: usize) {
        let used = self.pool_members.entry_ref(consumer_name).or_insert(0);

        *used = used.checked_sub(shrink).unwrap();

        self.used = self.used.saturating_sub(shrink);
    }
}

impl GreedyMemoryPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            meta: Mutex::new(SimplePool::default()),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.meta.lock();
        state.grow(reservation.consumer().name(), additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.meta.lock();
        state.shrink(reservation.consumer().name(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.meta.lock();
        let used: usize = state.used;
        if used.saturating_add(additional) > self.pool_size {
            // dropping the mutex so that the display trait method does not deadlock
            drop(state);

            debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
            return Err(insufficient_capacity_err(
                reservation,
                additional,
                self.pool_size.saturating_sub(used),
            ));
        }

        state.grow(reservation.consumer().name(), additional);

        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.meta.lock();
        state.used
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let mut state = self.meta.lock();
        state.unregister(consumer.name());
    }
}

impl Display for GreedyMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.meta.lock();
        let used: usize = state.used;
        let free = self.pool_size.saturating_sub(used);

        write!(
            f,
            "GreedyPool {} allocations, {} used, {} free, {} capacity.\nConsumers:\n{}",
            state.pool_members.len(),
            used,
            free,
            self.pool_size,
            state
        )
    }
}

impl Display for SimplePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut allocations = self.pool_members.iter().collect::<Vec<_>>();
        allocations.sort_by(|(_, a), (_, b)| a.cmp(b).reverse());

        let allocation_report = allocations
            .iter()
            .fold("".to_string(), |acc, (member, bytes)| {
                format!("{acc}{bytes}: {member}\n")
            });

        write!(f, "{}", allocation_report)
    }
}

/// A [`MemoryPool`] that prevents spillable reservations from using more than
/// an even fraction of the available memory sans any unspillable reservations
/// (i.e. `(pool_size - unspillable_memory) / num_spillable_reservations`)
///
/// This pool works best when you know beforehand the query has
/// multiple spillable operators that will likely all need to
/// spill. Sometimes it will cause spills even when there was
/// sufficient memory (reserved for other operators) to avoid doing
/// so.
///
/// ```text
///    ┌───────────────────────z──────────────────────z───────────────┐
///    │                       z                      z               │
///    │                       z                      z               │
///    │       Spillable       z       Unspillable    z     Free      │
///    │        Memory         z        Memory        z    Memory     │
///    │                       z                      z               │
///    │                       z                      z               │
///    └───────────────────────z──────────────────────z───────────────┘
/// ```
///
/// Unspillable memory is allocated in a first-come, first-serve fashion
#[derive(Debug)]
pub struct FairSpillPool {
    /// The total memory limit
    pool_size: usize,
    state: Mutex<FairSpillPoolState>,
}

#[derive(Debug, Default)]
struct FairSpillPoolState {
    /// The number of consumers that can spill
    num_spill: usize,
    /// The total amount of memory reserved that can be spilled
    spillable: usize,

    /// The total amount of memory reserved by consumers that cannot spill
    meta: SimplePool,
}

impl FairSpillPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new FairSpillPool(pool_size={pool_size})");
        Self {
            pool_size,
            state: Mutex::new(FairSpillPoolState::default()),
        }
    }
}

impl MemoryPool for FairSpillPool {
    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill {
            self.state.lock().num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        let used = state.meta.unregister(consumer.name());

        if consumer.can_spill {
            state.num_spill = state.num_spill.checked_sub(1).unwrap();
            state.spillable = state.spillable.checked_sub(used).unwrap();
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        state.meta.grow(reservation.consumer().name(), additional);

        if reservation.registration.consumer.can_spill {
            state.spillable = state.spillable.saturating_add(additional);
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        state.meta.shrink(reservation.consumer().name(), shrink);

        if reservation.registration.consumer.can_spill {
            state.spillable = state.spillable.checked_sub(shrink).unwrap();
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        match reservation.registration.consumer.can_spill {
            true => {
                let unspillable: usize = state.meta.used.saturating_sub(state.spillable);

                // The total amount of memory available to spilling consumers
                let spill_available = self.pool_size.saturating_sub(unspillable);

                let num_spill = state.num_spill;

                // No spiller may use more than their fraction of the memory available
                let available = spill_available
                    .checked_div(num_spill)
                    .unwrap_or(spill_available);

                if reservation.size + additional > available {
                    // dropping the mutex so that the display trait method does not deadlock
                    drop(state);

                    debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                    ));
                }

                state.meta.grow(reservation.consumer().name(), additional);
                state.spillable = state.spillable.saturating_add(additional);
            }
            false => {
                let total_used: usize = state.meta.used;
                let available = self.pool_size.saturating_sub(total_used);

                if available < additional {
                    drop(state);

                    debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                    ));
                }

                state.meta.grow(reservation.consumer().name(), additional);
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state.meta.used
    }
}

impl Display for FairSpillPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        let num_spill = state.num_spill;
        let unspillable_memory = state.meta.used.saturating_sub(state.spillable);
        let spillable_memory = state.spillable;

        let free = self
            .pool_size
            .saturating_sub(unspillable_memory.saturating_add(spillable_memory));

        write!(
            f,
            "FairSpillPool {} allocations, {} spillable used, {} total spillable, {} unspillable used, {} free, {} capacity.\nConsumers:\n{}",
            state.meta.pool_members.len(),
            spillable_memory,
            num_spill,
            unspillable_memory,
            free,
            self.pool_size,
            state.meta
        )
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {}", additional, reservation.registration.consumer.name, reservation.size, available))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_simple_pool() {
        let mut pool = SimplePool::default();
        let c_a = "A";
        let c_b = "B";

        // Test grow
        pool.grow(c_a, 100);
        pool.grow(c_b, 200);

        assert_eq!(pool.used, 300);

        // Test shrink
        pool.shrink(c_a, 50);
        pool.shrink(c_b, 100);

        assert_eq!(pool.used, 150);

        // Test unregister
        assert_eq!(pool.unregister(c_a), 50);
        assert!(!pool.pool_members.contains_key("A"));
        assert_eq!(pool.used, 100);

        // Ensure unregistering a non-existent consumer does nothing
        assert_eq!(pool.unregister(c_a), 0);
    }

    #[test]
    fn test_fair() {
        let pool = Arc::new(FairSpillPool::new(100)) as _;

        let mut r1 = MemoryConsumer::new("unspillable").register(&pool);
        // Can grow beyond capacity of pool
        r1.grow(2000);
        assert_eq!(pool.reserved(), 2000);

        let mut r2 = MemoryConsumer::new("r2")
            .with_can_spill(true)
            .register(&pool);
        // Can grow beyond capacity of pool
        r2.grow(2000);

        assert_eq!(pool.reserved(), 4000);

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

        r1.shrink(1990);
        r2.shrink(2000);

        assert_eq!(pool.reserved(), 10);

        r1.try_grow(10).unwrap();
        assert_eq!(pool.reserved(), 20);

        // Can grow r2 to 80 as only spilling consumer
        r2.try_grow(80).unwrap();
        assert_eq!(pool.reserved(), 100);

        r2.shrink(70);

        assert_eq!(r1.size(), 20);
        assert_eq!(r2.size(), 10);
        assert_eq!(pool.reserved(), 30);

        let mut r3 = MemoryConsumer::new("r3")
            .with_can_spill(true)
            .register(&pool);

        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated - maximum available is 40");

        //Shrinking r2 to zero doesn't allow a3 to allocate more than 45
        r2.free();
        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated - maximum available is 40");

        // But dropping r2 does
        drop(r2);
        assert_eq!(pool.reserved(), 20);
        r3.try_grow(80).unwrap();

        assert_eq!(pool.reserved(), 100);
        r1.free();
        assert_eq!(pool.reserved(), 80);

        let mut r4 = MemoryConsumer::new("s4").register(&pool);
        let err = r4.try_grow(30).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 30 bytes for s4 with 0 bytes already allocated - maximum available is 20");
    }

    #[test]
    fn test_greedy() {
        let pool = Arc::new(GreedyMemoryPool::new(100)) as _;
        let mut r1 = MemoryConsumer::new("r1").register(&pool);

        // Can grow beyond capacity of pool
        r1.grow(2000);
        assert_eq!(pool.reserved(), 2000);

        let mut r2 = MemoryConsumer::new("r2")
            .with_can_spill(true)
            .register(&pool);
        // Can grow beyond capacity of pool
        r2.grow(2000);

        let err = r1.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r1 with 2000 bytes already allocated - maximum available is 0");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

        r1.shrink(1990);
        r2.shrink(2000);

        assert_eq!(pool.reserved(), 10);

        r1.try_grow(10).unwrap();
        assert_eq!(pool.reserved(), 20);
    }

    #[test]
    fn test_greedy_pool_display() {
        let greedy_pool = Arc::new(GreedyMemoryPool::new(1000));
        let pool: Arc<dyn MemoryPool> = greedy_pool.clone();

        let mut r1 = MemoryConsumer::new("r1").register(&pool);
        let mut r2 = MemoryConsumer::new("r2").register(&pool);

        r1.grow(100);
        r2.grow(321);

        assert_eq!(
            format!("{}", greedy_pool),
            "GreedyPool 2 allocations, 421 used, 579 free, 1000 capacity.\nConsumers:\n321: r2\n100: r1\n"
        );
    }

    #[test]
    fn test_fair_pool_display() {
        let fair_pool = Arc::new(FairSpillPool::new(1000));
        let pool: Arc<dyn MemoryPool> = fair_pool.clone();

        let mut r1 = MemoryConsumer::new("r1").register(&pool);
        let mut r2 = MemoryConsumer::new("r2").register(&pool);

        r1.grow(100);
        r2.grow(321);

        assert_eq!(
        format!("{}", fair_pool),
        "FairSpillPool 2 allocations, 0 spillable used, 0 total spillable, 421 unspillable used, 579 free, 1000 capacity.\nConsumers:\n321: r2\n100: r1\n"
        );
    }
}
