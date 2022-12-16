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

use crate::execution::memory_pool::{AllocationOptions, MemoryPool, TrackedAllocation};
use datafusion_common::{DataFusionError, Result};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A [`MemoryPool`] that enforces no limit
#[derive(Debug, Default)]
pub struct UnboundedMemoryPool {
    used: AtomicUsize,
}

impl MemoryPool for UnboundedMemoryPool {
    fn grow(&self, _allocation: &TrackedAllocation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _allocation: &TrackedAllocation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, allocation: &TrackedAllocation, additional: usize) -> Result<()> {
        self.grow(allocation, additional);
        Ok(())
    }

    fn allocated(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// A [`MemoryPool`] that implements a greedy first-come first-serve limit
#[derive(Debug)]
pub struct GreedyMemoryPool {
    pool_size: usize,
    used: AtomicUsize,
}

impl GreedyMemoryPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            used: AtomicUsize::new(0),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, _allocation: &TrackedAllocation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _allocation: &TrackedAllocation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, allocation: &TrackedAllocation, additional: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= self.pool_size).then_some(new_used)
            })
            .map_err(|used| {
                insufficient_capacity_err(allocation, additional, self.pool_size - used)
            })?;
        Ok(())
    }

    fn allocated(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

/// A [`MemoryPool`] that prevents spillable allocations from using more than
/// an even fraction of the available memory sans any unspillable allocations
/// (i.e. `(pool_size - unspillable_memory) / num_spillable_allocations`)
///
///    ┌───────────────────────z──────────────────────z───────────────┐
///    │                       z                      z               │
///    │                       z                      z               │
///    │       Spillable       z       Unspillable    z     Free      │
///    │        Memory         z        Memory        z    Memory     │
///    │                       z                      z               │
///    │                       z                      z               │
///    └───────────────────────z──────────────────────z───────────────┘
///
/// Unspillable memory is allocated in a first-come, first-serve fashion
#[derive(Debug)]
pub struct FairSpillPool {
    /// The total memory limit
    pool_size: usize,

    state: Mutex<FairSpillPoolState>,
}

#[derive(Debug)]
struct FairSpillPoolState {
    /// The number of allocations that can spill
    num_spill: usize,

    /// The total amount of memory allocated that can be spilled
    spillable: usize,

    /// The total amount of memory allocated by consumers that cannot spill
    unspillable: usize,
}

impl FairSpillPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            state: Mutex::new(FairSpillPoolState {
                num_spill: 0,
                spillable: 0,
                unspillable: 0,
            }),
        }
    }
}

impl MemoryPool for FairSpillPool {
    fn allocate(&self, options: &AllocationOptions) {
        if options.can_spill {
            self.state.lock().num_spill += 1;
        }
    }

    fn free(&self, options: &AllocationOptions) {
        if options.can_spill {
            self.state.lock().num_spill -= 1;
        }
    }

    fn grow(&self, allocation: &TrackedAllocation, additional: usize) {
        let mut state = self.state.lock();
        match allocation.options.can_spill {
            true => state.spillable += additional,
            false => state.unspillable += additional,
        }
    }

    fn shrink(&self, allocation: &TrackedAllocation, shrink: usize) {
        let mut state = self.state.lock();
        match allocation.options.can_spill {
            true => state.spillable -= shrink,
            false => state.unspillable -= shrink,
        }
    }

    fn try_grow(&self, allocation: &TrackedAllocation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        match allocation.options.can_spill {
            true => {
                // The total amount of memory available to spilling consumers
                let spill_available = self.pool_size.saturating_sub(state.unspillable);

                // No spiller may use more than their fraction of the memory available
                let available = spill_available
                    .checked_div(state.num_spill)
                    .unwrap_or(spill_available);

                if allocation.size + additional > available {
                    return Err(insufficient_capacity_err(
                        allocation, additional, available,
                    ));
                }
                state.spillable += additional;
            }
            false => {
                let available = self
                    .pool_size
                    .saturating_sub(state.unspillable + state.unspillable);

                if available < additional {
                    return Err(insufficient_capacity_err(
                        allocation, additional, available,
                    ));
                }
                state.unspillable += additional;
            }
        }
        Ok(())
    }

    fn allocated(&self) -> usize {
        let state = self.state.lock();
        state.spillable + state.unspillable
    }
}

fn insufficient_capacity_err(
    allocation: &TrackedAllocation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {}", additional, allocation.options.name, allocation.size, available))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::memory_pool::AllocationOptions;
    use std::sync::Arc;

    #[test]
    fn test_fair() {
        let pool = Arc::new(FairSpillPool::new(100)) as _;

        let mut a1 = TrackedAllocation::new(&pool, "unspillable".to_string());
        // Can grow beyond capacity of pool
        a1.grow(2000);
        assert_eq!(pool.allocated(), 2000);

        let options = AllocationOptions::new("s1".to_string()).with_can_spill(true);
        let mut a2 = TrackedAllocation::new_with_options(&pool, options);
        // Can grow beyond capacity of pool
        a2.grow(2000);

        assert_eq!(pool.allocated(), 4000);

        let err = a2.try_grow(1).unwrap_err().to_string();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for s1 with 2000 bytes already allocated - maximum available is 0");

        let err = a2.try_grow(1).unwrap_err().to_string();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for s1 with 2000 bytes already allocated - maximum available is 0");

        a1.shrink(1990);
        a2.shrink(2000);

        assert_eq!(pool.allocated(), 10);

        a1.try_grow(10).unwrap();
        assert_eq!(pool.allocated(), 20);

        // Can grow a2 to 80 as only spilling consumer
        a2.try_grow(80).unwrap();
        assert_eq!(pool.allocated(), 100);

        a2.shrink(70);

        assert_eq!(a1.size(), 20);
        assert_eq!(a2.size(), 10);
        assert_eq!(pool.allocated(), 30);

        let options = AllocationOptions::new("s2".to_string()).with_can_spill(true);
        let mut a3 = TrackedAllocation::new_with_options(&pool, options);

        let err = a3.try_grow(70).unwrap_err().to_string();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for s2 with 0 bytes already allocated - maximum available is 40");

        //Shrinking a2 to zero doesn't allow a3 to allocate more than 45
        a2.free();
        let err = a3.try_grow(70).unwrap_err().to_string();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for s2 with 0 bytes already allocated - maximum available is 40");

        // But dropping a2 does
        drop(a2);
        assert_eq!(pool.allocated(), 20);
        a3.try_grow(80).unwrap();
    }
}
