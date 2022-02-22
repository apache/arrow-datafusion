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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::error::{BallistaError, Result};

#[async_trait]
pub trait EventAction<E>: Send + Sync {
    fn on_start(&self);

    fn on_stop(&self);

    async fn on_receive(&self, event: E) -> Result<Option<E>>;

    fn on_error(&self, error: BallistaError);
}

pub struct EventLoop<E> {
    name: String,
    stopped: Arc<AtomicBool>,
    buffer_size: usize,
    action: Arc<dyn EventAction<E>>,
    tx_event: Option<mpsc::Sender<E>>,
}

impl<E: Send + 'static> EventLoop<E> {
    pub fn new(
        name: String,
        buffer_size: usize,
        action: Arc<dyn EventAction<E>>,
    ) -> Self {
        Self {
            name,
            stopped: Arc::new(AtomicBool::new(false)),
            buffer_size,
            action,
            tx_event: None,
        }
    }

    fn run(&self, mut rx_event: mpsc::Receiver<E>) {
        assert!(
            self.tx_event.is_some(),
            "The event sender should be initialized first!"
        );
        let tx_event = self.tx_event.as_ref().unwrap().clone();
        let name = self.name.clone();
        let stopped = self.stopped.clone();
        let action = self.action.clone();
        tokio::spawn(async move {
            info!("Starting the event loop {}", name);
            while !stopped.load(Ordering::SeqCst) {
                let event = rx_event.recv().await.unwrap();
                match action.on_receive(event).await {
                    Ok(Some(event)) => {
                        if let Err(e) = tx_event.send(event).await {
                            let msg = format!("Fail to send event due to {}", e);
                            error!("{}", msg);
                            action.on_error(BallistaError::General(msg));
                        }
                    }
                    Err(e) => {
                        error!("Fail to process event due to {}", e);
                        action.on_error(e);
                    }
                    _ => {}
                }
            }
            info!("The event loop {} has been stopped", name);
        });
    }

    pub fn start(&mut self) -> Result<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(BallistaError::General(format!(
                "{} has already been stopped",
                self.name
            )));
        }
        self.action.on_start();

        let (tx_event, rx_event) = mpsc::channel::<E>(self.buffer_size);
        self.tx_event = Some(tx_event);
        self.run(rx_event);

        Ok(())
    }

    pub fn stop(&self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.action.on_stop();
        } else {
            // Keep quiet to allow calling `stop` multiple times.
        }
    }

    pub async fn post_event(&self, event: E) -> Result<()> {
        if let Some(tx_event) = self.tx_event.as_ref() {
            tx_event.send(event).await.map_err(|e| {
                BallistaError::General(format!("Fail to send event due to {}", e))
            })?
        } else {
            warn!("Event sender not exist!!!");
        }

        Ok(())
    }
}
