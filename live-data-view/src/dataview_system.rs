use std::{sync::OnceLock, time::Instant};

use thiserror::Error;

use tokio_util::sync::CancellationToken;

use crate::{DataRead, DataStorageId, DataStorageMessage, DataStorageSystem, DataView, DataWrite};

static DATAVIEW_SYSTEM_GLOBAL: OnceLock<DataViewSystem> = OnceLock::new();

#[derive(Error, Debug)]
pub enum DataViewSystemError {
    #[error("DataViewSystem is already initialized")]
    AlreadyInitialized,
    #[error("DataViewSystem is closed")]
    Closed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataViewSystemEvent {
    NewRead { id: DataStorageId, data: String },
    NewReadWrite { id: DataStorageId, data: String },
    Notify { id: DataStorageId, data: String },
    Drop { id: DataStorageId },
}

pub struct DataViewSystem {
    data_tx: flume::Sender<DataStorageMessage>,
    event_rx: tokio::sync::broadcast::Receiver<DataViewSystemEvent>,
    cancel: CancellationToken,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DataViewSystem {
    pub fn init_local() -> Self {
        let cancel = tokio_util::sync::CancellationToken::new();

        let (data_tx, data_rx) = flume::unbounded();
        let (event_tx, event_rx) = tokio::sync::broadcast::channel(10);

        let data_tx_clone = data_tx.clone();
        let cancel_clone = cancel.clone();
        let handle = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let this = DataStorageSystem::new(data_tx_clone, data_rx, event_tx);
                    let future = this.run();
                    let future = cancel_clone.run_until_cancelled_owned(future);
                    future.await;
                });
        });

        Self {
            data_tx,
            event_rx,
            cancel,
            handle: Some(handle),
        }
    }

    pub fn init_global() -> Result<(), DataViewSystemError> {
        let this = Self::init_local();
        DATAVIEW_SYSTEM_GLOBAL
            .set(this)
            .map_err(|_e| DataViewSystemError::AlreadyInitialized)?;
        Ok(())
    }

    pub fn get_global() -> Option<&'static Self> {
        DATAVIEW_SYSTEM_GLOBAL.get()
    }

    pub fn get_event_channel(&self) -> tokio::sync::broadcast::Receiver<DataViewSystemEvent> {
        self.event_rx.resubscribe()
    }

    pub fn register_read<T>(&self, inner: T) -> Result<DataView<T>, DataViewSystemError>
    where
        T: DataRead,
    {
        let (tx, rx) = flume::bounded(2);
        self.data_tx
            .send(DataStorageMessage::NewRead {
                initial: inner.read_string(),
                rx,
            })
            .map_err(|_e| DataViewSystemError::Closed)?;
        let view = DataView::new(inner, tx);
        Ok(view)
    }

    pub fn register_readwrite<T>(&self, inner: T) -> Result<DataView<T>, DataViewSystemError>
    where
        T: DataRead + DataWrite + Clone + 'static,
    {
        let (tx, rx) = flume::bounded(2);
        self.data_tx
            .send(DataStorageMessage::NewReadWrite {
                initial: inner.read_string(),
                rx,
                write: Box::new(inner.clone()),
            })
            .map_err(|_e| DataViewSystemError::Closed)?;
        let view = DataView::new(inner, tx);
        Ok(view)
    }
}

impl Drop for DataViewSystem {
    fn drop(&mut self) {
        let instant = Instant::now();
        self.cancel.cancel();
        self.handle.take().unwrap().join().unwrap();
        let elapsed = instant.elapsed();
        println!("DataViewSystem shutdown in {:?}", elapsed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn test_read() {
        let dataview_system = DataViewSystem::init_local();
        let mut event_rx = dataview_system.get_event_channel();

        let data = Arc::new(Mutex::new(3));
        let data = dataview_system.register_read(data).unwrap();

        {
            let mut lock = data.lock().unwrap();
            *lock += 2;
            drop(lock);
        }
        data.notify();

        let event = event_rx.blocking_recv().unwrap();
        assert_eq!(
            event,
            DataViewSystemEvent::NewRead {
                id: 1,
                data: "3".into()
            }
        );

        let event = event_rx.blocking_recv().unwrap();
        assert_eq!(
            event,
            DataViewSystemEvent::Notify {
                id: 1,
                data: "5".into()
            }
        );
    }

    #[test]
    fn test_readwrite() {
        let dataview_system = DataViewSystem::init_local();
        let mut event_rx = dataview_system.get_event_channel();

        let data = Arc::new(Mutex::new(3));
        let data = dataview_system.register_readwrite(data).unwrap();

        // Notify (T -> String)
        {
            let mut lock = data.lock().unwrap();
            *lock += 2;
            drop(lock);
        }
        data.notify();

        let event = event_rx.blocking_recv().unwrap();
        assert_eq!(
            event,
            DataViewSystemEvent::NewReadWrite {
                id: 1,
                data: "3".into()
            }
        );

        let event = event_rx.blocking_recv().unwrap();
        assert_eq!(
            event,
            DataViewSystemEvent::Notify {
                id: 1,
                data: "5".into()
            }
        );

        // Update (String -> T)
        dataview_system
            .data_tx
            .send(DataStorageMessage::Update {
                id: 1,
                data: "20".to_string(),
            })
            .unwrap();
        let event = event_rx.blocking_recv().unwrap();
        assert_eq!(
            event,
            DataViewSystemEvent::Notify {
                id: 1,
                data: "20".into()
            }
        );
        {
            let lock = data.lock().unwrap();
            assert_eq!(*lock, 20);
        }
    }
}
