use std::{
    collections::{HashMap, VecDeque},
    sync::OnceLock,
    time::Instant,
};

use thiserror::Error;

use tokio_util::sync::CancellationToken;

use crate::{DataRead, DataView, DataViewMessage, DataWrite};

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
    NewRead { id: DataId, value: String },
    NewReadWrite { id: DataId, value: String },
    Notify { id: DataId, data: String },
    Drop { id: DataId },
}

pub struct DataViewSystem {
    data_tx: flume::Sender<DataViewSystemAsyncMessage>,
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
                    let this = DataViewSystemAsync::new(data_tx_clone, data_rx, event_tx);
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
            .send(DataViewSystemAsyncMessage::NewRead {
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
            .send(DataViewSystemAsyncMessage::NewReadWrite {
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

enum DataViewSystemAsyncMessage {
    NewRead {
        initial: String,
        rx: flume::Receiver<DataViewMessage>,
    },
    NewReadWrite {
        initial: String,
        rx: flume::Receiver<DataViewMessage>,
        write: Box<dyn DataWrite>,
    },
    Notify {
        id: DataId,
        data: String,
    },
    Update {
        id: DataId,
        data: String,
    },
    Drop {
        id: DataId,
    },
}

type DataId = u64;

struct DataInfo {
    read: VecDeque<String>,
    write: Option<Box<dyn DataWrite>>,
}

struct DataViewSystemAsync {
    data_tx: flume::Sender<DataViewSystemAsyncMessage>,
    data_rx: flume::Receiver<DataViewSystemAsyncMessage>,
    event_tx: tokio::sync::broadcast::Sender<DataViewSystemEvent>,
    // TODO, Make key a UUID in the future (if needed)
    data: HashMap<DataId, DataInfo>,
    data_id: DataId,
}

impl DataViewSystemAsync {
    fn new(
        data_tx: flume::Sender<DataViewSystemAsyncMessage>,
        data_rx: flume::Receiver<DataViewSystemAsyncMessage>,
        event_tx: tokio::sync::broadcast::Sender<DataViewSystemEvent>,
    ) -> Self {
        Self {
            data_tx,
            data_rx,
            event_tx,
            data: HashMap::new(),
            data_id: 1,
        }
    }

    async fn run(mut self) {
        loop {
            let message = self
                .data_rx
                .recv_async()
                .await
                .expect("DataRx channel should never shutdown");
            match message {
                DataViewSystemAsyncMessage::NewRead { initial, rx } => {
                    let id = self.new_dataid();
                    self.register_read(id, rx);
                    let event = DataViewSystemEvent::NewRead {
                        id,
                        value: initial.clone(),
                    };
                    let data = DataInfo {
                        read: VecDeque::from([initial]),
                        write: None,
                    };
                    self.data.insert(id, data);
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
                DataViewSystemAsyncMessage::NewReadWrite { initial, rx, write } => {
                    let id = self.new_dataid();
                    self.register_read(id, rx);
                    let event = DataViewSystemEvent::NewReadWrite {
                        id,
                        value: initial.clone(),
                    };
                    let data = DataInfo {
                        read: VecDeque::from([initial]),
                        write: Some(write),
                    };
                    self.data.insert(id, data);
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
                DataViewSystemAsyncMessage::Notify { id, data } => {
                    let info = match self.data.get_mut(&id) {
                        Some(info) => info,
                        None => {
                            continue;
                        }
                    };
                    let event = DataViewSystemEvent::Notify {
                        id,
                        data: data.clone(),
                    };
                    info.read.push_front(data);
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
                DataViewSystemAsyncMessage::Update { id, data } => {
                    let info = match self.data.get_mut(&id) {
                        Some(info) => info,
                        None => {
                            continue;
                        }
                    };
                    let Some(write) = info.write.as_mut() else {
                        continue;
                    };
                    if write.write_string(data.clone()) {
                        let event = DataViewSystemEvent::Notify { id, data };
                        println!("{:?}", event);
                        let _ignore = self.event_tx.send(event);
                    }
                }
                DataViewSystemAsyncMessage::Drop { id } => {
                    self.data.remove(&id);
                    let event = DataViewSystemEvent::Drop { id };
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
            }
        }
    }

    fn new_dataid(&mut self) -> DataId {
        let id = self.data_id;
        self.data_id += 1;
        id
    }

    fn register_read(&self, id: DataId, rx: flume::Receiver<DataViewMessage>) {
        let tx = self.data_tx.clone();
        tokio::spawn(async move {
            loop {
                let message = rx.recv_async().await;
                let Ok(message) = message else {
                    break;
                };
                match message {
                    DataViewMessage::Notify { data } => {
                        let _ignore = tx
                            .send_async(DataViewSystemAsyncMessage::Notify { id, data })
                            .await;
                    }
                    DataViewMessage::Drop => {
                        let _ignore = tx.send_async(DataViewSystemAsyncMessage::Drop { id }).await;
                        break;
                    }
                }
            }
        });
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
                value: "3".into()
            }
        );

        let event = event_rx.blocking_recv().unwrap();
        // assert_eq!(event, "Notify: 1 : 5");
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
                value: "3".into()
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
            .send(DataViewSystemAsyncMessage::Update {
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
