use std::collections::{HashMap, VecDeque};

use crate::{DataViewMessage, DataViewSystemEvent, DataWrite};

pub(crate) enum DataStorageMessage {
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
        id: DataStorageId,
        data: String,
    },
    Update {
        id: DataStorageId,
        data: String,
    },
    Drop {
        id: DataStorageId,
    },
}

pub(crate) type DataStorageId = u64;

struct DataStorageInfo {
    read: VecDeque<String>,
    write: Option<Box<dyn DataWrite>>,
}

pub(crate) struct DataStorageSystem {
    data_tx: flume::Sender<DataStorageMessage>,
    data_rx: flume::Receiver<DataStorageMessage>,
    event_tx: tokio::sync::broadcast::Sender<DataViewSystemEvent>,
    // TODO, Make key a UUID in the future (if needed)
    data: HashMap<DataStorageId, DataStorageInfo>,
    data_id: DataStorageId,
}

impl DataStorageSystem {
    pub(crate) fn new(
        data_tx: flume::Sender<DataStorageMessage>,
        data_rx: flume::Receiver<DataStorageMessage>,
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

    pub(crate) async fn run(mut self) {
        loop {
            let message = self
                .data_rx
                .recv_async()
                .await
                .expect("DataRx channel should never shutdown");
            match message {
                DataStorageMessage::NewRead { initial, rx } => {
                    let id = self.new_dataid();
                    self.register_read(id, rx);
                    let event = DataViewSystemEvent::NewRead {
                        id,
                        data: initial.clone(),
                    };
                    let data = DataStorageInfo {
                        read: VecDeque::from([initial]),
                        write: None,
                    };
                    self.data.insert(id, data);
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
                DataStorageMessage::NewReadWrite { initial, rx, write } => {
                    let id = self.new_dataid();
                    self.register_read(id, rx);
                    let event = DataViewSystemEvent::NewReadWrite {
                        id,
                        data: initial.clone(),
                    };
                    let data = DataStorageInfo {
                        read: VecDeque::from([initial]),
                        write: Some(write),
                    };
                    self.data.insert(id, data);
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
                DataStorageMessage::Notify { id, data } => {
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
                DataStorageMessage::Update { id, data } => {
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
                DataStorageMessage::Drop { id } => {
                    self.data.remove(&id);
                    let event = DataViewSystemEvent::Drop { id };
                    println!("{:?}", event);
                    let _ignore = self.event_tx.send(event);
                }
            }
        }
    }

    fn new_dataid(&mut self) -> DataStorageId {
        let id = self.data_id;
        self.data_id += 1;
        id
    }

    fn register_read(&self, id: DataStorageId, rx: flume::Receiver<DataViewMessage>) {
        let tx = self.data_tx.clone();
        tokio::spawn(async move {
            loop {
                let message = rx.recv_async().await;
                let Ok(message) = message else {
                    break;
                };
                match message {
                    DataViewMessage::Notify { data } => {
                        let _ignore = tx.send_async(DataStorageMessage::Notify { id, data }).await;
                    }
                    DataViewMessage::Drop => {
                        let _ignore = tx.send_async(DataStorageMessage::Drop { id }).await;
                        break;
                    }
                }
            }
        });
    }
}
