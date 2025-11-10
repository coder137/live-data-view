use std::ops::{Deref, DerefMut};

use crate::DataRead;

pub(crate) enum DataViewMessage {
    Notify { data: String },
    Drop,
}

pub struct DataView<T> {
    inner: T,
    tx: flume::Sender<DataViewMessage>,
}

impl<T> DataView<T>
where
    T: DataRead,
{
    pub fn notify(&self) {
        let data = self.inner.read_string();
        let _ignore = self.tx.send(DataViewMessage::Notify { data });
    }

    pub(crate) fn new(inner: T, tx: flume::Sender<DataViewMessage>) -> Self {
        Self { inner, tx }
    }
}

impl<T> Deref for DataView<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for DataView<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Drop for DataView<T> {
    fn drop(&mut self) {
        let _ignore = self.tx.send(DataViewMessage::Drop);
    }
}
