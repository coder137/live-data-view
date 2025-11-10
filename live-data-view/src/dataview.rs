use std::ops::{Deref, DerefMut};

use crate::{DataRead, DataViewSystem};

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
    pub fn register(inner: T) -> Result<Self, &'static str> {
        let (tx, rx) = flume::bounded(2);
        let dataview_system = DataViewSystem::get_global()
            .ok_or("Register DataViewSystem globally via init_global API")?;
        dataview_system
            .register_read(inner.read_string(), rx)
            .map_err(|_| "DataViewSystem has not been initialized")?;
        let this = Self { inner, tx };
        Ok(this)
    }

    pub fn register_alt(dataview_system: &DataViewSystem, inner: T) -> Result<Self, &'static str> {
        let (tx, rx) = flume::bounded(2);
        dataview_system
            .register_read(inner.read_string(), rx)
            .map_err(|_| "DataViewSystem has not been initialized")?;
        let this = Self { inner, tx };
        Ok(this)
    }

    pub fn notify(&self) {
        let data = self.inner.read_string();
        let _ignore = self.tx.send(DataViewMessage::Notify { data });
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
