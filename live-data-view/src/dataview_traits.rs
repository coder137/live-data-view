pub trait DataRead {
    fn read_string(&self) -> String;
}

impl<T> DataRead for tokio::sync::watch::Sender<T>
where
    T: serde::Serialize,
{
    fn read_string(&self) -> String {
        let data = self.borrow();
        let data = serde_json::to_string_pretty(&*data).unwrap();
        data
    }
}

impl DataRead for u32 {
    fn read_string(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

pub trait DataWrite: Send {
    fn write_bytes(&mut self, data: &[u8]) -> Result<(), ()>;

    fn write_string(&mut self, data: String) -> Result<(), ()> {
        self.write_bytes(data.as_bytes())
    }
}

impl<T> DataWrite for tokio::sync::watch::Sender<T>
where
    T: serde::de::DeserializeOwned + Send + Sync,
{
    fn write_bytes(&mut self, data: &[u8]) -> Result<(), ()> {
        let data = serde_json::from_slice::<T>(data).map_err(|_e| ())?;
        self.send_replace(data);
        Ok(())
    }
}
