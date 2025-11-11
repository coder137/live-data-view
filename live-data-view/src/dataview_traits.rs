use std::ops::Deref;

pub trait DataRead {
    fn read_string(&self) -> String;
}

impl DataRead for u32 {
    fn read_string(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

impl<T> DataRead for std::sync::Mutex<T>
where
    T: DataRead,
{
    fn read_string(&self) -> String {
        self.lock().unwrap().read_string()
    }
}

impl<T> DataRead for std::sync::Arc<T>
where
    T: DataRead,
{
    fn read_string(&self) -> String {
        self.deref().read_string()
    }
}

impl<T> DataRead for tokio::sync::watch::Sender<T>
where
    T: DataRead,
{
    fn read_string(&self) -> String {
        self.borrow().read_string()
    }
}

pub trait DataWrite: Send {
    fn write_bytes(&mut self, data: &[u8]) -> bool;

    fn write_string(&mut self, data: String) -> bool {
        self.write_bytes(data.as_bytes())
    }
}

impl<T> DataWrite for std::sync::Arc<std::sync::Mutex<T>>
where
    T: serde::de::DeserializeOwned + Send,
{
    fn write_bytes(&mut self, data: &[u8]) -> bool {
        let data = serde_json::from_slice::<T>(data);
        let Ok(data) = data else {
            return false;
        };
        let guard = self.lock();
        let Ok(mut guard) = guard else { return false };
        *guard = data;
        true
    }
}

impl<T> DataWrite for tokio::sync::watch::Sender<T>
where
    T: serde::de::DeserializeOwned + Send + Sync,
{
    fn write_bytes(&mut self, data: &[u8]) -> bool {
        let data = serde_json::from_slice::<T>(data);
        let Ok(data) = data else {
            return false;
        };
        self.send_replace(data);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32() {
        let data = 123u32;
        assert_eq!(data.read_string(), "123");
    }
}
