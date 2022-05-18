mod memory;
pub use memory::MemTable;
use crate::{KvError, Value, Kvpair};


/// 对存储的抽象，我们不关心数据存在哪儿，但需要定义外界如何和存储打交道
pub trait Storage {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

/// provide Storage iterator
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self{
        Self{data}
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basi_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    fn test_basi_interface(store: impl Storage) {
        // for the first time of set, it will create table, insert value, and return None
        let v = store.set("t1", "hello".into(), "world".into());
        assert!(v.unwrap().is_none());
        // for the second time of set that insert into the same key, it will update and return the old value
        let v1 = store.set("t1", "hello".into(), "world2".into());
        assert_eq!(v1, Ok(Some("world".into())));                               
        
        //call get when the key that exists
        let v = store.get("t1", "hello");
        assert_eq!(v, Ok(Some("world2".into())));

        //call get when the key that does not exist
        assert_eq!(Ok(None), store.get("t1", "hello1"));
        assert!(store.get("t2", "hellow1").unwrap().is_none());

        //contains return Ok(true) if exsits; Ok(false) if not
        assert_eq!(store.contains("t1", "hello"), Ok(true));
        assert_eq!(store.contains("t1", "hellow1"), Ok(false));

        //del delete the key if exists and return old value
        let v = store.del("t1", "hello");
        assert_eq!(v, Ok(Some("world2".into())));

        //del if table or key does not exist return None
        assert_eq!(Ok(None), store.del("t1","hellow1"));
        assert_eq!(Ok(None), store.del("t2", "hello"));

    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();
        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }

    
    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();
        let mut data = store.get_iter("t2").unwrap().collect::<Vec<_>>();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }
    
}