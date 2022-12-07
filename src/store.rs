use std::collections::HashSet;

use anyhow::{anyhow, Result};
use futures::{Future, StreamExt};
use redis::{aio::ConnectionManager, Cmd, FromRedisValue, ToRedisArgs};
use redis::{AsyncCommands, RedisResult};

const EXPIRE_SECONDS: usize = 60 * 60 * 4;

pub fn check_string(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Key {
    token: String,
    key: String,
}

impl TryFrom<(&str, &str)> for Key {
    fn try_from((token, key): (&str, &str)) -> Result<Self> {
        if !check_string(token) {
            return Err(anyhow!("token must be [a-z0-9_.-]"));
        }

        Ok(Key {
            token: token.to_owned(),
            key: key.to_owned(),
        })
    }

    type Error = anyhow::Error;
}

impl Key {
    pub fn redis_key(&self, param: &str) -> String {
        assert!(param.contains(':') == false);

        return format!("pcafe:{}:{}:{}", self.token, self.key, param);
    }

    pub fn redis_key_pattern(&self) -> String {
        return format!("pcafe:{}:{}*", self.token, self.key);
    }

    pub fn from_redis_key(redis_key: &str) -> Result<Key> {
        let v: Vec<&str> = redis_key.split(":").collect();

        if !v.iter().all(|s| check_string(*s)) {
            return Err(anyhow!("Bad key"));
        }

        match (v[0], v[1], &v[2..v.len() - 1]) {
            ("pcafe", token, key) => Ok(Key {
                token: token.to_owned(),
                key: key.join(":"),
            }),
            _ => Err(anyhow!("Bad structure: {:?}", v)),
        }
    }
}

pub struct Update {
    key: Key,
    state: Option<Option<String>>,
    current: Option<Option<i64>>,
    max: Option<Option<i64>>,
}

#[derive(Debug)]
pub struct Value {
    state: Option<String>,
    current: Option<i64>,
    max: Option<i64>,
}

impl Update {
    pub fn new(
        key: Key,
        state: Option<Option<String>>,
        current: Option<Option<i64>>,
        max: Option<Option<i64>>,
    ) -> Update {
        Update {
            key,
            state,
            current,
            max,
        }
    }

    fn as_cmd<T>(&self, param: &str, val: &Option<Option<T>>) -> Option<Cmd>
    where
        T: ToOwned,
        <T as ToOwned>::Owned: ToRedisArgs,
    {
        let key = self.key.redis_key(param);

        val.as_ref().map(|v| match v {
            Some(_) => Cmd::set_ex(key, v.as_ref().map(|v| v.to_owned()), EXPIRE_SECONDS),
            None => Cmd::del(key),
        })
    }

    pub fn as_cmds(&self) -> impl Iterator<Item = Cmd> {
        [
            self.as_cmd("state", &self.state),
            self.as_cmd("current", &self.current),
            self.as_cmd("max", &self.max),
        ]
        .into_iter()
        .flatten()
    }
}

pub struct Store<C: redis::aio::ConnectionLike + AsyncCommands> {
    redis: C,
}

impl<C: redis::aio::ConnectionLike + AsyncCommands> Store<C> {
    pub fn new(redis: C) -> Store<C> {
        Store { redis }
    }

    pub async fn update(&mut self, update: &Update) -> Result<()> {
        for c in update.as_cmds() {
            c.query_async(&mut self.redis).await?;
        }

        Ok(())
    }

    async fn get_param<T: FromRedisValue>(&mut self, key: &Key, param: &str) -> Result<Option<T>> {
        Cmd::get(key.redis_key(param))
            .query_async(&mut self.redis)
            .await
            .map_err(|e| anyhow::Error::new(e))
    }

    pub async fn get_state(&mut self, key: &Key) -> Result<Value> {
        Ok(Value {
            state: self.get_param(key, "state").await?,
            current: self.get_param(key, "current").await?,
            max: self.get_param(key, "max").await?,
        })
    }

    pub async fn get_all_keys(&mut self, token: &str, keyprefix: &str) -> Result<HashSet<Key>> {
        let kpref = Key::try_from((token, keyprefix))?;

        Ok(self
            .redis
            .scan_match(kpref.redis_key_pattern())
            .await?
            .filter_map(|v: String| async move { Key::from_redis_key(&v).ok() })
            .collect::<HashSet<Key>>()
            .await)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use redis::aio::ConnectionManager;

    use crate::store::{Store, Update};

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let conm = ConnectionManager::new(client).await?;

        let mut store = Store::new(conm);

        store
            .update(&Update::new(
                ("randomtoken", "some:key").try_into()?,
                None,
                Some(Some(0)),
                Some(Some(10)),
            ))
            .await?;

        let keys = store.get_all_keys("randomtoken", "some:").await?;

        println!("{:?}", &keys);

        for key in keys {
            let st = store.get_state(&key).await?;
            println!("{:?} .. {:?}", key, st);
        }

        Ok(())
    }
}
