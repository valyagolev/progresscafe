use std::collections::HashSet;

use anyhow::{anyhow, Result};
use futures::{Future, StreamExt};
use redis::{aio::ConnectionManager, Cmd, FromRedisValue, ToRedisArgs};
use redis::{AsyncCommands, RedisResult};

const EXPIRE_SECONDS: usize = 60 * 60 * 4;

pub fn check_string(s: &str) -> Result<&str> {
    let r = s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.');

    if r {
        Ok(s)
    } else {
        Err(anyhow!("must be {}", STRING_REQ))
    }
}

pub fn parse_i64_or_null(s: &str) -> Result<Option<i64>> {
    if s.to_ascii_lowercase() == "null" {
        Ok(None)
    } else {
        Ok(Some(s.parse()?))
    }
}

const STRING_REQ: &str = "[a-z0-9_.-]";

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    token: String,
    pub key: String,
}

impl<S1: Into<String>, S2: Into<String>> TryFrom<(S1, S2)> for Key {
    fn try_from((token, key): (S1, S2)) -> Result<Self> {
        let token = token.into();
        check_string(&token)?;

        Ok(Key {
            token,
            key: key.into(),
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
        let vr: Result<Vec<&str>> = redis_key.split(":").map(check_string).collect();

        let v = vr?;

        match (v[0], v[1], &v[2..v.len() - 1]) {
            ("pcafe", token, key) => Ok(Key {
                token: token.to_owned(),
                key: key.join(":"),
            }),
            _ => Err(anyhow!("Bad structure: {:?}", v)),
        }
    }
}

#[derive(Debug)]
pub struct Update {
    key: Key,
    state: Option<String>,
    current: Option<Option<i64>>,
    max: Option<Option<i64>>,
}

#[derive(Debug)]
pub struct Value {
    pub state: Option<String>,
    pub current: Option<i64>,
    pub max: Option<i64>,
}

impl Update {
    pub fn new(
        key: Key,
        state: Option<String>,
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
            self.as_cmd("state", &Some(self.state.as_ref())),
            self.as_cmd("current", &self.current),
            self.as_cmd("max", &self.max),
        ]
        .into_iter()
        .flatten()
    }

    pub fn from_query(token: &str, (key, val): (String, String)) -> Result<Self> {
        let (state, rest) = match val.split_once("!") {
            Some((state, rest)) => (Some(check_string(state)?.to_owned()), rest),
            None => (None, val.as_ref()),
        };
        let (current, max) = match rest.split_once("/") {
            Some((c, m)) => (c, Some(parse_i64_or_null(m)?)),
            None => (rest, None),
        };

        let current = if current == "" {
            None
        } else {
            Some(parse_i64_or_null(current)?)
        };

        Ok(Update {
            key: (token.to_owned(), key).try_into()?,
            state,
            current,
            max,
        })
    }
}

#[derive(Clone)]
pub struct Store<C: redis::aio::ConnectionLike + AsyncCommands + Clone> {
    redis: C,
}

impl<C: redis::aio::ConnectionLike + AsyncCommands + Clone> Store<C> {
    pub fn new(redis: C) -> Store<C> {
        Store { redis }
    }

    pub async fn update(&self, update: &Update) -> Result<()> {
        for c in update.as_cmds() {
            c.query_async(&mut self.redis.clone()).await?;
        }

        Ok(())
    }

    async fn get_param<T: FromRedisValue>(&self, key: &Key, param: &str) -> Result<Option<T>> {
        Cmd::get(key.redis_key(param))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|e| anyhow::Error::new(e))
    }

    pub async fn get_state(&self, key: &Key) -> Result<Value> {
        Ok(Value {
            state: self.get_param(key, "state").await?,
            current: self.get_param(key, "current").await?,
            max: self.get_param(key, "max").await?,
        })
    }

    pub async fn get_all_keys(&self, token: &str, keyprefix: &str) -> Result<HashSet<Key>> {
        let kpref = Key::try_from((token.to_owned(), keyprefix.to_owned()))?;

        Ok(self
            .redis
            .clone()
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

        let store = Store::new(conm);

        store
            .update(&Update::new(
                ("randomtoken", "some:key").try_into()?,
                None,
                Some(Some(0)),
                Some(Some(10)),
            ))
            .await?;

        let keys = store.get_all_keys("randomtoken", "some:").await?;

        println!("get_all_keys {:?}", &keys);

        for key in keys {
            let st = store.get_state(&key).await?;
            println!("{:?} .. {:?}", key, st);
        }

        Ok(())
    }
}
