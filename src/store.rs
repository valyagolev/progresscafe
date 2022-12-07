use anyhow::{anyhow, Result};
use futures::{Future, StreamExt};
use redis::AsyncCommands;
use redis::{aio::ConnectionManager, Cmd, FromRedisValue, ToRedisArgs};

const EXPIRE_SECONDS: usize = 60 * 60 * 4;

pub fn check_string(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

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
        let v: Vec<&str> = redis_key.split(redis_key).collect();

        if !v.iter().all(|s| check_string(*s)) {
            return Err(anyhow!("Bad key"));
        }

        match (v[0], v[1], &v[2..]) {
            ("pcafe", token, key) => Ok(Key {
                token: token.to_owned(),
                key: key.join(":"),
            }),
            _ => Err(anyhow!("Bad structure")),
        }
    }
}

pub struct Update {
    key: Key,
    state: Option<Option<String>>,
    current: Option<Option<i64>>,
    max: Option<Option<i64>>,
}

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

    pub async fn exec(&mut self, update: &Update) -> Result<()> {
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

    pub async fn get_all_keys(&mut self, token: &str, keyprefix: &str) -> Result<Vec<Key>> {
        let kpref = Key::try_from((token, keyprefix))?;

        Ok(self
            .redis
            .scan_match(kpref.redis_key_pattern())
            .await?
            .filter_map(|v: String| async move { Key::from_redis_key(&v).ok() })
            .collect::<Vec<Key>>()
            .await)
    }
}
