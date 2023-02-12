use bb8::{CustomizeConnection, ManageConnection, Pool};

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::ToRedisArgs;
pub use redis::Value as RedisValue;
use redis::{aio::Connection, ErrorKind};
use redis::{Client, IntoConnectionInfo};
use std::ops::DerefMut;

pub use redis::RedisError;

pub type RedisPool = Pool<RedisConnectionManager>;

#[derive(Debug)]
pub struct NamespaceCustomizer {
    namespace: String,
}

#[async_trait]
impl CustomizeConnection<RedisConnection, RedisError> for NamespaceCustomizer {
    async fn on_acquire(&self, connection: &mut RedisConnection) -> Result<(), RedisError> {
        // All redis operations used by the sidekiq lib will use this as a prefix.
        connection.set_namespace(self.namespace.clone());

        Ok(())
    }
}

pub fn with_custom_namespace(namespace: String) -> Box<NamespaceCustomizer> {
    Box::new(NamespaceCustomizer { namespace })
}

/// A `bb8::ManageConnection` for `redis::Client::get_async_connection` wrapped in a helper type
/// for namespacing.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    /// See `redis::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(info: T) -> Result<Self, RedisError> {
        Ok(RedisConnectionManager {
            client: Client::open(info.into_connection_info()?)?,
        })
    }
}

#[async_trait]
impl ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(RedisConnection::new(
            self.client.get_async_connection().await?,
        ))
    }

    async fn is_valid(&self, mut conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn.deref_mut().connection)
            .await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.has_broken()
    }
}

/// A wrapper type for making the redis crate compatible with namespacing.
pub struct RedisConnection {
    connection: Connection,
    namespace: Option<String>,
    has_broken: bool,
}

impl RedisConnection {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            namespace: None,
            has_broken: false,
        }
    }

    pub fn has_broken(&self) -> bool {
        self.has_broken
    }

    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = Some(namespace);
    }

    pub fn with_namespace(self, namespace: String) -> Self {
        Self {
            connection: self.connection,
            namespace: Some(namespace),
            has_broken: self.has_broken,
        }
    }

    fn check_connection(&mut self, err: RedisError) -> RedisError {
        if err.is_io_error() {
            self.has_broken = true;
        }
        err
    }

    fn namespaced_key(&self, key: String) -> String {
        if let Some(ref namespace) = self.namespace {
            return format!("{namespace}:{key}");
        }

        key
    }

    fn namespaced_keys(&self, keys: Vec<String>) -> Vec<String> {
        if let Some(ref namespace) = self.namespace {
            let keys: Vec<String> = keys
                .iter()
                .map(|key| format!("{namespace}:{key}"))
                .collect();

            return keys;
        }

        keys
    }

    /// This allows you to borrow the raw redis connection without any namespacing support.
    pub fn unnamespaced_borrow_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    pub async fn brpop(
        &mut self,
        keys: Vec<String>,
        timeout: usize,
    ) -> Result<Option<(String, String)>, RedisError> {
        Ok(self
            .connection
            .brpop(self.namespaced_keys(keys), timeout)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub fn cmd_with_key(&mut self, cmd: &str, key: String) -> redis::Cmd {
        let mut c = redis::cmd(cmd);
        c.arg(self.namespaced_key(key));
        c
    }

    pub async fn del(&mut self, key: String) -> Result<usize, RedisError> {
        Ok(self
            .connection
            .del(self.namespaced_key(key))
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn expire(&mut self, key: String, value: usize) -> Result<usize, RedisError> {
        Ok(self
            .connection
            .expire(self.namespaced_key(key), value)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn lpush(&mut self, key: String, value: String) -> Result<(), RedisError> {
        Ok(self
            .connection
            .lpush(self.namespaced_key(key), value)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn sadd(&mut self, key: String, value: String) -> Result<(), RedisError> {
        Ok(self
            .connection
            .sadd(self.namespaced_key(key), value)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn set_nx_ex(
        &mut self,
        key: String,
        value: String,
        ttl_in_seconds: usize,
    ) -> Result<RedisValue, RedisError> {
        Ok(redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_in_seconds)
            .query_async(self.unnamespaced_borrow_mut())
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn zrange(
        &mut self,
        key: String,
        lower: isize,
        upper: isize,
    ) -> Result<Vec<String>, RedisError> {
        Ok(self
            .connection
            .zrange(self.namespaced_key(key), lower, upper)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn zrangebyscore_limit<L: ToRedisArgs + Send + Sync, U: ToRedisArgs + Sync + Send>(
        &mut self,
        key: String,
        lower: L,
        upper: U,
        offset: isize,
        limit: isize,
    ) -> Result<Vec<String>, RedisError> {
        Ok(self
            .connection
            .zrangebyscore_limit(self.namespaced_key(key), lower, upper, offset, limit)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn zadd<V: ToRedisArgs + Send + Sync, S: ToRedisArgs + Send + Sync>(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<usize, RedisError> {
        Ok(self
            .connection
            .zadd(self.namespaced_key(key), value, score)
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn zadd_ch<V: ToRedisArgs + Send + Sync, S: ToRedisArgs + Send + Sync>(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<bool, RedisError> {
        Ok(redis::cmd("ZADD")
            .arg(self.namespaced_key(key))
            .arg("CH")
            .arg(score)
            .arg(value)
            .query_async(self.unnamespaced_borrow_mut())
            .await
            .map_err(|err| self.check_connection(err))?)
    }

    pub async fn zrem(&mut self, key: String, value: String) -> Result<bool, RedisError> {
        Ok(self
            .connection
            .zrem(self.namespaced_key(key), value)
            .await
            .map_err(|err| self.check_connection(err))?)
    }
}
