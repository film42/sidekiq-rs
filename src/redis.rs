use bb8::{ManageConnection, Pool, PooledConnection};

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::ToRedisArgs;
use redis::{aio::Connection, ErrorKind};
use redis::{Client, IntoConnectionInfo, RedisError};
use std::ops::DerefMut;

pub type RedisPool = Pool<RedisConnectionManager>;

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

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn.deref_mut().connection)
            .await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// A wrapper type for making the redis crate compatible with namespacing.
pub struct RedisConnection {
    connection: Connection,
    namespace: Option<String>,
}

impl RedisConnection {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            namespace: None,
        }
    }

    pub fn with_namespace(self, namespace: String) -> Self {
        Self {
            connection: self.connection,
            namespace: Some(namespace),
        }
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

    pub fn borrow_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    pub async fn del(&mut self, key: String) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self.connection.del(self.namespaced_key(key)).await?)
    }

    pub async fn brpop(
        &mut self,
        keys: Vec<String>,
        timeout: usize,
    ) -> Result<Option<(String, String)>, Box<dyn std::error::Error>> {
        Ok(self
            .connection
            .brpop(self.namespaced_keys(keys), timeout)
            .await?)
    }
    //    pub async fn brpop_direct(
    //        &self,
    //        conn: &mut RedisConnection,
    //        keys: Vec<String>,
    //        timeout: usize,
    //    ) -> Result<Option<(String, String)>, Box<dyn std::error::Error>> {
    //        Ok(conn.brpop(self.namespaced_keys(keys), timeout).await?)
    //    }

    pub async fn zadd<V: ToRedisArgs + Send + Sync, S: ToRedisArgs + Send + Sync>(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self
            .connection
            .zadd(self.namespaced_key(key), value, score)
            .await?)
    }
    //    pub async fn zadd_direct(
    //        &self,
    //        conn: &mut RedisConnection,
    //        key: String,
    //        value: String,
    //        score: f64,
    //    ) -> Result<usize, Box<dyn std::error::Error>> {
    //        Ok(conn.zadd(self.namespaced_key(key), value, score).await?)
    //    }

    pub async fn rpush(
        &mut self,
        key: String,
        value: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self
            .connection
            .rpush(self.namespaced_key(key), value)
            .await?)
    }
    //    pub async fn rpush_direct(
    //        &self,
    //        conn: &mut RedisConnection,
    //        key: String,
    //        value: String,
    //    ) -> Result<(), Box<dyn std::error::Error>> {
    //        conn.rpush(self.namespaced_key(key), value).await?;
    //
    //        Ok(())
    //    }

    pub async fn zrangebyscore_limit<L: ToRedisArgs + Send + Sync, U: ToRedisArgs + Sync + Send>(
        &mut self,
        key: String,
        lower: L,
        upper: U,
        offset: isize,
        limit: isize,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(self
            .connection
            .zrangebyscore_limit(self.namespaced_key(key), lower, upper, offset, limit)
            .await?)
    }
    //    pub async fn zrangebyscore_limit_direct(
    //        &self,
    //        conn: &mut RedisConnection,
    //        keys: Vec<String>,
    //        lower: &str,
    //        upper: &str,
    //        offset: isize,
    //        limit: isize,
    //    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    //        Ok(conn
    //            .zrangebyscore_limit(self.namespaced_keys(keys), lower, upper, offset, limit)
    //            .await?)
    //    }

    pub async fn zrem(
        &mut self,
        key: String,
        value: String,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self
            .connection
            .zrem(self.namespaced_key(key), value)
            .await?)
    }
    //    pub async fn zrem_direct(
    //        &self,
    //        conn: &mut RedisConnection,
    //        key: String,
    //        value: String,
    //    ) -> Result<(), Box<dyn std::error::Error>> {
    //        conn.zrem(self.namespaced_key(key), value).await?;
    //        Ok(())
    //    }
}
