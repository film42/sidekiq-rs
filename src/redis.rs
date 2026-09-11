use bb8::{CustomizeConnection, ManageConnection, Pool};
use redis::AsyncCommands;
pub use redis::RedisError;
use redis::ToRedisArgs;
pub use redis::Value as RedisValue;
use redis::aio::MultiplexedConnection as Connection;
use redis::{Client, IntoConnectionInfo};
use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;

pub type RedisPool = Pool<RedisConnectionManager>;

#[derive(Debug)]
pub struct NamespaceCustomizer {
    namespace: String,
}

impl CustomizeConnection<RedisConnection, RedisError> for NamespaceCustomizer {
    fn on_acquire<'a>(
        &'a self,
        connection: &'a mut RedisConnection,
    ) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + 'a>> {
        Box::pin(async {
            // All redis operations used by the sidekiq lib will use this as a prefix.
            connection.set_namespace(self.namespace.clone());

            Ok(())
        })
    }
}

#[must_use]
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
        Ok(Self {
            client: Client::open(info.into_connection_info()?)?,
        })
    }
}

impl ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // Disable response timeout because this connection is used for blocking
        // commands like BRPOP which can legitimately block for seconds.
        let config = redis::AsyncConnectionConfig::new().set_response_timeout(None);
        Ok(RedisConnection::new(
            self.client
                .get_multiplexed_async_connection_with_config(&config)
                .await?,
        ))
    }

    async fn is_valid(&self, mut conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn.deref_mut().connection)
            .await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                "ping request",
            ))),
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// A wrapper type for making the redis crate compatible with namespacing.
pub struct RedisConnection {
    connection: Connection,
    namespace: Option<String>,
}

impl RedisConnection {
    #[must_use]
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            namespace: None,
        }
    }

    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = Some(namespace);
    }

    #[must_use]
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

    /// This allows you to borrow the raw redis connection without any namespacing support.
    pub fn unnamespaced_borrow_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    pub async fn brpop(
        &mut self,
        keys: Vec<String>,
        timeout: usize,
    ) -> Result<Option<(String, String)>, RedisError> {
        self.connection
            .brpop(self.namespaced_keys(keys), timeout as f64)
            .await
    }

    pub fn cmd_with_key(&mut self, cmd: &str, key: String) -> redis::Cmd {
        let mut c = redis::cmd(cmd);
        c.arg(self.namespaced_key(key));
        c
    }

    pub async fn del(&mut self, key: String) -> Result<usize, RedisError> {
        self.connection.del(self.namespaced_key(key)).await
    }

    pub async fn expire(&mut self, key: String, value: usize) -> Result<usize, RedisError> {
        self.connection
            .expire(self.namespaced_key(key), value as i64)
            .await
    }

    pub async fn lpush<V>(&mut self, key: String, value: V) -> Result<(), RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.lpush(self.namespaced_key(key), value).await
    }

    pub async fn sadd<V>(&mut self, key: String, value: V) -> Result<(), RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.sadd(self.namespaced_key(key), value).await
    }

    pub async fn hset(
        &mut self,
        key: String,
        field: String,
        value: String,
    ) -> Result<(), RedisError> {
        self.connection
            .hset(self.namespaced_key(key), field, value)
            .await
    }

    pub async fn hdel(&mut self, key: String, field: String) -> Result<(), RedisError> {
        self.connection.hdel(self.namespaced_key(key), field).await
    }

    pub async fn srem<V>(&mut self, key: String, value: V) -> Result<(), RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.srem(self.namespaced_key(key), value).await
    }

    pub async fn unlink(&mut self, key: String) -> Result<(), RedisError> {
        let _: i64 = self.connection.unlink(self.namespaced_key(key)).await?;
        Ok(())
    }

    /// Pipeline SREM + UNLINK in a single round-trip. Both keys are namespaced.
    /// Equivalent to Ruby's `conn.pipelined { |p| p.srem(set, member); p.unlink(hash_key) }`.
    pub async fn srem_and_unlink(
        &mut self,
        set_key: String,
        member: String,
        hash_key: String,
    ) -> Result<(), RedisError> {
        redis::pipe()
            .cmd("SREM")
            .arg(self.namespaced_key(set_key))
            .arg(&member)
            .cmd("UNLINK")
            .arg(self.namespaced_key(hash_key))
            .query_async::<()>(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn set_nx_ex<V>(
        &mut self,
        key: String,
        value: V,
        ttl_in_seconds: usize,
    ) -> Result<RedisValue, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        redis::cmd("SET")
            .arg(self.namespaced_key(key))
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_in_seconds)
            .query_async(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn zrange(
        &mut self,
        key: String,
        lower: isize,
        upper: isize,
    ) -> Result<Vec<String>, RedisError> {
        self.connection
            .zrange(self.namespaced_key(key), lower, upper)
            .await
    }

    pub async fn zrangebyscore_limit<
        L: redis::ToSingleRedisArg + Send + Sync,
        U: redis::ToSingleRedisArg + Sync + Send,
    >(
        &mut self,
        key: String,
        lower: L,
        upper: U,
        offset: isize,
        limit: isize,
    ) -> Result<Vec<String>, RedisError> {
        self.connection
            .zrangebyscore_limit(self.namespaced_key(key), lower, upper, offset, limit)
            .await
    }

    pub async fn zadd<
        V: redis::ToSingleRedisArg + Send + Sync,
        S: redis::ToSingleRedisArg + Send + Sync,
    >(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<usize, RedisError> {
        self.connection
            .zadd(self.namespaced_key(key), value, score)
            .await
    }

    pub async fn zadd_ch<V: ToRedisArgs + Send + Sync, S: ToRedisArgs + Send + Sync>(
        &mut self,
        key: String,
        value: V,
        score: S,
    ) -> Result<bool, RedisError> {
        redis::cmd("ZADD")
            .arg(self.namespaced_key(key))
            .arg("CH")
            .arg(score)
            .arg(value)
            .query_async(self.unnamespaced_borrow_mut())
            .await
    }

    pub async fn zrem<V>(&mut self, key: String, value: V) -> Result<usize, RedisError>
    where
        V: ToRedisArgs + Send + Sync,
    {
        self.connection.zrem(self.namespaced_key(key), value).await
    }
}
