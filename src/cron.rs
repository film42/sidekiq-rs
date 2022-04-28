use crate::{new_jid, Job, Processor, Worker};
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
pub use cron_clock::{Schedule as Cron, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::str::FromStr;

pub fn parse(cron: &str) -> Result<Cron, Box<dyn std::error::Error>> {
    Ok(Cron::from_str(cron)?)
}

pub async fn destroy_all(
    redis: Pool<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.del("periodic").await?;
    Ok(())
}

pub struct Builder {
    pub(crate) name: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) args: Option<JsonValue>,
    pub(crate) retry: Option<bool>,
    pub(crate) cron: Cron,
}

pub fn builder(cron_str: &str) -> Result<Builder, Box<dyn std::error::Error>> {
    Ok(Builder {
        name: None,
        queue: None,
        args: None,
        retry: None,
        cron: Cron::from_str(cron_str)?,
    })
}

impl Builder {
    pub fn name<S: Into<String>>(self, name: S) -> Builder {
        Builder {
            name: Some(name.into()),
            ..self
        }
    }
    pub fn queue<S: Into<String>>(self, queue: S) -> Builder {
        Builder {
            queue: Some(queue.into()),
            ..self
        }
    }
    pub fn args<Args>(self, args: Args) -> Result<Builder, Box<dyn std::error::Error>>
    where
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + serde::Serialize + 'static,
    {
        let args = serde_json::to_value(args)?;

        // Ensure args are always wrapped in an array.
        let args = if args.is_array() {
            args
        } else {
            JsonValue::Array(vec![args])
        };

        Ok(Builder {
            args: Some(args),
            ..self
        })
    }
    pub fn retry(self, retry: bool) -> Builder {
        Builder {
            retry: Some(retry),
            ..self
        }
    }

    pub async fn register<W, Args>(
        self,
        processor: &mut Processor,
        worker: W,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
        W: Worker<Args> + 'static,
    {
        processor.register(worker);
        processor
            .register_periodic(self.into_periodic_job(W::class_name())?)
            .await?;

        Ok(())
    }

    pub fn into_periodic_job(
        &self,
        class_name: String,
    ) -> Result<PeriodicJob, Box<dyn std::error::Error>> {
        let name = self
            .name
            .clone()
            .unwrap_or_else(|| "Scheduled PeriodicJob".into());

        let mut pj = PeriodicJob {
            name,
            class: class_name,
            cron: self.cron.to_string(),

            ..Default::default()
        };

        pj.retry = self.retry.clone();
        pj.queue = self.queue.clone();
        pj.args = self.args.clone().map(|a| a.to_string());

        pj.hydrate_attributes()?;

        Ok(pj)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PeriodicJob {
    pub(crate) name: String,
    pub(crate) class: String,
    pub(crate) cron: String,
    pub(crate) queue: Option<String>,
    pub(crate) args: Option<String>,
    retry: Option<bool>,

    #[serde(skip)]
    cron_schedule: Option<Cron>,

    #[serde(skip)]
    json_args: Option<JsonValue>,
}

impl PeriodicJob {
    pub fn from_periodic_job_string(
        periodic_job_str: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pj: Self = serde_json::from_str(&periodic_job_str)?;
        pj.hydrate_attributes()?;
        Ok(pj)
    }

    fn hydrate_attributes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.cron_schedule = Some(Cron::from_str(&self.cron)?);
        self.json_args = if let Some(ref args) = self.args {
            Some(serde_json::from_str(&args)?)
        } else {
            Some(JsonValue::Null)
        };
        Ok(())
    }

    pub async fn insert(
        &self,
        conn: &mut redis::aio::Connection,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let payload = serde_json::to_string(self)?;
        self.update(conn, &payload).await
    }

    pub async fn update(
        &self,
        conn: &mut redis::aio::Connection,
        periodic_job_str: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(next_scheduled_time) = self.next_scheduled_time() {
            // [ZADD key CH score value] will return true/ false if the value added changed
            // when we submit it to redis. We can use this to determine if we were the lucky
            // process that changed the periodic job to its next scheduled time.
            return Ok(redis::cmd("ZADD")
                .arg("periodic")
                .arg("CH")
                .arg(next_scheduled_time)
                .arg(periodic_job_str)
                .query_async(&mut *conn)
                .await?);
        }

        Err(format!(
            "Unable to fetch next schedled time for periodic job: class: {}, name: {}",
            &self.class, &self.name
        )
        .into())
    }

    pub fn next_scheduled_time(&self) -> Option<f64> {
        if let Some(ref cron_sched) = self.cron_schedule {
            cron_sched
                .upcoming(Utc)
                .next()
                .map(|dt| dt.timestamp() as f64)
        } else {
            None
        }
    }

    pub fn into_job(&self) -> Job {
        let args = self.json_args.clone().expect("always set in contructor");

        Job {
            queue: self.queue.clone().unwrap_or_else(|| "default".to_string()),
            class: self.class.clone(),
            jid: new_jid(),
            created_at: chrono::Utc::now().timestamp() as f64,
            enqueued_at: None,
            retry: self.retry.clone().unwrap_or(false),
            args,

            // Make default eventually...
            error_message: None,
            failed_at: None,
            retry_count: None,
            retried_at: None,
        }
    }
}
