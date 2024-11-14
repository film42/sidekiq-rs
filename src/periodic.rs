use super::Result;
use crate::{new_jid, Error, Job, Processor, RedisConnection, RedisPool, RetryOpts, Worker};
pub use cron_clock::{Schedule as Cron, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::str::FromStr;

pub fn parse(cron: &str) -> Result<Cron> {
    Ok(Cron::from_str(cron)?)
}

pub async fn destroy_all(redis: RedisPool) -> Result<()> {
    let mut conn = redis.get().await?;
    conn.del("periodic".to_string()).await?;
    Ok(())
}

pub struct Builder {
    pub(crate) name: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) args: Option<JsonValue>,
    pub(crate) retry: Option<RetryOpts>,
    pub(crate) cron: Cron,
}

pub fn builder(cron_str: &str) -> Result<Builder> {
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

    #[must_use]
    pub fn retry<RO>(self, retry: RO) -> Builder
    where
        RO: Into<RetryOpts>,
    {
        Self {
            retry: Some(retry.into()),
            ..self
        }
    }

    pub fn queue<S: Into<String>>(self, queue: S) -> Builder {
        Builder {
            queue: Some(queue.into()),
            ..self
        }
    }
    pub fn args<Args>(self, args: Args) -> Result<Builder>
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

        Ok(Self {
            args: Some(args),
            ..self
        })
    }

    pub async fn register<W, Args>(self, processor: &mut Processor, worker: W) -> Result<()>
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

    pub fn into_periodic_job(&self, class_name: String) -> Result<PeriodicJob> {
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

        pj.retry.clone_from(&self.retry);
        pj.queue.clone_from(&self.queue);
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
    retry: Option<RetryOpts>,

    #[serde(skip)]
    cron_schedule: Option<Cron>,

    #[serde(skip)]
    json_args: Option<JsonValue>,
}

impl PeriodicJob {
    pub fn from_periodic_job_string(periodic_job_str: String) -> Result<Self> {
        let mut pj: Self = serde_json::from_str(&periodic_job_str)?;
        pj.hydrate_attributes()?;
        Ok(pj)
    }

    fn hydrate_attributes(&mut self) -> Result<()> {
        self.cron_schedule = Some(Cron::from_str(&self.cron)?);
        self.json_args = if let Some(ref args) = self.args {
            Some(serde_json::from_str(args)?)
        } else {
            Some(JsonValue::Null)
        };
        Ok(())
    }

    pub async fn insert(&self, conn: &mut RedisConnection) -> Result<bool> {
        let payload = serde_json::to_string(self)?;
        self.update(conn, &payload).await
    }

    pub async fn update(&self, conn: &mut RedisConnection, periodic_job_str: &str) -> Result<bool> {
        if let Some(next_scheduled_time) = self.next_scheduled_time() {
            // [ZADD key CH score value] will return true/ false if the value added changed
            // when we submit it to redis. We can use this to determine if we were the lucky
            // process that changed the periodic job to its next scheduled time and enqueue
            // the job.
            return Ok(conn
                .zadd_ch(
                    "periodic".to_string(),
                    periodic_job_str,
                    next_scheduled_time,
                )
                .await?);
        }

        Err(Error::Message(format!(
            "Unable to fetch next schedled time for periodic job: class: {}, name: {}",
            &self.class, &self.name
        )))
    }

    #[must_use]
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

    #[must_use]
    pub fn into_job(&self) -> Job {
        let args = self.json_args.clone().expect("always set in contructor");

        Job {
            queue: self.queue.clone().unwrap_or_else(|| "default".to_string()),
            class: self.class.clone(),
            jid: new_jid(),
            created_at: chrono::Utc::now().timestamp() as f64,
            enqueued_at: None,
            retry: self.retry.clone().unwrap_or(RetryOpts::Never),
            args,

            // Make default eventually...
            error_message: None,
            error_class: None,
            failed_at: None,
            retry_count: None,
            retried_at: None,
            retry_queue: None,

            // Meta data not used in periodic jobs right now...
            unique_for: None,
        }
    }
}
