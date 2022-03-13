use async_trait::async_trait;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[async_trait]
trait Worker<Args>: Send + Sync {
    async fn perform(&mut self, args: Args) -> Result<(), Box<dyn std::error::Error>>;

    async fn handle(&mut self, args: JsonValue) -> Result<(), Box<dyn std::error::Error>>
    where
        for<'de> Args: Deserialize<'de>,
        Args: Send,
    {
        let mut args: Vec<Args> = serde_json::from_value(args)?;

        // How do we control this variadic argument problem? If it's an array with one object we
        // can do this, but if not we'll likely need the caller to specify a tuple type.
        let args = args.pop().unwrap();
        self.perform(args).await
    }
}

// Example:
//
// {
//   "retry": true,
//   "queue": "yolo",
//   "class": "YoloWorker",
//   "args": [
//     {
//       "yolo": "hiiii"
//     }
//   ],
//   "jid": "f33f7063c6d7a4db0869289a",
//   "created_at": 1647119929.3788748,
//   "enqueued_at": 1647119929.378998
// }
//
#[derive(Serialize, Deserialize, Debug)]
struct Job {
    queue: String,
    args: JsonValue,
    retry: bool,
    class: String,
    jid: String,
    created_at: f64,
    enqueued_at: f64,
}

#[derive(Debug)]
struct UnitOfWork {
    queue: String,
    job: Job,
}

impl UnitOfWork {
    pub async fn reenqueue(
        &self,
        client: &mut redis::aio::Connection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        client
            .rpush(&self.queue, serde_json::to_string(&self.job)?)
            .await?;

        Ok(())
    }
}

struct Processor<T: Sized> {
    redis: redis::aio::Connection,
    queues: Vec<String>,
    workers: BTreeMap<String, Box<dyn Worker<T>>>,
}

impl<T: Sized + Send + for<'de> serde::Deserialize<'de>> Processor<T> {
    pub async fn fetch(&mut self) -> Result<UnitOfWork, Box<dyn std::error::Error>> {
        let (queue, job_raw): (String, String) = self.redis.brpop(&self.queues, 0).await?;
        let job: Job = serde_json::from_str(&job_raw)?;
        // println!("{:?}", (&queue, &args));
        Ok(UnitOfWork { queue, job })
    }

    pub async fn process_one(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let work = self.fetch().await?;

        println!("WORK: {:?}", work);

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            worker.handle(work.job.args.clone()).await?;
        } else {
            // Handle missing worker.
            println!("!!! Worker not found {} !!!", &work.job.class);
            work.reenqueue(&mut self.redis).await?;
        }

        Ok(())
    }

    pub fn register<S: Into<String>>(&mut self, name: S, worker: Box<dyn Worker<T>>) {
        self.workers.insert(name.into(), worker);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_tokio_connection().await?;
    // let res = con.brpop("some:queue", 1).await?;
    // println!("Hello, world! {:?}", res);

    let worker = HelloWorker;
    HelloWorker.perform(()).await?;

    let mut p = Processor {
        redis: con,
        queues: vec!["queue:yolo".to_string()],
        workers: BTreeMap::new(),
    };

    // Add known workers
    // p.register("HelloWorker", Box::new(HelloWorker));
    p.register("YoloWorker", Box::new(YoloWorker));

    loop {
        p.process_one().await?;
    }

    //     let j = p.fetch().await?;
    //     println!("JOB: {:?}", j);
    //     j.reenqueue(&mut p.redis).await?;

    Ok(())
}

#[derive(Clone)]
struct HelloWorker;

#[async_trait]
impl Worker<()> for HelloWorker {
    // TODO: Add some kind of context here. Or should self be the context?
    async fn perform(&mut self, _args: ()) -> Result<(), Box<dyn std::error::Error>> {
        println!("Hello, I'm worker!");

        Ok(())
    }
}

#[derive(Clone)]
struct YoloWorker;

#[derive(Deserialize, Debug)]
struct YoloArgs {
    yolo: String,
}

#[async_trait]
impl Worker<YoloArgs> for YoloWorker {
    async fn perform(&mut self, args: YoloArgs) -> Result<(), Box<dyn std::error::Error>> {
        println!("YOLO: {}", &args.yolo);

        Ok(())
    }
}
