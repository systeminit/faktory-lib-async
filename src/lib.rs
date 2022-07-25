mod protocol;
mod error;

use crate::protocol::BeatReply;
pub use crate::protocol::{BatchConfig, BeatState, FailConfig, HelloConfig};
pub use crate::error::{Result, Error};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Config {
    //password?: string;
    //labels: string[];
    pub worker_id: Option<String>,
    pub hostname: Option<String>,
    pub uri: String,
}

impl Config {
    pub fn from_uri(uri: &str, hostname: Option<String>, worker_id: Option<String>) -> Self {
        Self {
            uri: uri.to_owned(),
            hostname,
            worker_id,
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    config: Config,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    last_beat: BeatState,
}

impl Connection {
    pub async fn new(config: Config) -> Result<Self> {
        let (reader, writer) = TcpStream::connect(&config.uri).await?.into_split();
        let mut conn = Connection {
            config,
            reader: BufReader::new(reader),
            writer,
            last_beat: BeatState::Ok,
        };
        // TODO: properly parse the HI response
        conn.validate_response("HI {\"v\":2}").await?;
        conn.default_hello().await?;

        Ok(conn)
    }

    pub async fn default_hello(&mut self) -> Result<()> {
        // TODO: improve hello config usage
        let mut config = HelloConfig::default();
        config.pid = Some(std::process::id() as usize);
        config.labels = vec!["faktory-async-rust".to_owned()];
        config.wid = self.config.worker_id.clone();
        self.hello(config).await?;

        Ok(())
    }

    pub fn last_beat(&self) -> BeatState {
        self.last_beat
    }

    pub async fn end(&mut self) -> Result<()> {
        self.send_command("END", vec![]).await?;
        Ok(())
    }

    // TODO: handle extra arguments: {wid: String, current_state: String, rss_kb: Integer}
    // https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md#beat-command
    pub async fn beat(&mut self) -> Result<BeatState> {
        self.send_command(
            "BEAT",
            vec![serde_json::to_string(
                &serde_json::json!({ "wid": self.config.worker_id }),
            )?],
        )
        .await?;
        match self.read_string().await?.as_deref() {
            Some("OK") => {
                self.last_beat = BeatState::Ok;
                Ok(BeatState::Ok)
            }
            Some(output) => {
                self.last_beat = serde_json::from_str::<BeatReply>(output)?.state;
                Ok(self.last_beat)
            }
            None => Err(Error::ReceivedEmptyMessage),
        }
    }

    pub async fn hello(&mut self, config: HelloConfig) -> Result<()> {
        self.send_command("HELLO", vec![serde_json::to_string(&config)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fetch(&mut self, queues: &[String]) -> Result<Option<Job>> {
        if queues.is_empty() {
            self.send_command("FETCH", vec![]).await?;
        } else {
            self.send_command("FETCH", queues.to_owned()).await?;
        }
        Ok(self
            .read_string()
            .await?
            .map(|msg| serde_json::from_str(&msg))
            .transpose()?)
    }

    pub async fn ack(&mut self, jid: String) -> Result<()> {
        self.send_command(
            "ACK",
            vec![serde_json::to_string(&serde_json::json!({ "jid": jid }))?],
        )
        .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fail(&mut self, config: FailConfig) -> Result<()> {
        self.send_command("FAIL", vec![serde_json::to_string(&config)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn push(&mut self, job: Job) -> Result<()> {
        self.send_command("PUSH", vec![serde_json::to_string(&job)?])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn batch_new(&mut self, config: BatchConfig) -> Result<String> {
        self.send_command("BATCH NEW", vec![serde_json::to_string(&config)?])
            .await?;
        self.read_string().await?.ok_or(Error::ReceivedEmptyMessage)
    }

    pub async fn batch_commit(&mut self, bid: String) -> Result<()> {
        self.send_command("BATCH COMMIT", vec![bid]).await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    async fn send_command(&mut self, key: &'static str, args: Vec<String>) -> Result<()> {
        let mut args = vec![key.into(), args.into_iter().collect::<Vec<_>>().join(" ")].join(" ");
        args.push_str("\r\n");
        self.writer.write_all(dbg!(&args).as_bytes()).await?;
        Ok(())
    }

    async fn read_string(&mut self) -> Result<Option<String>> {
        let mut output = String::new();
        self.reader.read_line(&mut output).await?;

        if dbg!(&output).is_empty() {
            return Err(Error::ReceivedEmptyMessage);
        }
        if !output.ends_with("\r\n") {
            return Err(Error::MissingCarriageReturn);
        }

        match output.remove(0) {
            '$' => {
                if output == "-1\r\n" {
                    return Ok(None);
                }

                let len: usize = output[0..output.len() - 2].parse()?;
                let mut output = vec![0; len];
                self.reader.read_exact(&mut output).await?;
                self.reader.read_exact(&mut [0; 2]).await?;
                Ok(Some(String::from_utf8(output)?))
            }
            '+' => {
                output.truncate(output.len() - 2);
                Ok(Some(output))
            }
            '-' => {
                let (kind, msg) = output
                    .split_once(' ')
                    .ok_or_else(|| Error::ReceivedInvalidErrorMessage(output.clone()))?;
                Err(Error::ReceivedErrorMessage(
                    kind.to_owned(),
                    msg[..msg.len() - 2].to_owned(),
                ))
            }
            prefix => Err(Error::InvalidMessagePrefix(format!("{prefix}{output}"))),
        }
    }

    async fn validate_response(&mut self, expected: &str) -> Result<()> {
        let output = self
            .read_string()
            .await?
            .ok_or(Error::ReceivedEmptyMessage)?;
        if output != expected {
            return Err(Error::UnexpectedResponse(output, expected.to_owned()))?;
        }
        Ok(())
    }
}

/// A Faktory job.
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Job {
    /// The job's unique identifier.
    pub(crate) jid: String,

    /// The queue this job belongs to. Usually `default`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// The job's type. Called `kind` because `type` is reserved.
    #[serde(rename = "jobtype")]
    pub(crate) kind: String,

    /// The arguments provided for this job.
    pub(crate) args: Vec<serde_json::Value>,

    /// When this job was created.
    // note that serializing works correctly here since the default chrono serialization
    // is RFC3339, which is also what Faktory expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    /// When this job was supplied to the Faktory server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enqueued_at: Option<DateTime<Utc>>,

    /// When this job is scheduled for.
    ///
    /// Defaults to immediately.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,

    /// How long to allow this job to run for.
    ///
    /// Defaults to 600 seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserve_for: Option<usize>,

    /// Number of times to retry this job.
    ///
    /// Defaults to 25.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<isize>,

    /// The priority of this job from 1-9 (9 is highest).
    ///
    /// Pushing a job with priority 9 will effectively put it at the front of the queue.
    /// Defaults to 5.
    pub priority: Option<u8>,

    /// Number of lines of backtrace to keep if this job fails.
    ///
    /// Defaults to 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<usize>,

    /// Data about this job's most recent failure.
    ///
    /// This field is read-only.
    #[serde(skip_serializing)]
    failure: Option<Failure>,

    /// Extra context to include with the job.
    ///
    /// Faktory workers can have plugins and middleware which need to store additional context with
    /// the job payload. Faktory supports a custom hash to store arbitrary key/values in the JSON.
    /// This can be extremely helpful for cross-cutting concerns which should propagate between
    /// systems, e.g. locale for user-specific text translations, request_id for tracing execution
    /// across a complex distributed system, etc.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default = "HashMap::default")]
    pub custom: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Failure {
    retry_count: usize,
    failed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errtype")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

impl Job {
    /// Create a new job of type `kind`, with the given arguments.
    pub fn new<S, A>(kind: S, args: Vec<A>) -> Self
    where
        S: Into<String>,
        A: Into<serde_json::Value>,
    {
        let random_jid = Uuid::new_v4().to_string();
        Job {
            jid: random_jid,
            queue: Some("default".into()),
            kind: kind.into(),
            args: args.into_iter().map(|s| s.into()).collect(),

            created_at: Some(Utc::now()),
            enqueued_at: None,
            at: None,
            reserve_for: Some(600),
            retry: Some(25),
            priority: Some(5),
            backtrace: Some(0),
            failure: None,
            custom: Default::default(),
        }
    }

    /// Place this job on the given `queue`.
    ///
    /// If this method is not called (or `self.queue` set otherwise), the queue will be set to
    /// "default".
    pub fn on_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// This job's id.
    pub fn id(&self) -> &str {
        &self.jid
    }

    /// This job's type.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The arguments provided for this job.
    pub fn args(&self) -> &[serde_json::Value] {
        &self.args
    }

    /// Data about this job's most recent failure.
    pub fn failure(&self) -> &Option<Failure> {
        &self.failure
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
