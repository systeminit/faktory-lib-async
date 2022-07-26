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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct BeatReply {
    pub state: BeatState,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BeatState {
    Ok,
    Quiet,
    Terminate,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct FailConfig {
    jid: String,
    message: String,
    errtype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backtrace: Option<Vec<String>>,
}

impl FailConfig {
    pub fn new(
        jid: String,
        mut message: String,
        errtype: String,
        backtrace: Option<Vec<String>>,
    ) -> Self {
        message.truncate(1000);
        Self {
            jid,
            message,
            errtype,
            backtrace,
        }
    }
}

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
pub struct BatchConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_bid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    success: Option<Job>,
    #[serde(skip_serializing_if = "Option::is_none")]
    complete: Option<Job>,
}

#[derive(Serialize)]
pub struct HelloConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    #[serde(rename = "v")]
    version: usize,

    /// Hash is hex(sha256(password + salt))
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl Default for HelloConfig {
    fn default() -> Self {
        Self {
            hostname: None,
            wid: None,
            pid: None,
            labels: Vec::new(),
            password_hash: None,
            version: 2,
        }
    }
}

/// A Faktory job.
///
/// See also the [Faktory wiki](https://github.com/contribsys/faktory/wiki/The-Job-Payload).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Job {
    /// The job's unique identifier.
    pub jid: String,

    /// The queue this job belongs to. Usually `default`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// The job's type. Called `kind` because `type` is reserved.
    #[serde(rename = "jobtype")]
    pub kind: String,

    /// The arguments provided for this job.
    pub args: Vec<serde_json::Value>,

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
    pub failure: Option<Failure>,

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
