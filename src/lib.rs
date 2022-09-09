mod error;
mod protocol;

pub use crate::error::{Error, Result};
pub use crate::protocol::{BatchConfig, BeatState, Config, FailConfig, Failure, HelloConfig, Job};

use crate::protocol::BeatReply;

use std::borrow::Cow;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

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
        self.hello(&config).await?;

        Ok(())
    }

    pub fn last_beat(&self) -> BeatState {
        self.last_beat
    }

    pub async fn end(&mut self) -> Result<()> {
        self.send_command("END", &[]).await?;
        Ok(())
    }

    // TODO: handle extra arguments: {wid: String, current_state: String, rss_kb: Integer}
    // https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md#beat-command
    pub async fn beat(&mut self) -> Result<BeatState> {
        self.send_command(
            "BEAT",
            &[serde_json::to_string(&serde_json::json!({ "wid": self.config.worker_id }))?.into()],
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

    pub async fn hello(&mut self, config: &HelloConfig) -> Result<()> {
        self.send_command("HELLO", &[serde_json::to_string(config)?.into()])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fetch(&mut self, queues: &[Cow<'_, str>]) -> Result<Option<Job>> {
        if queues.is_empty() {
            self.send_command("FETCH", &[]).await?;
        } else {
            self.send_command("FETCH", queues).await?;
        }
        Ok(self
            .read_string()
            .await?
            .map(|msg| serde_json::from_str(&msg))
            .transpose()?)
    }

    pub async fn ack(&mut self, jid: &str) -> Result<()> {
        self.send_command(
            "ACK",
            &[serde_json::to_string(&serde_json::json!({ "jid": jid }))?.into()],
        )
        .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn fail(&mut self, config: &FailConfig) -> Result<()> {
        self.send_command("FAIL", &[serde_json::to_string(config)?.into()])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn push(&mut self, job: &Job) -> Result<()> {
        self.send_command("PUSH", &[serde_json::to_string(job)?.into()])
            .await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    pub async fn batch_new(&mut self, config: &BatchConfig) -> Result<String> {
        self.send_command("BATCH NEW", &[serde_json::to_string(config)?.into()])
            .await?;
        self.read_string().await?.ok_or(Error::ReceivedEmptyMessage)
    }

    pub async fn batch_commit(&mut self, bid: &str) -> Result<()> {
        self.send_command("BATCH COMMIT", &[bid.into()]).await?;
        self.validate_response("OK").await?;
        Ok(())
    }

    async fn send_command<'a>(
        &'a mut self,
        key: &'a str,
        args: &'a [Cow<'a, str>],
    ) -> Result<()> {
        let args: String = args.join(" ");
        let mut args = vec![key.into(), args].join(" ");
        args.push_str("\r\n");
        self.writer.write_all(&args.as_bytes()).await?;
        Ok(())
    }

    async fn read_string(&mut self) -> Result<Option<String>> {
        let mut output = String::new();
        self.reader.read_line(&mut output).await?;

        if output.is_empty() {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
