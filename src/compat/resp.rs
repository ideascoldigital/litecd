//! The few Redis commands the Argo CD UI compatibility mode sends (AUTH,
//! SELECT, SET, GET, DEL, PUBLISH, SCAN), over RESP2 on a plain TCP
//! connection. Small enough that the feature pulls no Redis client crate;
//! TLS and Sentinel are not supported (Argo CD's chart talks to Redis, or
//! to the HA chart's HAProxy, in plain TCP).

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Where Redis is: `redis://[[user]:password@]host[:port][/db]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    pub host: String,
    pub port: u16,
    pub db: u32,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Address {
    pub fn parse(url: &str) -> Result<Address> {
        let rest = url
            .strip_prefix("redis://")
            .ok_or_else(|| anyhow!("{url}: only redis:// addresses are supported"))?;
        let (auth, rest) = match rest.rsplit_once('@') {
            Some((a, r)) => (Some(a), r),
            None => (None, rest),
        };
        let (hostport, db) = match rest.split_once('/') {
            Some((h, d)) if !d.is_empty() => (h, d.parse().context("the database number")?),
            Some((h, _)) => (h, 0),
            None => (rest, 0),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().context("the port")?),
            None => (hostport, 6379),
        };
        if host.is_empty() {
            bail!("{url}: no host");
        }
        let (username, password) = match auth {
            None => (None, None),
            Some(a) => match a.split_once(':') {
                Some((u, p)) => (
                    (!u.is_empty()).then(|| u.to_string()),
                    (!p.is_empty()).then(|| p.to_string()),
                ),
                None => (None, Some(a.to_string())),
            },
        };
        Ok(Address {
            host: host.to_string(),
            port,
            db,
            username,
            password,
        })
    }
}

/// A reply.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Status(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}

pub struct Conn {
    io: BufReader<TcpStream>,
}

const TIMEOUT: Duration = Duration::from_secs(10);

impl Conn {
    pub async fn connect(addr: &Address, password: Option<&str>) -> Result<Conn> {
        let tcp =
            tokio::time::timeout(TIMEOUT, TcpStream::connect((addr.host.as_str(), addr.port)))
                .await
                .map_err(|_| {
                    anyhow!(
                        "connecting to {}:{}: timed out (a NetworkPolicy keeping litecd out? \
                 deploy/argocd-ui-compat/networkpolicy.yaml)",
                        addr.host,
                        addr.port
                    )
                })?
                .with_context(|| format!("connecting to {}:{}", addr.host, addr.port))?;
        let mut c = Conn {
            io: BufReader::new(tcp),
        };
        if let Some(p) = addr.password.as_deref().or(password) {
            let mut cmd: Vec<&[u8]> = vec![b"AUTH"];
            if let Some(u) = &addr.username {
                cmd.push(u.as_bytes());
            }
            cmd.push(p.as_bytes());
            c.call(&cmd).await.context("AUTH")?;
        }
        if addr.db != 0 {
            c.call(&[b"SELECT", addr.db.to_string().as_bytes()])
                .await
                .context("SELECT")?;
        }
        Ok(c)
    }

    pub async fn call(&mut self, args: &[&[u8]]) -> Result<Reply> {
        tokio::time::timeout(TIMEOUT, async {
            self.io.get_mut().write_all(&encode(args)).await?;
            read_reply(&mut self.io).await
        })
        .await
        .map_err(|_| anyhow!("redis: timed out"))?
    }
}

pub fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn read_line<R: AsyncBufReadExt + Unpin>(r: &mut R) -> Result<String> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line).await?;
    if !line.ends_with(b"\r\n") {
        bail!("redis: connection closed");
    }
    line.truncate(line.len() - 2);
    Ok(String::from_utf8_lossy(&line).into_owned())
}

pub async fn read_reply<R: AsyncBufReadExt + Unpin>(r: &mut R) -> Result<Reply> {
    let line = read_line(r).await?;
    let (t, rest) = line.split_at(line.len().min(1));
    Ok(match t {
        "+" => Reply::Status(rest.to_string()),
        "-" => bail!("redis: {rest}"),
        ":" => Reply::Int(rest.parse().context("redis: integer reply")?),
        "$" => {
            let n: i64 = rest.parse().context("redis: bulk length")?;
            if n < 0 {
                Reply::Bulk(None)
            } else {
                let mut buf = vec![0; n as usize + 2];
                r.read_exact(&mut buf).await?;
                buf.truncate(n as usize);
                Reply::Bulk(Some(buf))
            }
        }
        "*" => {
            let n: i64 = rest.parse().context("redis: array length")?;
            let mut items = Vec::new();
            for _ in 0..n.max(0) {
                items.push(Box::pin(read_reply(r)).await?);
            }
            Reply::Array(items)
        }
        _ => bail!("redis: unexpected reply {line:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        let a = Address::parse("redis://argocd-redis-ha-haproxy:6379").unwrap();
        assert_eq!(
            (a.host.as_str(), a.port, a.db),
            ("argocd-redis-ha-haproxy", 6379, 0)
        );
        assert_eq!(a.password, None);
        let a = Address::parse("redis://:s3cret@redis/2").unwrap();
        assert_eq!((a.host.as_str(), a.port, a.db), ("redis", 6379, 2));
        assert_eq!(a.password.as_deref(), Some("s3cret"));
        let a = Address::parse("redis://default:pw@10.0.0.1:6380").unwrap();
        assert_eq!(a.username.as_deref(), Some("default"));
        assert_eq!(a.port, 6380);
        assert!(Address::parse("rediss://redis:6379").is_err());
        assert!(Address::parse("redis://:6379").is_err());
    }

    #[tokio::test]
    async fn replies() {
        let wire: &[u8] =
            b"+OK\r\n:3\r\n$5\r\nhe\r\no\r\n$-1\r\n*2\r\n$1\r\n0\r\n*1\r\n$3\r\nkey\r\n-ERR no\r\n";
        let mut r = BufReader::new(wire);
        assert_eq!(
            read_reply(&mut r).await.unwrap(),
            Reply::Status("OK".into())
        );
        assert_eq!(read_reply(&mut r).await.unwrap(), Reply::Int(3));
        assert_eq!(
            read_reply(&mut r).await.unwrap(),
            Reply::Bulk(Some(b"he\r\no".to_vec()))
        );
        assert_eq!(read_reply(&mut r).await.unwrap(), Reply::Bulk(None));
        assert_eq!(
            read_reply(&mut r).await.unwrap(),
            Reply::Array(vec![
                Reply::Bulk(Some(b"0".to_vec())),
                Reply::Array(vec![Reply::Bulk(Some(b"key".to_vec()))]),
            ])
        );
        assert!(read_reply(&mut r).await.is_err());
        assert_eq!(encode(&[b"GET", b"k"]), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }
}
