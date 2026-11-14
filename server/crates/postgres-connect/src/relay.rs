//! A TCP relay to a live PostgreSQL that a test can silence, for tests of
//! [`Endpoint::bound_replies`](crate::Endpoint::bound_replies) and of the
//! replication stream's silence bound. Built only with the `test-relay`
//! feature.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
};

/// Once silenced, the relay forwards nothing more in either direction and
/// keeps both sockets open, so the client's kernel sees its bytes taken and no
/// reply, as behind a proxy or a server that stopped answering. With only the
/// replies silenced, it still forwards what the client sends, and keeps a copy.
pub struct SilenceableRelay {
    url: String,
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    silent: AtomicBool,
    replies_silent: AtomicBool,
    sent_unanswered: Mutex<Vec<u8>>,
}

impl SilenceableRelay {
    /// Relays to the host and port of `url`, a
    /// `postgresql://user:password@host:port/database` URL, and serves the
    /// same URL with the relay's loopback address in their place.
    pub fn start(url: &str) -> Self {
        let config: postgres::Config = url.parse().expect("a PostgreSQL URL");
        let host = match &config.get_hosts()[0] {
            postgres::config::Host::Tcp(host) => host.clone(),
            #[cfg(unix)]
            postgres::config::Host::Unix(_) => panic!("the relay needs a TCP database URL"),
        };
        let upstream = format!(
            "{host}:{}",
            config.get_ports().first().copied().unwrap_or(5432)
        );
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let port = listener.local_addr().expect("a bound address").port();
        let shared = Arc::new(Shared::default());
        let accepting = Arc::clone(&shared);
        thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                let Ok(server) = TcpStream::connect(&upstream) else {
                    continue;
                };
                let (Ok(client_reader), Ok(server_reader)) =
                    (client.try_clone(), server.try_clone())
                else {
                    continue;
                };
                let requests = Arc::clone(&accepting);
                thread::spawn(move || forward(client_reader, server, &requests, false));
                let replies = Arc::clone(&accepting);
                thread::spawn(move || forward(server_reader, client, &replies, true));
            }
        });
        let (credentials, rest) = url.split_once('@').expect("a URL with credentials");
        let (_, path) = rest.split_once('/').expect("a URL with a database");
        Self {
            url: format!("{credentials}@127.0.0.1:{port}/{path}"),
            shared,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn silence(&self) {
        self.shared.silent.store(true, Ordering::SeqCst);
    }

    /// Stops forwarding what the server sends, and starts keeping a copy of
    /// what the client sends.
    pub fn silence_replies(&self) {
        self.shared.replies_silent.store(true, Ordering::SeqCst);
    }

    /// What clients sent since `silence_replies`, in arrival order.
    pub fn sent_unanswered(&self) -> Vec<u8> {
        self.shared
            .sent_unanswered
            .lock()
            .expect("relay copy mutex poisoned")
            .clone()
    }
}

fn forward(mut from: TcpStream, mut to: TcpStream, shared: &Shared, replies: bool) {
    let mut buffer = [0; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let replies_silent = shared.replies_silent.load(Ordering::SeqCst);
        if shared.silent.load(Ordering::SeqCst) || (replies && replies_silent) {
            // Hold both sockets open and forward nothing, for the rest of the
            // test binary's life.
            loop {
                thread::park();
            }
        }
        if replies_silent {
            shared
                .sent_unanswered
                .lock()
                .expect("relay copy mutex poisoned")
                .extend_from_slice(&buffer[..read]);
        }
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
    }
    let _ = to.shutdown(Shutdown::Write);
}
