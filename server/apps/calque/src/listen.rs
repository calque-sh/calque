//! The HTTP listeners `dev` and `serve` share: the application's, and the
//! metrics listener when one is configured. Each address is bound before
//! startup creates state or reaches PostgreSQL, and listened on only once the
//! process is ready to serve.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use axum::{serve::Listener, Router};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpSocket, TcpStream},
    task::JoinHandle,
    time::Sleep,
};
use tokio_util::sync::CancellationToken;

/// The backlog `tokio::net::TcpListener::bind` passes.
const BACKLOG: u32 = 128;

/// How long a connection's write may wait for its peer to take any of it.
/// A client that stops reading holds what its unwritten response holds, the
/// room of a change batch among it, until its connection is closed.
pub const WRITE_STALL_BOUND: Duration = Duration::from_secs(30);

/// A listener's address could not be bound or listened on.
#[derive(Debug, Error)]
#[error("cannot listen on {address}: {error}{}", remedy(.error, .flag))]
pub struct ListenError {
    address: SocketAddr,
    /// The command-line flag that moves this listener.
    flag: &'static str,
    error: io::Error,
}

fn remedy(error: &io::Error, flag: &str) -> String {
    if error.kind() == io::ErrorKind::AddrInUse {
        format!("; another process holds that port, so stop it or pass another {flag}")
    } else {
        String::new()
    }
}

/// A socket bound to the HTTP address and not yet listening.
///
/// Binding first refuses a port another process holds before startup does any
/// work, and keeps the port from being taken while startup prepares. A socket
/// that is bound but not listening accepts no connection, so the process is
/// not observable as ready until [`ReservedListener::listen`].
pub struct ReservedListener {
    address: SocketAddr,
    flag: &'static str,
    socket: TcpSocket,
}

impl ReservedListener {
    /// Binds `address`. `flag` is the option that sets it, which a refusal
    /// names.
    pub fn bind(address: SocketAddr, flag: &'static str) -> Result<Self, ListenError> {
        let failed = |error: io::Error| ListenError {
            address,
            flag,
            error,
        };
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(failed)?;
        // `tokio::net::TcpListener::bind` sets this on Unix, so a port a
        // previous run left in TIME_WAIT binds here as it did there.
        #[cfg(not(windows))]
        socket.set_reuseaddr(true).map_err(failed)?;
        socket.bind(address).map_err(failed)?;
        Ok(Self {
            address,
            flag,
            socket,
        })
    }

    pub fn listen(self) -> Result<HttpListener, ListenError> {
        let Self {
            address,
            flag,
            socket,
        } = self;
        let listener = socket.listen(BACKLOG).map_err(|error| ListenError {
            address,
            flag,
            error,
        })?;
        Ok(HttpListener {
            listener,
            stall_bound: WRITE_STALL_BOUND,
        })
    }
}

/// Serves the metrics listener's routes beside the application listener,
/// until `stop` is cancelled.
pub fn spawn_metrics_server(
    listener: HttpListener,
    router: Router,
    stop: CancellationToken,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
    })
}

/// Stops the metrics listener's server, if there is one, and returns how it
/// ended.
pub async fn stop_metrics_server(
    server: Option<JoinHandle<io::Result<()>>>,
    stop: &CancellationToken,
) -> io::Result<()> {
    stop.cancel();
    match server {
        Some(server) => server.await.map_err(io::Error::other)?,
        None => Ok(()),
    }
}

/// The listener `axum::serve` takes from [`ReservedListener::listen`]: every
/// connection it accepts sends each write at once, and fails a write that
/// waits longer than its stall bound.
pub struct HttpListener {
    listener: TcpListener,
    stall_bound: Duration,
}

impl Listener for HttpListener {
    type Io = StallBounded<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = Listener::accept(&mut self.listener).await;
        send_at_once(&stream);
        (StallBounded::new(stream, self.stall_bound), address)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

/// Turns Nagle's algorithm off, as neither tokio nor axum does. With it on, a
/// write shorter than a segment can wait for the peer to acknowledge what was
/// sent before it, one round trip on a real link.
fn send_at_once(stream: &TcpStream) {
    // Fails only for a connection that is already unusable, which its first
    // read or write reports.
    let _ = stream.set_nodelay(true);
}

/// A connection whose write fails once it has waited `bound` without the
/// peer taking any of it. The connection then closes, and what its response
/// held is given back.
pub struct StallBounded<S> {
    io: S,
    bound: Duration,
    stalled: Option<Pin<Box<Sleep>>>,
}

impl<S> StallBounded<S> {
    fn new(io: S, bound: Duration) -> Self {
        Self {
            io,
            bound,
            stalled: None,
        }
    }

    #[cfg(test)]
    fn get_ref(&self) -> &S {
        &self.io
    }

    /// `written`, or, while it is pending, a failure once it has been
    /// pending for the bound.
    fn bounded<T>(
        &mut self,
        cx: &mut Context<'_>,
        written: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if written.is_ready() {
            self.stalled = None;
            return written;
        }
        let bound = self.bound;
        let stalled = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(bound)));
        if stalled.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.stalled = None;
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the peer took none of a write within the stall bound",
        )))
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StallBounded<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StallBounded<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write(cx, buf);
        self.bounded(cx, written)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
        self.bounded(cx, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let flushed = Pin::new(&mut self.io).poll_flush(cx);
        self.bounded(cx, flushed)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, TcpStream},
        time::Duration,
    };

    use axum::serve::Listener;

    use super::*;

    #[tokio::test]
    async fn a_reserved_address_accepts_nothing_until_it_listens() {
        let reserved = ReservedListener::bind((Ipv4Addr::LOCALHOST, 0).into(), "--port").unwrap();
        let address = reserved.socket.local_addr().unwrap();
        // Linux refuses the connection at once. macOS sends no reset, so a
        // connect there waits out the kernel's timeout; a listening socket on
        // loopback accepts well within the bound.
        assert!(
            TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_err(),
            "a bound socket that is not listening must accept no connection"
        );

        let mut listener = reserved.listen().unwrap();
        let connected = TcpStream::connect(address).unwrap();
        let (_, peer) = Listener::accept(&mut listener).await;
        assert_eq!(peer, connected.local_addr().unwrap());
    }

    #[tokio::test]
    async fn every_accepted_connection_sends_each_write_at_once() {
        let reserved = ReservedListener::bind((Ipv4Addr::LOCALHOST, 0).into(), "--port").unwrap();
        let address = reserved.socket.local_addr().unwrap();
        let mut listener = reserved.listen().unwrap();
        let _connected = TcpStream::connect(address).unwrap();
        let (accepted, _) = Listener::accept(&mut listener).await;
        assert!(
            accepted.get_ref().nodelay().unwrap(),
            "Nagle's algorithm is on"
        );
    }

    /// Holds its body's bytes and says when they are given back.
    struct Signalled {
        bytes: Vec<u8>,
        dropped: std::sync::mpsc::Sender<()>,
    }

    impl AsRef<[u8]> for Signalled {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for Signalled {
        fn drop(&mut self) {
            let _ = self.dropped.send(());
        }
    }

    /// A write fails once it alone has waited out the bound: waits that the
    /// peer ends by taking some of what was written do not add up.
    #[tokio::test]
    async fn a_write_fails_once_it_has_waited_out_the_bound_on_its_own() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let bound = Duration::from_millis(300);
        let (writer, mut reader) = tokio::io::duplex(16);
        let mut bounded = StallBounded::new(writer, bound);
        bounded.write_all(&[1; 16]).await.unwrap();
        for _ in 0..3 {
            let taken = async {
                tokio::time::sleep(bound / 2).await;
                let mut taken = [0; 16];
                reader.read_exact(&mut taken).await.unwrap();
            };
            let (written, ()) = tokio::join!(bounded.write_all(&[2; 16]), taken);
            written.unwrap();
        }

        let started = std::time::Instant::now();
        let wait = 10 * bound;
        let plain = tokio::time::timeout(wait, bounded.write(&[3; 16]))
            .await
            .expect("a plain write stalled past the bound")
            .unwrap_err();
        let vectored =
            tokio::time::timeout(wait, bounded.write_vectored(&[io::IoSlice::new(&[4; 16])]))
                .await
                .expect("a vectored write stalled past the bound")
                .unwrap_err();
        assert_eq!(
            (plain.kind(), vectored.kind()),
            (io::ErrorKind::TimedOut, io::ErrorKind::TimedOut)
        );
        assert!(started.elapsed() >= 2 * bound);
    }

    /// A response its client reads arrives whole however long that takes,
    /// and one its client has stopped reading is given back once a write has
    /// waited out the stall bound.
    #[tokio::test]
    async fn a_connection_whose_peer_takes_nothing_for_the_stall_bound_is_closed() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        const BODY: usize = 16 * 1024 * 1024;
        let bound = Duration::from_millis(300);
        let reserved = ReservedListener::bind((Ipv4Addr::LOCALHOST, 0).into(), "--port").unwrap();
        let address = reserved.socket.local_addr().unwrap();
        let mut listener = reserved.listen().unwrap();
        assert_eq!(listener.stall_bound, WRITE_STALL_BOUND);
        listener.stall_bound = bound;
        let (dropped, released) = std::sync::mpsc::channel();
        // Some local tools send a request to every new loopback port. Serving
        // the body at a path of its own keeps their requests from building
        // one, whose release the test would take for the stalled response's.
        let router = Router::new().route(
            "/body",
            axum::routing::get(move || {
                let dropped = dropped.clone();
                async move {
                    axum::body::Bytes::from_owner(Signalled {
                        bytes: vec![7; BODY],
                        dropped,
                    })
                }
            }),
        );
        tokio::spawn(async move { axum::serve(listener, router).await });
        let request = format!("GET /body HTTP/1.1\r\nhost: {address}\r\nconnection: close\r\n\r\n");

        let started = std::time::Instant::now();
        let mut reading = tokio::net::TcpStream::connect(address).await.unwrap();
        reading.write_all(request.as_bytes()).await.unwrap();
        let mut received = 0;
        let mut buffer = vec![0; 256 * 1024];
        loop {
            let read = reading.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            received += read;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(received > BODY, "the reading client got {received} bytes");
        assert!(
            started.elapsed() > bound,
            "the read took {:?}",
            started.elapsed()
        );
        released.recv_timeout(Duration::from_secs(5)).unwrap();

        let started = std::time::Instant::now();
        let mut stopped = tokio::net::TcpStream::connect(address).await.unwrap();
        stopped.write_all(request.as_bytes()).await.unwrap();
        let waited = tokio::task::spawn_blocking(move || {
            released
                .recv_timeout(Duration::from_secs(20))
                .map(|()| started.elapsed())
        })
        .await
        .unwrap()
        .expect("the stalled response is given back");
        assert!(waited >= bound, "given back after {waited:?}");
        drop(stopped);
    }

    #[test]
    fn a_held_port_is_refused_with_its_address_and_the_flag_that_moves_it() {
        let holder = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = holder.local_addr().unwrap();

        let Err(error) = ReservedListener::bind(address, "--metrics-port") else {
            panic!("{address} is held, and binding it must fail");
        };
        assert_eq!(error.error.kind(), io::ErrorKind::AddrInUse);
        let message = error.to_string();
        assert!(
            message.starts_with(&format!("cannot listen on {address}: ")),
            "{message}"
        );
        assert!(
            message.ends_with("pass another --metrics-port"),
            "{message}"
        );
    }
}
