use crate::orderbook::{BboUpdate, BBO_SERIALIZED_SIZE};
use crate::options::{AggregatedOptionSnapshot, MSG_TYPE_SPOT_BBO};
use arrayvec::ArrayVec;
use crossbeam_queue::ArrayQueue;
use parking_lot::RwLock;
use std::io::{ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, warn};

const MAX_CLIENTS: usize = 256;
const BROADCAST_QUEUE_CAP: usize = 1 << 15;
const OPTION_QUEUE_CAP: usize = 1 << 13;

#[derive(Error, Debug)]
pub enum BroadcastError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Max clients reached")]
    MaxClientsReached,
}

struct ClientConnection {
    stream: TcpStream,
    addr: SocketAddr,
}

pub struct TcpBroadcastServer {
    clients: Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>>,
    broadcast_queue: Arc<ArrayQueue<BboUpdate>>,
    option_queue: Arc<ArrayQueue<AggregatedOptionSnapshot>>,
    listener_addr: SocketAddr,
    running: Arc<std::sync::atomic::AtomicBool>,
    dropped_count: Arc<AtomicU64>,
    option_dropped_count: Arc<AtomicU64>,
}

impl TcpBroadcastServer {
    pub fn new(bind_addr: SocketAddr) -> Result<Self, BroadcastError> {
        let clients: Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>> =
            Arc::new(RwLock::new(ArrayVec::new()));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let broadcast_queue = Arc::new(ArrayQueue::new(BROADCAST_QUEUE_CAP));
        let option_queue = Arc::new(ArrayQueue::new(OPTION_QUEUE_CAP));
        let dropped_count = Arc::new(AtomicU64::new(0));
        let option_dropped_count = Arc::new(AtomicU64::new(0));

        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let listener_addr = listener.local_addr()?;

        {
            let clients_clone = clients.clone();
            let running_clone = running.clone();
            thread::Builder::new()
                .name("tcp-acceptor".to_string())
                .spawn(move || {
                    Self::accept_loop(listener, clients_clone, running_clone);
                })
                .expect("Failed to spawn TCP acceptor thread");
        }

        {
            let clients_clone = clients.clone();
            let running_clone = running.clone();
            let queue_clone = broadcast_queue.clone();
            let option_queue_clone = option_queue.clone();
            let dropped_clone = dropped_count.clone();
            let option_dropped_clone = option_dropped_count.clone();
            thread::Builder::new()
                .name("tcp-broadcaster".to_string())
                .spawn(move || {
                    Self::broadcast_loop(
                        queue_clone,
                        option_queue_clone,
                        clients_clone,
                        running_clone,
                        dropped_clone,
                        option_dropped_clone,
                    );
                })
                .expect("Failed to spawn TCP broadcaster thread");
        }

        info!("TCP broadcast server listening on {}", listener_addr);

        Ok(Self {
            clients,
            broadcast_queue,
            option_queue,
            listener_addr,
            running,
            dropped_count,
            option_dropped_count,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener_addr
    }

    fn accept_loop(
        listener: TcpListener,
        clients: Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>>,
        running: Arc<std::sync::atomic::AtomicBool>,
    ) {
        while running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    debug!("New TCP client connecting from {}", addr);
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY for {}: {}", addr, e);
                    }
                    if let Err(e) = stream.set_write_timeout(Some(Duration::from_millis(100))) {
                        warn!("Failed to set write timeout for {}: {}", addr, e);
                    }

                    let mut clients_lock = clients.write();
                    if clients_lock.is_full() {
                        warn!("Max clients reached, rejecting connection from {}", addr);
                        drop(clients_lock);
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        continue;
                    }
                    clients_lock.push(ClientConnection { stream, addr });
                    info!(
                        "Client {} connected. Total clients: {}",
                        addr,
                        clients_lock.len()
                    );
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    error!("TCP accept error: {}", e);
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn broadcast_loop(
        queue: Arc<ArrayQueue<BboUpdate>>,
        option_queue: Arc<ArrayQueue<AggregatedOptionSnapshot>>,
        clients: Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>>,
        running: Arc<std::sync::atomic::AtomicBool>,
        dropped_count: Arc<AtomicU64>,
        option_dropped_count: Arc<AtomicU64>,
    ) {
        const BATCH_SIZE: usize = 64;
        const MAX_OPTION_BATCH: usize = 16;
        let mut batch = ArrayVec::<BboUpdate, BATCH_SIZE>::new();
        let mut option_batch = ArrayVec::<AggregatedOptionSnapshot, MAX_OPTION_BATCH>::new();
        let mut serialize_buf = Vec::with_capacity(65536);

        while running.load(Ordering::Relaxed) {
            while batch.len() < BATCH_SIZE {
                match queue.pop() {
                    Some(bbo) => batch.push(bbo),
                    None => break,
                }
            }

            while option_batch.len() < MAX_OPTION_BATCH {
                match option_queue.pop() {
                    Some(snap) => option_batch.push(snap),
                    None => break,
                }
            }

            if batch.is_empty() && option_batch.is_empty() {
                thread::sleep(Duration::from_micros(50));
                continue;
            }

            serialize_buf.clear();

            for bbo in batch.iter() {
                let header: u16 = BBO_SERIALIZED_SIZE as u16;
                serialize_buf.push(MSG_TYPE_SPOT_BBO);
                serialize_buf.extend_from_slice(&header.to_le_bytes());
                let old_len = serialize_buf.len();
                serialize_buf.resize(old_len + BBO_SERIALIZED_SIZE, 0);
                let payload: &mut [u8; BBO_SERIALIZED_SIZE] =
                    (&mut serialize_buf[old_len..old_len + BBO_SERIALIZED_SIZE])
                        .try_into()
                        .unwrap();
                bbo.serialize(payload);
            }

            for snap in option_batch.iter() {
                snap.serialize(&mut serialize_buf);
            }

            if !serialize_buf.is_empty() {
                Self::broadcast_bytes(&clients, &serialize_buf);
            }

            batch.clear();
            option_batch.clear();

            let dropped = dropped_count.swap(0, Ordering::Relaxed);
            let opt_dropped = option_dropped_count.swap(0, Ordering::Relaxed);
            if dropped > 0 || opt_dropped > 0 {
                warn!(
                    "Dropped {} BBO + {} option snapshots due to queue overflow",
                    dropped, opt_dropped
                );
            }
        }
    }

    fn broadcast_bytes(
        clients: &Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>>,
        data: &[u8],
    ) {
        let mut clients = clients.write();
        let mut to_remove = ArrayVec::<usize, 64>::new();

        for (idx, client) in clients.iter_mut().enumerate() {
            match client.stream.write_all(data) {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    warn!("Write would block for client {}, skipping", client.addr);
                }
                Err(ref e)
                    if e.kind() == ErrorKind::BrokenPipe
                        || e.kind() == ErrorKind::ConnectionReset
                        || e.kind() == ErrorKind::ConnectionAborted =>
                {
                    debug!("Client {} disconnected: {}", client.addr, e);
                    to_remove.push(idx);
                }
                Err(e) => {
                    warn!("Write error for client {}: {}", client.addr, e);
                    to_remove.push(idx);
                }
            }
        }

        if !to_remove.is_empty() {
            for &idx in to_remove.iter().rev() {
                let client = clients.swap_remove(idx);
                let _ = client.stream.shutdown(std::net::Shutdown::Both);
                info!("Client {} removed. Remaining: {}", client.addr, clients.len());
            }
        }
    }

    #[inline(always)]
    pub fn enqueue_bbo(&self, bbo: BboUpdate) {
        if let Err(bbo) = self.broadcast_queue.push(bbo) {
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
            drop(bbo);
        }
    }

    #[inline(always)]
    pub fn enqueue_option_snapshot(&self, snapshot: AggregatedOptionSnapshot) {
        if let Err(snapshot) = self.option_queue.push(snapshot) {
            self.option_dropped_count.fetch_add(1, Ordering::Relaxed);
            drop(snapshot);
        }
    }

    #[inline(always)]
    pub fn broadcast_bbo(&self, bbo: &BboUpdate) {
        self.enqueue_bbo(bbo.clone());
    }

    pub fn client_count(&self) -> usize {
        self.clients.read().len()
    }

    pub fn queue_len(&self) -> usize {
        self.broadcast_queue.len() + self.option_queue.len()
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        let mut clients = self.clients.write();
        for client in clients.drain(..) {
            let _ = client.stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

impl Drop for TcpBroadcastServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpStream;
    use std::time::Duration;

    #[test]
    fn test_server_creation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = TcpBroadcastServer::new(addr).unwrap();
        assert!(server.local_addr().port() > 0);
    }

    #[test]
    fn test_client_connection_and_broadcast() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = TcpBroadcastServer::new(addr).unwrap();
        let server_addr = server.local_addr();

        thread::sleep(Duration::from_millis(50));

        let mut stream = TcpStream::connect(server_addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        thread::sleep(Duration::from_millis(50));
        assert_eq!(server.client_count(), 1);

        let bbo = BboUpdate {
            stock: 123,
            timestamp: 456,
            seq_num: 789,
            bid_price: 1000000,
            bid_volume: 500,
            ask_price: 1010000,
            ask_volume: 300,
            top_bids: ArrayVec::new(),
            top_asks: ArrayVec::new(),
        };

        server.enqueue_bbo(bbo);
        thread::sleep(Duration::from_millis(100));

        let mut header_buf = [0u8; 3];
        stream.read_exact(&mut header_buf).unwrap();
        let msg_type = header_buf[0];
        let msg_len = u16::from_le_bytes([header_buf[1], header_buf[2]]) as usize;
        assert_eq!(msg_type, MSG_TYPE_SPOT_BBO);
        assert_eq!(msg_len, BBO_SERIALIZED_SIZE);

        let mut data_buf = vec![0u8; msg_len];
        stream.read_exact(&mut data_buf).unwrap();

        assert_eq!(u64::from_le_bytes(data_buf[0..8].try_into().unwrap()), 123);
        assert_eq!(u64::from_le_bytes(data_buf[8..16].try_into().unwrap()), 456);
    }
}
