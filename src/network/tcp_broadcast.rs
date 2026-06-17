use crate::orderbook::{BboUpdate, BBO_SERIALIZED_SIZE};
use arrayvec::ArrayVec;
use parking_lot::RwLock;
use std::io::{ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, warn};

const MAX_CLIENTS: usize = 256;

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
    listener_addr: SocketAddr,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl TcpBroadcastServer {
    pub fn new(bind_addr: SocketAddr) -> Result<Self, BroadcastError> {
        let clients: Arc<RwLock<ArrayVec<ClientConnection, MAX_CLIENTS>>> =
            Arc::new(RwLock::new(ArrayVec::new()));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let listener_addr = listener.local_addr()?;

        let clients_clone = clients.clone();
        let running_clone = running.clone();

        thread::Builder::new()
            .name("tcp-acceptor".to_string())
            .spawn(move || {
                Self::accept_loop(listener, clients_clone, running_clone);
            })
            .expect("Failed to spawn TCP acceptor thread");

        info!("TCP broadcast server listening on {}", listener_addr);

        Ok(Self {
            clients,
            listener_addr,
            running,
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
        while running.load(std::sync::atomic::Ordering::Relaxed) {
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

    #[inline(always)]
    pub fn broadcast_bbo(&self, bbo: &BboUpdate) {
        let mut buf = [0u8; BBO_SERIALIZED_SIZE + 2];
        let header: u16 = (BBO_SERIALIZED_SIZE as u16).to_le();
        buf[0..2].copy_from_slice(&header.to_le_bytes());
        bbo.serialize(
            (&mut buf[2..2 + BBO_SERIALIZED_SIZE])
                .try_into()
                .unwrap(),
        );
        self.broadcast_raw(&buf);
    }

    #[inline(always)]
    pub fn broadcast_raw(&self, data: &[u8]) {
        let mut clients = self.clients.write();
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

    pub fn client_count(&self) -> usize {
        self.clients.read().len()
    }

    pub fn shutdown(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

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

        server.broadcast_bbo(&bbo);

        let mut header_buf = [0u8; 2];
        stream.read_exact(&mut header_buf).unwrap();
        let msg_len = u16::from_le_bytes(header_buf) as usize;
        assert_eq!(msg_len, BBO_SERIALIZED_SIZE);

        let mut data_buf = vec![0u8; msg_len];
        stream.read_exact(&mut data_buf).unwrap();

        assert_eq!(u64::from_le_bytes(data_buf[0..8].try_into().unwrap()), 123);
        assert_eq!(u64::from_le_bytes(data_buf[8..16].try_into().unwrap()), 456);
    }
}
