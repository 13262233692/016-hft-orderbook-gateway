use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use thiserror::Error;

pub mod tcp_broadcast;
pub use tcp_broadcast::TcpBroadcastServer;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Socket error: {0}")]
    Socket(String),
}

pub struct MulticastReceiver {
    socket: Socket,
    buffer: Box<[MaybeUninit<u8>]>,
}

impl MulticastReceiver {
    pub fn new(
        multicast_addr: Ipv4Addr,
        port: u16,
        interface: Ipv4Addr,
        buffer_size: usize,
    ) -> Result<Self, NetworkError> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

        socket.set_reuse_address(true)?;
        #[cfg(unix)]
        socket.set_reuse_port(true)?;

        let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        socket.bind(&SocketAddr::V4(bind_addr).into())?;

        socket.join_multicast_v4(&multicast_addr, &interface)?;

        socket.set_recv_buffer_size(buffer_size)?;
        socket.set_nonblocking(false)?;

        let buffer = vec![MaybeUninit::uninit(); 65536].into_boxed_slice();

        Ok(Self { socket, buffer })
    }

    #[inline(always)]
    pub fn receive(&mut self) -> Result<&[u8], NetworkError> {
        match self.socket.recv(&mut self.buffer) {
            Ok(n) => Ok(unsafe {
                std::slice::from_raw_parts(self.buffer.as_ptr() as *const u8, n)
            }),
            Err(e) => Err(NetworkError::Io(e)),
        }
    }

    pub fn try_clone(&self) -> Result<Self, NetworkError> {
        let socket = self.socket.try_clone()?;
        let buffer = vec![MaybeUninit::uninit(); 65536].into_boxed_slice();
        Ok(Self { socket, buffer })
    }

    pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> Result<(), NetworkError> {
        self.socket.set_read_timeout(timeout)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multicast_receiver_creation() {
        let result = MulticastReceiver::new(
            Ipv4Addr::new(233, 1, 1, 1),
            12345,
            Ipv4Addr::new(0, 0, 0, 0),
            8 * 1024 * 1024,
        );
        assert!(result.is_ok());
    }
}
