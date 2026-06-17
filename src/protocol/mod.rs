use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Insufficient data: expected {expected}, got {got}")]
    InsufficientData { expected: usize, got: usize },
    #[error("Parse error: {0}")]
    ParseError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline(always)]
    pub fn from_byte(b: u8) -> Result<Self, ProtocolError> {
        match b {
            b'B' => Ok(Side::Buy),
            b'S' => Ok(Side::Sell),
            _ => Err(ProtocolError::InvalidMessageType(b)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ItchEvent {
    AddOrder {
        timestamp: u64,
        order_ref: u64,
        side: Side,
        shares: u32,
        stock: u64,
        price: u64,
    },
    AddOrderMpid {
        timestamp: u64,
        order_ref: u64,
        side: Side,
        shares: u32,
        stock: u64,
        price: u64,
        mpid: u32,
    },
    OrderExecuted {
        timestamp: u64,
        order_ref: u64,
        shares: u32,
        match_number: u64,
    },
    OrderExecutedPrice {
        timestamp: u64,
        order_ref: u64,
        shares: u32,
        match_number: u64,
        printable: bool,
        price: u64,
    },
    OrderCancel {
        timestamp: u64,
        order_ref: u64,
        shares: u32,
    },
    OrderDelete {
        timestamp: u64,
        order_ref: u64,
    },
    OrderReplace {
        timestamp: u64,
        order_ref: u64,
        new_order_ref: u64,
        shares: u32,
        price: u64,
    },
    StockDirectory {
        timestamp: u64,
        stock: u64,
        market_category: u8,
        financial_status: u8,
        round_lot_size: u32,
        round_lots_only: bool,
        issue_classification: u8,
    },
    SystemEvent {
        timestamp: u64,
        event_code: u8,
    },
}

impl ItchEvent {
    #[inline(always)]
    pub fn timestamp(&self) -> u64 {
        match self {
            ItchEvent::AddOrder { timestamp, .. }
            | ItchEvent::AddOrderMpid { timestamp, .. }
            | ItchEvent::OrderExecuted { timestamp, .. }
            | ItchEvent::OrderExecutedPrice { timestamp, .. }
            | ItchEvent::OrderCancel { timestamp, .. }
            | ItchEvent::OrderDelete { timestamp, .. }
            | ItchEvent::OrderReplace { timestamp, .. }
            | ItchEvent::StockDirectory { timestamp, .. }
            | ItchEvent::SystemEvent { timestamp, .. } => *timestamp,
        }
    }
}

pub struct ItchParser;

impl ItchParser {
    pub fn new() -> Self {
        Self
    }

    #[inline(always)]
    pub fn parse_message(&self, data: &[u8]) -> Result<ItchEvent, ProtocolError> {
        if data.len() < 3 {
            return Err(ProtocolError::InsufficientData {
                expected: 3,
                got: data.len(),
            });
        }

        let msg_type = data[2];
        let mut cursor = Cursor::new(&data[3..]);

        match msg_type {
            b'A' => self.parse_add_order(&mut cursor, false),
            b'F' => self.parse_add_order(&mut cursor, true),
            b'E' => self.parse_order_executed(&mut cursor, false),
            b'C' => self.parse_order_executed(&mut cursor, true),
            b'X' => self.parse_order_cancel(&mut cursor),
            b'D' => self.parse_order_delete(&mut cursor),
            b'U' => self.parse_order_replace(&mut cursor),
            b'R' => self.parse_stock_directory(&mut cursor),
            b'S' => self.parse_system_event(&mut cursor),
            _ => Err(ProtocolError::InvalidMessageType(msg_type)),
        }
    }

    #[inline(always)]
    fn parse_add_order(
        &self,
        cursor: &mut Cursor<&[u8]>,
        with_mpid: bool,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = if with_mpid { 36 } else { 32 };
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let order_ref = cursor.read_u64::<BigEndian>().unwrap();
        let buy_sell = cursor.read_u8().unwrap();
        let side = Side::from_byte(buy_sell)?;
        let shares = cursor.read_u32::<BigEndian>().unwrap();
        let stock = cursor.read_u64::<BigEndian>().unwrap();
        let price = cursor.read_u32::<BigEndian>().unwrap() as u64;

        if with_mpid {
            let mpid = cursor.read_u32::<BigEndian>().unwrap();
            Ok(ItchEvent::AddOrderMpid {
                timestamp,
                order_ref,
                side,
                shares,
                stock,
                price,
                mpid,
            })
        } else {
            Ok(ItchEvent::AddOrder {
                timestamp,
                order_ref,
                side,
                shares,
                stock,
                price,
            })
        }
    }

    #[inline(always)]
    fn parse_order_executed(
        &self,
        cursor: &mut Cursor<&[u8]>,
        with_price: bool,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = if with_price { 31 } else { 26 };
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let order_ref = cursor.read_u64::<BigEndian>().unwrap();
        let shares = cursor.read_u32::<BigEndian>().unwrap();
        let match_number = cursor.read_u64::<BigEndian>().unwrap();

        if with_price {
            let printable = cursor.read_u8().unwrap() == b'Y';
            let price = cursor.read_u32::<BigEndian>().unwrap() as u64;
            Ok(ItchEvent::OrderExecutedPrice {
                timestamp,
                order_ref,
                shares,
                match_number,
                printable,
                price,
            })
        } else {
            Ok(ItchEvent::OrderExecuted {
                timestamp,
                order_ref,
                shares,
                match_number,
            })
        }
    }

    #[inline(always)]
    fn parse_order_cancel(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = 22;
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let order_ref = cursor.read_u64::<BigEndian>().unwrap();
        let shares = cursor.read_u32::<BigEndian>().unwrap();

        Ok(ItchEvent::OrderCancel {
            timestamp,
            order_ref,
            shares,
        })
    }

    #[inline(always)]
    fn parse_order_delete(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = 18;
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let order_ref = cursor.read_u64::<BigEndian>().unwrap();

        Ok(ItchEvent::OrderDelete { timestamp, order_ref })
    }

    #[inline(always)]
    fn parse_order_replace(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = 34;
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let order_ref = cursor.read_u64::<BigEndian>().unwrap();
        let new_order_ref = cursor.read_u64::<BigEndian>().unwrap();
        let shares = cursor.read_u32::<BigEndian>().unwrap();
        let price = cursor.read_u32::<BigEndian>().unwrap() as u64;

        Ok(ItchEvent::OrderReplace {
            timestamp,
            order_ref,
            new_order_ref,
            shares,
            price,
        })
    }

    #[inline(always)]
    fn parse_stock_directory(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = 39;
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let stock = cursor.read_u64::<BigEndian>().unwrap();
        let market_category = cursor.read_u8().unwrap();
        let financial_status = cursor.read_u8().unwrap();
        let round_lot_size = cursor.read_u32::<BigEndian>().unwrap();
        let round_lots_only = cursor.read_u8().unwrap() == b'Y';
        let issue_classification = cursor.read_u8().unwrap();

        Ok(ItchEvent::StockDirectory {
            timestamp,
            stock,
            market_category,
            financial_status,
            round_lot_size,
            round_lots_only,
            issue_classification,
        })
    }

    #[inline(always)]
    fn parse_system_event(
        &self,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<ItchEvent, ProtocolError> {
        let expected = 11;
        let remaining = cursor.get_ref().len() - cursor.position() as usize;
        if remaining < expected {
            return Err(ProtocolError::InsufficientData {
                expected,
                got: remaining,
            });
        }

        let timestamp = cursor.read_u16::<BigEndian>().unwrap() as u64;
        let timestamp2 = cursor.read_u32::<BigEndian>().unwrap() as u64;
        let timestamp = (timestamp << 32) | timestamp2;
        let event_code = cursor.read_u8().unwrap();

        Ok(ItchEvent::SystemEvent { timestamp, event_code })
    }

    pub fn parse_multicast_packet<'a>(
        &'a self,
        packet: &'a [u8],
    ) -> impl Iterator<Item = Result<ItchEvent, ProtocolError>> + 'a {
        let mut offset = 0usize;
        std::iter::from_fn(move || {
            if offset + 2 > packet.len() {
                return None;
            }
            let len = ((packet[offset] as usize) << 8) | (packet[offset + 1] as usize);
            if len < 2 || offset + 2 + len > packet.len() {
                offset = packet.len();
                return None;
            }
            let msg_data = &packet[offset..offset + 2 + len];
            offset += 2 + len;
            Some(self.parse_message(msg_data))
        })
    }
}

impl Default for ItchParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;

    #[test]
    fn test_side_from_byte() {
        assert_eq!(Side::from_byte(b'B').unwrap(), Side::Buy);
        assert_eq!(Side::from_byte(b'S').unwrap(), Side::Sell);
        assert!(Side::from_byte(b'X').is_err());
    }

    #[test]
    fn test_add_order_parse() {
        let parser = ItchParser::new();
        let mut data = vec![0u8; 36];

        let len: u16 = 34;
        data[0] = (len >> 8) as u8;
        data[1] = len as u8;
        data[2] = b'A';

        let mut cursor = Cursor::new(&mut data[3..]);
        cursor.write_u16::<BigEndian>(0x1234).unwrap();
        cursor.write_u32::<BigEndian>(0x56789ABC).unwrap();
        cursor.write_u64::<BigEndian>(12345).unwrap();
        cursor.write_u8(b'B').unwrap();
        cursor.write_u32::<BigEndian>(1000).unwrap();
        cursor.write_u64::<BigEndian>(0x4141504C20202020).unwrap();
        cursor.write_u32::<BigEndian>(1500000).unwrap();

        let event = parser.parse_message(&data).unwrap();
        match event {
            ItchEvent::AddOrder {
                order_ref,
                side,
                shares,
                price,
                ..
            } => {
                assert_eq!(order_ref, 12345);
                assert_eq!(side, Side::Buy);
                assert_eq!(shares, 1000);
                assert_eq!(price, 1500000);
            }
            _ => panic!("Expected AddOrder"),
        }
    }
}
