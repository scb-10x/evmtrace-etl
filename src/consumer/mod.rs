use alloy_primitives::Address;

mod trace;

pub use trace::*;

pub const EC_MUL_ADDRESS: Address = Address::new([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 6 bytes
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 12 bytes
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 18 bytes
    0x00, 0x07, // 24 bytes
]);

pub const EC_PAIRING_ADDRESS: Address = Address::new([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 6 bytes
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 12 bytes
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 18 bytes
    0x00, 0x08, // 24 bytes
]);