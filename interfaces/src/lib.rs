pub mod types {
    use arrayref::array_ref;

    tonic::include_proto!("types");

    macro_rules! U {
        ($proto:ty, $h:ty, $u:ty) => {
            impl From<$u> for $proto {
                fn from(value: $u) -> Self {
                    Self::from(<$h>::from(<[u8; <$h>::len_bytes()]>::from(value)))
                }
            }

            impl From<$proto> for $u {
                fn from(value: $proto) -> Self {
                    Self::from(<$h>::from(value).0)
                }
            }
        };
    }

    // to PB
    impl From<ethereum_types::H128> for H128 {
        fn from(value: ethereum_types::H128) -> Self {
            Self {
                hi: u64::from_be_bytes(*array_ref!(value, 0, 8)),
                lo: u64::from_be_bytes(*array_ref!(value, 8, 8)),
            }
        }
    }

    impl From<ethereum_types::H160> for H160 {
        fn from(value: ethereum_types::H160) -> Self {
            Self {
                hi: Some(ethereum_types::H128::from_slice(&value[..16]).into()),
                lo: u32::from_be_bytes(*array_ref!(value, 16, 4)),
            }
        }
    }

    impl From<ethereum_types::H256> for H256 {
        fn from(value: ethereum_types::H256) -> Self {
            Self {
                hi: Some(ethereum_types::H128::from_slice(&value[..16]).into()),
                lo: Some(ethereum_types::H128::from_slice(&value[16..]).into()),
            }
        }
    }

    impl From<ethereum_types::H512> for H512 {
        fn from(value: ethereum_types::H512) -> Self {
            Self {
                hi: Some(ethereum_types::H256::from_slice(&value[..32]).into()),
                lo: Some(ethereum_types::H256::from_slice(&value[32..]).into()),
            }
        }
    }

    // from PB
    impl From<H128> for ethereum_types::H128 {
        fn from(value: H128) -> Self {
            let mut v = [0; Self::len_bytes()];
            v[..8].copy_from_slice(&value.hi.to_be_bytes());
            v[8..].copy_from_slice(&value.lo.to_be_bytes());

            v.into()
        }
    }

    impl From<H160> for ethereum_types::H160 {
        fn from(value: H160) -> Self {
            type H = ethereum_types::H128;

            let mut v = [0; Self::len_bytes()];
            v[..H::len_bytes()]
                .copy_from_slice(H::from(value.hi.unwrap_or_default()).as_fixed_bytes());
            v[H::len_bytes()..].copy_from_slice(&value.lo.to_be_bytes());

            v.into()
        }
    }

    impl From<H256> for ethereum_types::H256 {
        fn from(value: H256) -> Self {
            type H = ethereum_types::H128;

            let mut v = [0; Self::len_bytes()];
            v[..H::len_bytes()]
                .copy_from_slice(H::from(value.hi.unwrap_or_default()).as_fixed_bytes());
            v[H::len_bytes()..]
                .copy_from_slice(H::from(value.lo.unwrap_or_default()).as_fixed_bytes());

            v.into()
        }
    }

    impl From<H512> for ethereum_types::H512 {
        fn from(value: H512) -> Self {
            type H = ethereum_types::H256;

            let mut v = [0; Self::len_bytes()];
            v[..H::len_bytes()]
                .copy_from_slice(H::from(value.hi.unwrap_or_default()).as_fixed_bytes());
            v[H::len_bytes()..]
                .copy_from_slice(H::from(value.lo.unwrap_or_default()).as_fixed_bytes());

            v.into()
        }
    }

    U!(H128, ethereum_types::H128, ethereum_types::U128);
    U!(H256, ethereum_types::H256, ethereum_types::U256);
    U!(H512, ethereum_types::H512, ethereum_types::U512);

    impl From<ethnum::U256> for H256 {
        fn from(v: ethnum::U256) -> Self {
            ethereum_types::H256(v.to_be_bytes()).into()
        }
    }

    impl From<H256> for ethnum::U256 {
        fn from(v: H256) -> Self {
            ethnum::U256::from_be_bytes(ethereum_types::H256::from(v).0)
        }
    }
}

#[cfg(feature = "consensus")]
pub mod consensus {
    tonic::include_proto!("consensus");
}

#[cfg(feature = "sentry")]
pub mod sentry {
    tonic::include_proto!("sentry");
}

#[cfg(feature = "remotekv")]
pub mod remotekv {
    tonic::include_proto!("remote");
}

#[cfg(feature = "snapshotsync")]
pub mod snapshotsync {
    tonic::include_proto!("downloader");
}

#[cfg(feature = "txpool")]
pub mod txpool {
    tonic::include_proto!("txpool");
    tonic::include_proto!("txpool_control");
}

#[cfg(feature = "web3")]
pub mod web3 {
    tonic::include_proto!("web3");
}
