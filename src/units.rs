use nutype::nutype;

#[nutype(derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct Bytes(u64);

#[nutype(derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct Secs(u32);

#[nutype(derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct Nanosecs(u64);

#[nutype(derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct Mbps(u32);

#[nutype(derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct BitsPerSec(u64);

impl From<Mbps> for BitsPerSec {
    fn from(mbps: Mbps) -> Self {
        BitsPerSec::new(u64::from(mbps.into_inner()) * 1_000_000)
    }
}

#[nutype(
    validate(less = 64),
    derive(
        Debug,
        Display,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Hash,
        FromStr,
        PartialOrd,
        Ord,
        PartialEq,
        Eq,
        Serialize,
        Deserialize
    )
)]
pub struct Dscp(u32);
