use nutype::nutype;

#[nutype(derive(
    Debug,
    Display,
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

#[nutype(
    derive(
        Debug,
        Display,
        Default,
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
    ),
    default = 0
)]
pub struct Secs(u32);

#[nutype(
    derive(
        Debug,
        Display,
        Default,
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
    ),
    default = 0
)]
pub struct Microsecs(u64);

#[nutype(
    derive(
        Debug,
        Display,
        Default,
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
    ),
    default = 0
)]
pub struct Nanosecs(u64);

impl From<Microsecs> for Nanosecs {
    fn from(microsecs: Microsecs) -> Self {
        Nanosecs::new(microsecs.into_inner() * 1_000)
    }
}

#[nutype(
    derive(
        Debug,
        Display,
        Default,
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
    ),
    default = 0
)]
pub struct Mbps(u32);

#[nutype(
    derive(
        Debug,
        Display,
        Default,
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
    ),
    default = 0
)]
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
        Default,
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
    ),
    default = 0
)]
pub struct Dscp(u32);
