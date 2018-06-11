pub trait ToNanos {
    fn to_nanos(&self) -> u64;
}

impl ToNanos for ::std::time::Duration {
    fn to_nanos(&self) -> u64 {
        self.as_secs() as u64 * 1_000_000_000 + self.subsec_nanos() as u64
    }
}
