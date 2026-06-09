#![forbid(unsafe_code)]

pub fn baseline_benchmarks_expected() -> &'static [&'static str] {
    &["fio-seq-write-qd32", "fio-seq-read-qd32", "fio-randread-4k"]
}

#[cfg(test)]
mod tests {
    #[test]
    fn m0_baselines_are_named() {
        assert_eq!(super::baseline_benchmarks_expected().len(), 3);
    }
}
