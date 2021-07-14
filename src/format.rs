
#[cfg(feature = "hdrhist-support")]
pub fn format_detailed_timeline(
    prefix: String,
    timeline: Vec<::timeline::TimelineElement<u64, ::hdrhist::HDRHist>>) -> String {
    timeline.into_iter().map(|::timeline::TimelineElement { time, metrics, .. }|
        metrics
            .ccdf_upper_bound()
            .map(|(value, prob)| format!("{}\t{}\t{}\t{}", prefix, time, value, prob))
            .collect::<Vec<_>>()
            .join("\n")).collect::<Vec<_>>().join("\n")
}

#[cfg(feature = "hdrhist-support")]
pub fn format_summary_timeline(
    prefix: String,
    timeline: Vec<::timeline::TimelineElement<u64, ::hdrhist::HDRHist>>) -> String {
    timeline.into_iter().map(|::timeline::TimelineElement { time, metrics, .. }|
        format!("{}\t{}\t{}", prefix, time, metrics
            .summary()
            .map(|(_, _, upper_bound)| format!("{}", upper_bound))
            .collect::<Vec<_>>()
            .join("\t"))).collect::<Vec<_>>().join("\n")
}
