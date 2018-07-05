
#[cfg(feature = "hdrhist-support")]
pub fn format_detailed_timeline(
    prefix: String,
    timeline: Vec<::timeline::TimelineElement<u64, ::hdrhist::HDRHist>>) -> String {
    timeline.into_iter().map(|::timeline::TimelineElement { time, metrics, .. }|
        metrics
            .ccdf()
            .map(|(value, prob, count)| format!("{}\t{}\t{}\t{}\t{}", prefix, time, value, prob, count))
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
            .map(|(_, count)| format!("{}", count))
            .collect::<Vec<_>>()
            .join("\t"))).collect::<Vec<_>>().join("\n")
}
