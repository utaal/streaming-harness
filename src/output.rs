use super::input::InputTimeResumableIterator;
use num_traits::{Zero, Bounded};

pub trait Metrics<T: Eq+Ord+Copy> {
    #[inline(always)]
    fn record(&mut self, begin_t: T, end_t: T);

    fn combined(self, other: Self) -> Self;
}

#[cfg(feature = "hdrhist-support")]
impl Metrics<u64> for ::hdrhist::HDRHist {
    fn record(&mut self, begin_t: u64, end_t: u64) {
        self.add_value(end_t - begin_t);
    }

    fn combined(self, other: Self) -> Self {
        self.combined(other)
    }
}

#[derive(Debug)]
pub struct MetricCollector<
    T: Eq+Ord+Copy+Zero+Bounded,
    I: InputTimeResumableIterator<T>,
    M: Metrics<T>> {

    input_times: I,
    latency_metrics: M,
    recorded_samples: usize,
    _phantom: ::std::marker::PhantomData<T>,
}

impl<
    T: Eq+Ord+Copy+Zero+Bounded,
    I: InputTimeResumableIterator<T>,
    M: Metrics<T>> MetricCollector<T, I, M> {

    pub fn new(input_times: I, latency_metrics: M) -> Self {
        Self {
            input_times,
            latency_metrics,
            recorded_samples: 0usize,
            _phantom: ::std::marker::PhantomData,
        }
    }

    pub fn into_inner(self) -> M {
        let MetricCollector { latency_metrics, .. } = self;
        latency_metrics
    }

    pub fn recorded_samples(&self) -> usize {
        self.recorded_samples
    }

    #[inline(always)]
    pub fn acknowledge_next(&mut self, at: T) {
        let begin_t = self.input_times.next().expect("No additional input_times");
        self.latency_metrics.record(begin_t, at);
        self.recorded_samples += 1;
    }

    #[inline(always)]
    pub fn acknowledge_till_input_t(&mut self, at: T, till_input_t: T) {
        loop {
            if let Some(&input_t) = self.input_times.peek() {
                if input_t <= till_input_t {
                    self.input_times.next().unwrap();
                    self.latency_metrics.record(input_t, at);
                    self.recorded_samples += 1;
                    continue;
                }
            }
            break;
        }
    }

    #[inline(always)]
    pub fn acknowledge_while(&mut self, at: T, mut ack: impl FnMut(T)->bool) {
        loop {
            if let Some(&input_t) = self.input_times.peek() {
                if ack(input_t) {
                    self.input_times.next().unwrap();
                    self.latency_metrics.record(input_t, at);
                    self.recorded_samples += 1;
                    continue;
                }
            }
            break;
        }
    }
}

pub struct WarmupDurationMetrics<T: Eq+Ord+Copy, M: Metrics<T>> {
    metrics: M,
    warmup_end: T,
    experiment_end: T,
}

impl<T: Eq+Ord+Copy+::std::fmt::Debug, M: Metrics<T>> WarmupDurationMetrics<T, M> {
    pub fn new(metrics: M, warmup_end: T, experiment_end: T) -> Self {
        Self {
            metrics,
            warmup_end,
            experiment_end,
        }
    }

    pub fn into_inner(self) -> M {
        let WarmupDurationMetrics {
            metrics,
            ..
        } = self;
        metrics
    }
}

impl<T: Eq+Ord+Copy+::std::fmt::Debug, M: Metrics<T>> Metrics<T> for WarmupDurationMetrics<T, M> {
    fn record(&mut self, begin_t: T, end_t: T) {
        if begin_t >= self.warmup_end && begin_t < self.experiment_end {
            self.metrics.record(begin_t, end_t);
        }
    }

    fn combined(self, other: Self) -> Self {
        let WarmupDurationMetrics {
            metrics,
            warmup_end,
            experiment_end,
        } = self;
        let WarmupDurationMetrics {
            metrics: other_metrics,
            warmup_end: other_warmup_end,
            experiment_end: other_experiment_end,
        } = other;
        assert_eq!(warmup_end, other_warmup_end);
        assert_eq!(experiment_end, other_experiment_end);
        WarmupDurationMetrics {
            metrics: metrics.combined(other_metrics),
            warmup_end,
            experiment_end,
        }
    }
}

pub fn combine_all<T: Eq+Ord+Copy, M: Metrics<T>, A: IntoIterator<Item=M>>(all: A) -> M {
    let mut it = all.into_iter();
    let first = it.next().expect("expected at least one metric");
    it.fold(first, |a, b| a.combined(b))
}

pub mod default {
    #[cfg(feature = "hdrhist-support")]
    pub fn hdrhist_timeline_collector<I: super::InputTimeResumableIterator<u64>>(
        input_times: I,
        start: u64,
        overall_start: u64,
        overall_end: u64,
        total_duration: u64,
        timeline_inerval: u64) ->
        super::MetricCollector<
            u64,
            I,
            ::timeline::Timeline<u64, u64, super::WarmupDurationMetrics<u64, ::hdrhist::HDRHist>, ::hdrhist::HDRHist>> {

        super::MetricCollector::new(
            input_times,
            ::timeline::Timeline::new(
                start, total_duration, timeline_inerval,
                super::WarmupDurationMetrics::new(::hdrhist::HDRHist::new(), overall_start, overall_end),
                || ::hdrhist::HDRHist::new()))
    }
}
