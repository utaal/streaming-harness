use super::input::InputTimeResumableIterator;
use num_traits::{Zero, Bounded};

pub trait Metrics<T: Eq+Ord+Copy> {
    #[inline(always)]
    fn record(&mut self, begin_t: T, end_t: T);
}

#[cfg(feature = "hdrhist-support")]
impl Metrics<u64> for ::hdrhist::HDRHist {
    fn record(&mut self, begin_t: u64, end_t: u64) {
        self.add_value(end_t - begin_t);
    }
}

pub struct MetricCollector<
    T: Eq+Ord+Copy+Zero+Bounded,
    I: InputTimeResumableIterator<T>,
    M: Metrics<T>> {

    input_times: I,
    latency_metrics: M,
    warmup_end: T,
    experiment_end: T,
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
            warmup_end: T::zero(),
            experiment_end: T::max_value(),
            recorded_samples: 0usize,
            _phantom: ::std::marker::PhantomData,
        }
    }

    pub fn with_warmup(mut self, warmup_end: T) -> Self {
        self.warmup_end = warmup_end;
        self
    }

    pub fn with_cooldown(mut self, experiment_end: T) -> Self {
        self.experiment_end = experiment_end;
        self
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
        if at >= self.warmup_end && at < self.experiment_end {
            self.latency_metrics.record(begin_t, at);
            self.recorded_samples += 1;
        }
    }

    #[inline(always)]
    pub fn acknowledge_till_input_t(&mut self, at: T, till_input_t: T) {
        loop {
            if let Some(&input_t) = self.input_times.peek() {
                if input_t <= till_input_t {
                    self.input_times.next().unwrap();
                    if at >= self.warmup_end && at < self.experiment_end {
                        self.latency_metrics.record(input_t, at);
                        self.recorded_samples += 1;
                    }
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
                    continue;
                }
            }
            break;
        }
    }
}
