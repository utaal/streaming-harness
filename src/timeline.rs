use super::output::Metrics;
use std::ops::{Add, Sub};
use num_traits::{Zero, Bounded};

#[derive(Debug, Clone)]
pub struct TimelineElement<T: Eq+Ord+Copy+Zero+Bounded, M: Metrics<T>> {
    pub time: T,
    pub metrics: M,
    pub samples: usize,
}

impl<T: Eq+Ord+Copy+Zero+Bounded, M: Metrics<T>> TimelineElement<T, M> {
    pub fn combined(mut self, other: Self) -> Self {
        assert!(self.time == other.time, "self.time != other.time");
        self.metrics = self.metrics.combined(other.metrics);
        self.samples = self.samples + other.samples;
        self
    }
}

pub struct Timeline<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy,
    M: Metrics<T>,
    TM: Metrics<T>> {

    pub latency_metrics: M,
    timeline_dt: DT,
    cur_element: usize,
    cur_element_t: T,
    pub timeline: Vec<TimelineElement<T, TM>>,
}

impl<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy,
    M: Metrics<T>,
    TM: Metrics<T>> Timeline<T, DT, M, TM> {

    pub fn new(start_t: T, end_t: T, timeline_dt: DT, latency_metrics: M, timeline_metrics: impl Fn()->TM) -> Self {
        Self {
            latency_metrics,
            timeline_dt,
            cur_element: 0usize,
            cur_element_t: start_t,
            timeline: (0..).scan(start_t, |t, _| {
                let cur = *t;
                *t = *t + timeline_dt;
                Some(cur)
            }).take_while(|t| *t < end_t).map(|time| TimelineElement {
                time,
                metrics: timeline_metrics(),
                samples: 0,
            }).collect(),
        }
    }
}

impl<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy+Eq+::std::fmt::Debug,
    M: Metrics<T>,
    TM: Metrics<T>> Metrics<T> for Timeline<T, DT, M, TM> {

    #[inline(always)]
    fn record(&mut self, begin_t: T, end_t: T) {
        self.latency_metrics.record(begin_t, end_t);
        while begin_t >= self.cur_element_t + self.timeline_dt {
            self.cur_element_t = self.cur_element_t + self.timeline_dt;
            self.cur_element += 1;
        }
        let TimelineElement {
            ref mut metrics,
            ref mut samples,
            ..
        } = &mut self.timeline[self.cur_element];
        metrics.record(begin_t, end_t);
        *samples += 1;
    }

    fn combined(self, other: Self) -> Self {
        let Timeline {
            timeline,
            latency_metrics,
            timeline_dt,
            cur_element,
            cur_element_t,
        } = self;
        let Timeline {
            timeline: other_timeline,
            latency_metrics: other_latency_metrics,
            timeline_dt: other_timeline_dt,
            ..
        } = other;
        assert_eq!(timeline_dt, other_timeline_dt);
        Timeline {
            timeline: 
                timeline.into_iter().zip(other_timeline.into_iter()).map(|(s, m)| s.combined(m)).collect(),
            latency_metrics: latency_metrics.combined(other_latency_metrics),
            timeline_dt,
            cur_element,
            cur_element_t,
        }
    }
}
