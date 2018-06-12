use super::output::Metrics;
use std::ops::{Add, Sub};
use num_traits::{Zero, Bounded};

#[derive(Debug, Clone)]
pub struct TimelineElement<T: Eq+Ord+Copy+Zero+Bounded> {
    time: T,
    min: T,
    max: T,
    samples: usize,
}

impl<T: Eq+Ord+Copy+Zero+Bounded> TimelineElement<T> {
    pub fn combined(mut self, other: Self) -> Self {
        assert!(self.time == other.time, "self.time != other.time");
        self.min = ::std::cmp::min(self.min, other.min);
        self.max = ::std::cmp::max(self.max, other.max);
        self.samples = self.samples + other.samples;
        self
    }
}

pub struct Timeline<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy,
    M: Metrics<T>> {

    latency_metrics: M,
    timeline_dt: DT,
    cur_element: usize,
    cur_element_t: T,
    timeline: Vec<TimelineElement<T>>,
}

impl<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy,
    M: Metrics<T>> Timeline<T, DT, M> {

    pub fn new(start_t: T, end_t: T, timeline_dt: DT, latency_metrics: M) -> Self {
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
                min: T::max_value(),
                max: T::min_value(),
                samples: 0,
            }).collect(),
        }
    }

    pub fn into_inner(self) -> (M, Box<[TimelineElement<T>]>) {
        let Timeline {
            latency_metrics,
            timeline,
            ..
        } = self;
        (latency_metrics, timeline.into_boxed_slice())
    }
}

impl<
    T: Eq+Ord+Copy+Zero+Bounded+Add<DT, Output=T>+Sub<T, Output=T>,
    DT: Copy,
    M: Metrics<T>> Metrics<T> for Timeline<T, DT, M> {

    #[inline(always)]
    fn record(&mut self, begin_t: T, end_t: T) {
        self.latency_metrics.record(begin_t, end_t);
        while end_t > self.cur_element_t + self.timeline_dt {
            self.cur_element_t = self.cur_element_t + self.timeline_dt;
            self.cur_element += 1;
        }
        let TimelineElement {
            ref mut min,
            ref mut max,
            ref mut samples,
            ..
        } = &mut self.timeline[self.cur_element];
        let latency = end_t - begin_t;
        *min = ::std::cmp::min(*min, latency);
        *max = ::std::cmp::max(*max, latency);
        *samples += 1;
    }
}
