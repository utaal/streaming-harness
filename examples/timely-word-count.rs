extern crate timely;
extern crate streaming_harness;
extern crate streaming_harness_hdrhist as hdrhist;
extern crate rand;

use std::collections::HashMap;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rand::{SeedableRng, StdRng, Rng};
// use abomonation::Abomonation;

use timely::dataflow::*;
use timely::dataflow::operators::{Capability, Input, Inspect, Map, Probe, Operator, FrontierNotificator};
use timely::dataflow::channels::pact::{Exchange, Pipeline};

use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::generic::{FrontieredInputHandle, source};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use streaming_harness::util::ToNanos;
use streaming_harness::input::InputTimeResumableIterator;
use streaming_harness::timely_support::flow_controlled;
use streaming_harness::output::Metrics;
use streaming_harness::timeline::Timeline;
use streaming_harness::timely_support::Acknowledge;

fn main() {
    let mut args = std::env::args();
    let _cmd = args.next();

    // How many seconds.
    let seconds: u64 = args.next().unwrap().parse().unwrap();
    // How many updates to perform in each round.
    let throughput: u64 = args.next().unwrap().parse().unwrap();
    assert_eq!(1_000_000_000 % throughput, 0, "throughput must be a divisor of 1_000_000_000ns");
    // Number of distinct keys.
    let keys: usize = args.next().unwrap().parse().unwrap();

    let timelines: Vec<_> = timely::execute_from_args(args, move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let (output_metric_collector,) = worker.dataflow(|scope| {
            let mut probe_handle = ProbeHandle::new();

            let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
                    1, 1_000_000_000 / throughput, seconds * 1_000_000_000);
            let output_metric_collector = Rc::new(RefCell::new(streaming_harness::output::MetricCollector::new(
                input_times(),
                streaming_harness::timeline::Timeline::new(
                    0_000_000_000, 10_000_000_000, 100_000_000,
                    hdrhist::HDRHist::new(),
                    || hdrhist::HDRHist::new())
                ).with_warmup(0_000_000_000).with_cooldown(10_000_000_000))); 
            let output_metric_collector_for_acknowledge = output_metric_collector.clone();

            let mut data_loaded = ::std::rc::Rc::new(Cell::new(None));
            { 
                let mut loading = true;
                let data_loaded = data_loaded.clone();

                let mut probe_handle = probe_handle.clone();
                let probe_handle_for_source = probe_handle.clone();

                let seed: &[_] = &[1, 2, 3, index];
                let mut rng: StdRng = SeedableRng::from_seed(seed);

                let mut input_times = streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

                flow_controlled::iterator_source(scope, "WordsSource", move |last_ts| {
                    if loading {
                        loading = false;
                        Some(flow_controlled::IteratorSourceInput {
                            lower_bound: RootTimestamp::new(1),
                            data: vec![
                                (RootTimestamp::new(0), (0 .. keys / peers).map(|i| ((i * peers + index) as u64, 1)).collect::<Vec<_>>())
                            ],
                            target: RootTimestamp::new(1),
                        })
                    } else {
                        if data_loaded.get().is_none() {
                            data_loaded.set(Some(::std::time::Instant::now()));
                        }
                        let elapsed = data_loaded.get().as_ref().map(|t| t.elapsed()).unwrap();
                        let target_ns = (elapsed.to_nanos() + 1) / 1_000_000 * 1_000_000;
                        input_times.iter_until_incl(target_ns).map(|it|
                            flow_controlled::IteratorSourceInput {
                                lower_bound: RootTimestamp::new(target_ns),
                                data: vec![(*last_ts, it.map(|ns| (ns, rng.next_u64())).collect::<Vec<_>>())],
                                target: *last_ts,
                            })
                    }
                }, probe_handle)
            }.unary_frontier::<(u64, u64), _, _, _>(Exchange::new(|&(k, _)| k),
                                "word_count",
                                |_cap| {
                let mut notificator = FrontierNotificator::new();
                let mut counts = HashMap::new();
                let mut stash: HashMap<Capability<_>, Vec<Vec<(u64, u64)>>> = HashMap::new();

                move |input, output| {
                    input.for_each(|time, data| {
                        stash.entry(time.clone()).or_insert_with(Vec::new).push(data.replace_with(Vec::new()));
                        notificator.notify_at(time);
                    });

                    notificator.for_each(&[input.frontier()], |time, _| {
                        let mut affected = HashMap::with_capacity(2048);
                        let mut data = stash.remove(&time).unwrap();
                        for d in data.drain(..) {
                            for (_t, k) in d.into_iter() {
                                let mut new_count = 1;
                                if counts.contains_key(&k) {
                                    let v = counts.get_mut(&k).unwrap();
                                    new_count = *v + 1;
                                    *v = new_count;
                                } else {
                                    counts.insert(k.clone(), 1);
                                }
                                affected.insert(k, new_count);
                            }
                        }
                        output.session(&time).give_iterator(affected.into_iter());
                    });
                }
            })
            .acknowledge(
                output_metric_collector_for_acknowledge,
                data_loaded)
            .probe_with(&mut probe_handle);

            (output_metric_collector,)
        });

        while worker.step() { }

        match Rc::try_unwrap(output_metric_collector) {
            Ok(out) => out.into_inner().into_inner(),
            Err(_) => panic!("dataflow is still running"),
        }
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();
 
    let Timeline {
        timeline,
        latency_metrics,
        ..
    } = {
        let mut timelines_it = timelines.into_iter();
        let timelines_first = timelines_it.next().unwrap();
        timelines_it.fold(timelines_first, |a, b| a.combined(b))
    };

    eprintln!("== summary ==\n{}", latency_metrics.summary_string());
    eprintln!("== timeline ==\n{}",
              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
                    format!("-- {} ({} samples) --\n{}", time, samples, metrics.summary_string())).collect::<Vec<_>>().join("\n"));
    // println!("{}",
    //          timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, .. }|
    //                 metrics
    //                 .ccdf()
    //                 .map(|(value, prob, count)| format!("timeline_ccdf {}\t{}\t{}\t{}", time, value, prob, count))
    //                 .collect::<Vec<_>>()
    //                 .join("\n")).collect::<Vec<_>>().join("\n"));
    // println!("{}",
    //          timeline.into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, .. }|
    //                 format!("timeline_percentiles {}\t{}", time, metrics
    //                 .summary()
    //                 .map(|(_, count)| format!("{}", count))
    //                 .collect::<Vec<_>>()
    //                 .join("\t"))).collect::<Vec<_>>().join("\n"));
}

