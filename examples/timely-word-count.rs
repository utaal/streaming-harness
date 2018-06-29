extern crate timely;
extern crate streaming_harness;
extern crate streaming_harness_hdrhist as hdrhist;
extern crate rand;

use std::collections::HashMap;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use rand::RngCore;

use timely::dataflow::*;
use timely::dataflow::operators::{Capability, Probe, Operator, FrontierNotificator};
use timely::dataflow::operators::flow_controlled;
use timely::dataflow::channels::pact::Exchange;

use timely::progress::timestamp::RootTimestamp;

use streaming_harness::util::ToNanos;
use streaming_harness::output;
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
            let output_metric_collector = Rc::new(RefCell::new(
                streaming_harness::output::default::hdrhist_timeline_collector(
                    input_times(),
                    0, 2_000_000_000, 8_000_000_000, 10_000_000_000, 1_000_000_000)));
            let output_metric_collector_for_acknowledge = output_metric_collector.clone();

            let data_loaded = ::std::rc::Rc::new(Cell::new(None));
            { 
                let mut loading = true;
                let data_loaded = data_loaded.clone();

                let probe_handle = probe_handle.clone();

                let seed: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, index as u8];
                let mut rng: ::rand::rngs::SmallRng = ::rand::SeedableRng::from_seed(seed);

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
                                |_cap, _| {
                let mut notificator = FrontierNotificator::new();
                let mut counts = HashMap::new();
                let mut stash: HashMap<Capability<_>, Vec<Vec<(u64, u64)>>> = HashMap::new();

                move |input, output| {
                    input.for_each(|time, data| {
                        let time = time.retain();
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

        Rc::try_unwrap(output_metric_collector).map_err(|_| ()).expect("dataflow still running").into_inner().into_inner()
    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();
 
    let Timeline { timeline, latency_metrics, .. } = output::combine_all(timelines);

    eprintln!("== summary ==\n{}", latency_metrics.into_inner().summary_string());
    eprintln!("== timeline ==\n{}",
              timeline.clone().into_iter().map(|::streaming_harness::timeline::TimelineElement { time, metrics, samples }|
                    format!("-- {} ({} samples) --\n{}", time, samples, metrics.summary_string())).collect::<Vec<_>>().join("\n"));
    println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
}
