extern crate timely;
extern crate streaming_harness;
extern crate streaming_harness_hdrhist as hdrhist;
extern crate rand;

use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use rand::{SeedableRng, StdRng, Rng};
// use abomonation::Abomonation;

use timely::dataflow::*;
use timely::dataflow::operators::{Capability, Input, Map, Probe, Operator, FrontierNotificator};
use timely::dataflow::channels::pact::{Exchange, Pipeline};

use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::generic::{FrontieredInputHandle, source};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use streaming_harness::util::ToNanos;
use streaming_harness::input::InputTimeResumableIterator;
use streaming_harness::timely_support::flow_controlled;

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

    let metrics = timely::execute_from_args(args, move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let (output_metric_collector,) = worker.dataflow(|scope| {
            let data_loaded = Rc::new(RefCell::new(None));
            let mut probe_handle = ProbeHandle::new();

            let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
                    1, 1_000_000_000 / throughput, seconds * 1_000_000_000);
            let output_metric_collector = Rc::new(RefCell::new(streaming_harness::output::MetricCollector::new(
                input_times(), hdrhist::HDRHist::new()).with_warmup(2).with_cooldown(8))); 
            let output_metric_collector_for_sink = output_metric_collector.clone();


            { 
                let mut loading = true;

                let data_loaded = data_loaded.clone();
                let mut probe_handle = probe_handle.clone();

                let seed: &[_] = &[1, 2, 3, index];
                let mut rng: StdRng = SeedableRng::from_seed(seed);

                let mut input_times = streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

                flow_controlled::iterator_source(scope, "WordsSource", move |last_ts| {
                    if loading {
                        loading = false;
                        Some((RootTimestamp::new(1),
                            (0 .. keys / peers)
                                .map(|i| (RootTimestamp::new(0), ((i * peers + index) as u64, 1)))
                                .collect::<Vec<_>>(), RootTimestamp::new(1)))
                    } else {
                        let mut data_loaded = data_loaded.borrow_mut();
                        if data_loaded.is_none() {
                            *data_loaded = Some(::std::time::Instant::now());
                        }
                        let target_ns = (data_loaded.as_ref().unwrap().elapsed().to_nanos() + 1)
                            / 1_000 * 1_000;
                        input_times.iter_until_incl(target_ns).map(|it|
                            (RootTimestamp::new(target_ns),
                             it.map(|ns| (*last_ts, (ns, rng.next_u64()))).collect::<Vec<_>>(),
                             *last_ts))
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
            .probe_with(&mut probe_handle)
            .sink(Pipeline, "example", move |input| {
                while let Some((time, data)) = input.next() { } 

                if let Some(elapsed) = data_loaded.borrow().as_ref().map(|t| t.elapsed()) {
                    output_metric_collector_for_sink.borrow_mut().acknowledge_while(
                        elapsed.to_nanos(),
                        |t| !input.frontier().less_than(&RootTimestamp::new(t)));
                }
            });

            (output_metric_collector,)
        });

        while worker.step() { }

        match Rc::try_unwrap(output_metric_collector) {
            Ok(out) => out.into_inner().into_inner(),
            Err(_) => panic!("dataflow is still running"),
        }
    }).expect("unsuccessful execution").join().into_iter().fold(
        hdrhist::HDRHist::new(), |acc, res| acc.combined(res.unwrap()));

    eprintln!("== summary ==\n{}", metrics.summary_string());
}

