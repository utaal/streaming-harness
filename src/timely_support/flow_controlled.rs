//! Methods to construct flow-controlled sources.

use timely::Data;
use timely::order::{PartialOrder, TotalOrder};
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::{Stream, Scope};

/// Construct a source that repeatedly calls the provided function to ingest input.
/// - The function can return None to signal the end of the input;
/// - otherwise, it should return Some((t, data, target_t)), where:
///   * `t` is a lower bound on timestamps that can be emitted by this input in the future,
///   `Default::default()` can be used if this isn't needed (the source will assume that
///   the timestamps in `data` are monotonically increasing and will release capabilities
///   accordingly);
///   * `data` is any `T: IntoIterator` of (time, datum) to new input data: time must be
///   monotonically increasing;
///   * `target_t` is a timestamp that represents the frontier that the probe should have
///   reached before the function is invoked again to ingest additional input.
/// The function will receive the current lower bound of timestamps that can be inserted,
/// `lower_bound`.
///
/// # Example
/// ```rust
/// extern crate timely;
/// 
/// use timely::dataflow::operators::flow_controlled::iterator_source;
/// use timely::dataflow::operators::{probe, Probe, Inspect};
/// 
/// use timely::progress::timestamp::RootTimestamp;
/// 
/// fn main() {
///     timely::execute_from_args(std::env::args(), |worker| {
///         let mut input = (0u64..100000).peekable();
///         worker.dataflow(|scope| {
///             let mut probe_handle = probe::Handle::new();
///             let probe_handle_2 = probe_handle.clone();
/// 
///             let mut next_t: u64 = 0;
///             iterator_source(
///                 scope,
///                 "Source",
///                 move |prev_t| {
///                     if input.peek().is_some() {
///                         Some((Default::default(), input.by_ref().take(10).map(|x| {
///                             next_t = x / 100 * 100;
///                             (RootTimestamp::new(next_t), x)
///                         }).collect::<Vec<_>>(), *prev_t))
///                     } else {
///                         None
///                     }
///                 },
///                 probe_handle_2)
///             .inspect_time(|t, d| eprintln!("@ {:?}: {:?}", t, d))
///             .probe_with(&mut probe_handle);
///         });
///     }).unwrap();
/// }
/// ```
pub fn iterator_source<
    G: Scope,
    D: Data,
    I: IntoIterator<Item=(G::Timestamp, D)>,
    F: FnMut(&G::Timestamp)->Option<(G::Timestamp, I, G::Timestamp)>+'static>(
        scope: &G,
        name: &str,
        mut input_f: F,
        probe: Handle<G::Timestamp>,
        ) -> Stream<G, D> where G::Timestamp: TotalOrder {

    let mut target_t = Default::default();
    source(scope, name, |cap| {
        let mut cap = Some(cap);
        move |output| {
            cap = cap.take().and_then(|mut cap| {
                if !probe.less_than(&target_t) {
                    loop {
                        if let Some((done_until_t, data, new_target_t)) = input_f(cap.time()) {
                            target_t = new_target_t;
                            let mut has_data = false;
                            for (t, d) in data.into_iter() {
                                cap = if cap.time() != &t { cap.delayed(&t) } else { cap };
                                output.session(&cap).give(d);
                                has_data = true;
                            }

                            cap = if cap.time().less_than(&done_until_t) { cap.delayed(&done_until_t) } else { cap };
                            if !has_data {
                                break Some(cap);
                            }
                        } else {
                            break None;
                        }
                    }
                } else {
                    Some(cap)
                }
            });
        }
    })
}
