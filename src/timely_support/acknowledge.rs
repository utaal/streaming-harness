use std::rc::Rc;
use std::cell::{Cell, RefCell};

use input::InputTimeResumableIterator;
use output::{Metrics, MetricCollector};
use util::ToNanos;

use timely::Data;
use timely::dataflow::{Stream, Scope, channels::pact::Pipeline};
use timely::dataflow::operators::generic::operator::Operator;
use timely::progress::{nested::product::Product, timestamp::RootTimestamp};

pub trait Acknowledge<G: Scope<Timestamp=Product<RootTimestamp, u64>>, D: Data> {
    fn acknowledge<
        I: InputTimeResumableIterator<u64>+'static,
        M: Metrics<u64>+'static>(
            &self,
            metric_collector: Rc<RefCell<MetricCollector<u64, I, M>>>,
            data_loaded: Rc<Cell<Option<::std::time::Instant>>>) -> Stream<G, D>;
}

impl<G: Scope<Timestamp=Product<RootTimestamp, u64>>, D: Data> Acknowledge<G, D> for Stream<G, D> {
    fn acknowledge<
        I: InputTimeResumableIterator<u64>+'static,
        M: Metrics<u64>+'static>(
            &self,
            metric_collector: Rc<RefCell<MetricCollector<u64, I, M>>>,
            data_loaded: Rc<Cell<Option<::std::time::Instant>>>) -> Stream<G, D> {

        self.unary_frontier(Pipeline, "Acknowledge", move |_cap, _| {
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    output.session(&time).give_content(data);
                }
                if let Some(elapsed) = data_loaded.get().map(|t| t.elapsed()) {
                    metric_collector.borrow_mut().acknowledge_while(
                        elapsed.to_nanos(),
                        |t| !input.frontier().less_than(&RootTimestamp::new(t)));
                }
            }
        })
    }
}
