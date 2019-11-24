extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;
extern crate dogsdogsdogs;

use timely::dataflow::*;

use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::input::Input;
use differential_dataflow::collection::concatenate;
use dogsdogsdogs::altneu::AltNeu;
use dogsdogsdogs::operators::propose;

type Node = u32;
type Time = u32;

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();

        println!("Worker {}", index);

        let mut probe = ProbeHandle::new();

        let (mut nodes, mut edges) = worker.dataflow::<Time,_,_>(|scope| {
            let (nodes_input, nodes) = scope.new_collection();
            let (edges_input, edges) = scope.new_collection();

            let nodes_arr = nodes.map(|x: Node|(x,x)).arrange_by_key();
            let edges_arr1 = edges.map(|(x,y)|(x,(x,y))).arrange_by_key();
            let edges_arr2 = edges.map(|(x,y)|(y,(x,y))).arrange_by_key();

            /* valid_edges(x,y) :- edges(x,y), nodes(x), nodes(y). */
            let valid_edges = edges_arr1
                              .join_core(&nodes_arr, |_, (x,y), _|Some((*y, (*x,*y))))
                              .arrange_by_key()
                              .join_core(&nodes_arr, |_, (x,y), _|Some((*x,*y)));

            /* Compute the same rule, but now with delta queries. */
            let valid_edges_mw = scope.scoped::<AltNeu<Time>, _, _>("delta query", |inner| {
                let nodes = nodes.enter(inner);
                let edges = edges.enter(inner);

                let nodes_arr_alt = nodes_arr.enter_at(inner, |_,_,t|AltNeu::alt(t.clone()));
                let nodes_arr_neu = nodes_arr.enter_at(inner, |_,_,t|AltNeu::neu(t.clone()));

                let edges_arr1_alt = edges_arr1.enter_at(inner, |_,_,t|AltNeu::alt(t.clone()));
                let edges_arr2_alt = edges_arr2.enter_at(inner, |_,_,t|AltNeu::alt(t.clone()));

                /* d(valid_edges)/d(edges) :- d(edges(x,y)), nodes'(x), nodes'(y). */
                let delta1 = propose(&propose(&edges,
                                              nodes_arr_neu.clone(),
                                              |(x,_)| x.clone())
                                     .flat_map(|((x,y), _)| Some((x,y))),
                                     nodes_arr_neu.clone(),
                                     |(_,y)| y.clone())
                             .flat_map(|((x,y), _)| Some((x,y)));

                /* d(valid_edges)/d(nodes(x)) :- d(nodes(x)), edges(x,y), nodes'(y). */
                let delta2 = propose(&propose(&nodes,
                                              edges_arr1_alt.clone(),
                                              |x| x.clone())
                                     .flat_map(|(_, (x,y))| Some((x,y))),
                                     nodes_arr_neu.clone(),
                                     |(_,y)| y.clone())
                             .flat_map(|((x,y), _)| Some((x,y)));

                /* d(valid_edges)/d(nodes(y)) :- d(nodes(y)), edges(x,y), nodes(x). */
                let delta3 = propose(&propose(&nodes,
                                              edges_arr2_alt.clone(),
                                              |y| y.clone())
                                     .flat_map(|(_, (x,y))| Some((x,y))),
                                     nodes_arr_alt.clone(),
                                     |(x,_)| x.clone())
                             .flat_map(|((x,y), _)| Some((x,y)));

                concatenate(inner, vec![delta1, delta2, delta3].into_iter()).leave()
            });

            valid_edges.consolidate()
                       .inspect(|x| println!("\tvalid_edges{:?}", x))
                       .probe_with(&mut probe);
            valid_edges_mw.consolidate()
                          .inspect(|x| println!("\tvalid_edges_mw: {:?}", x))
                          .probe_with(&mut probe);
            (nodes_input, edges_input)
        });

        if index != 0 {
            nodes.close();
            edges.close();
            worker.step_while(|| probe.less_than(&3));
        } else {
            /* Transaction 1: insert node 0 */
            nodes.insert(0);
            nodes.advance_to(1);
            edges.advance_to(1);
            nodes.flush();
            edges.flush();
            worker.step_while(|| probe.less_than(nodes.time()));

            /* Transaction 2: Simply advance the time stamp.  This should be a no-op, but without
             * it the bug does not show up. */
            nodes.advance_to(2);
            edges.advance_to(2);
            nodes.flush();
            edges.flush();
            worker.step_while(|| probe.less_than(nodes.time()));

            /* Transaction 3: insert node 562; insert edge 562->0 */
            nodes.insert(562);
            edges.insert((562,0));
            nodes.advance_to(3);
            edges.advance_to(3);
            nodes.flush();
            edges.flush();
            worker.step_while(|| probe.less_than(nodes.time()));
        }
    }).unwrap();
}
