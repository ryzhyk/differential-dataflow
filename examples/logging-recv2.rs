extern crate timely;
extern crate differential_dataflow;

use std::sync::{Arc, Mutex};
use std::net::TcpListener;
use std::time::Duration;

use timely::dataflow::operators::{Map, Inspect};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::TimelyEvent;
use timely::communication::logging::{CommunicationEvent, CommunicationSetup};
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::capture::{EventReader, Replay};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Consolidate, Join};
// use differential_dataflow::logging::DifferentialEvent;

fn main() {

    let mut args = ::std::env::args();
    args.next().unwrap();

    let work_peers = args.next().expect("Must provide number of source peers").parse::<usize>().expect("Source peers must be an unsigned integer");
    let comm_peers = args.next().expect("Must provide number of source peers").parse::<usize>().expect("Source peers must be an unsigned integer");

    let t_listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    let d_listener = TcpListener::bind("127.0.0.1:9000").unwrap();
    let t_sockets =
    Arc::new(Mutex::new((0..work_peers).map(|_| {
            let socket = t_listener.incoming().next().unwrap().unwrap();
            socket.set_nonblocking(true).expect("failed to set nonblocking");
            Some(socket)
        }).collect::<Vec<_>>()));
    let d_sockets =
    Arc::new(Mutex::new((0..comm_peers).map(|_| {
            let socket = d_listener.incoming().next().unwrap().unwrap();
            socket.set_nonblocking(true).expect("failed to set nonblocking");
            Some(socket)
        }).collect::<Vec<_>>()));

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let t_streams =
        t_sockets
            .lock()
            .unwrap()
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| *i % peers == index)
            .map(move |(_, s)| s.take().unwrap())
            .map(|r| EventReader::<Product<RootTimestamp, Duration>, (Duration, usize, TimelyEvent),_>::new(r))
            .collect::<Vec<_>>();

        let d_streams =
        d_sockets
            .lock()
            .unwrap()
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| *i % peers == index)
            .map(move |(_, s)| s.take().unwrap())
            .map(|r| EventReader::<Product<RootTimestamp, Duration>, (Duration, CommunicationSetup, CommunicationEvent),_>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow::<_,_,_>(|scope| {

            let t_events = t_streams.replay_into(scope);
            let d_events = d_streams.replay_into(scope);

            // let operates =
            t_events
                .filter(|x| x.1 == 0)
                .flat_map(move |(ts, _worker, datum)| {
                    let ts = Duration::from_secs(ts.as_secs() + 1);
                    if let TimelyEvent::Channels(event) = datum {
                        Some((event, RootTimestamp::new(ts), 1))
                    }
                    else { None }
                })
                .as_collection()
                // .consolidate()
                .inspect(|x| println!("CHANNEL\t{:?}", x));
                ;

            t_events
                .filter(|x| x.1 == 0)
                .flat_map(move |(ts, _worker, datum)| {
                    let ts = Duration::from_secs(ts.as_secs() + 1);
                    if let TimelyEvent::CommChannels(event) = datum {
                        Some((event, RootTimestamp::new(ts), 1))
                    }
                    else { None }
                })
                .as_collection()
                // .consolidate()
                .inspect(|x| println!("COMM_CHANNEL\t{:?}", x));
                ;

            // let memory =
            d_events
                // .inspect(|x| println!("MESSAGE\t{:?}", x))
                .flat_map(|(ts, _worker, datum)| {
                    let ts = Duration::from_secs(ts.as_secs() + 1);
                    if let CommunicationEvent::Message(x) = datum {
                        Some((x.header.channel, RootTimestamp::new(ts), x.header.length as isize))
                    }
                    else { None }
                })
                .as_collection()
                .consolidate()
                .inspect(|x| println!("MESSAGE\t{:?}", x));
                ;

            // operates
            //     .inspect(|x| println!("OPERATES: {:?}", x))
            //     .semijoin(&memory)
            //     .inspect(|x| println!("{:?}", x));

        });

    }).unwrap(); // asserts error-free execution
}