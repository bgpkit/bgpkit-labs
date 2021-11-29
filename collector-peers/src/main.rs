use std::collections::HashMap;
use std::fs::File;
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use rayon::prelude::*;
use log::info;
use bgp_models::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
struct Latest {
    data: Vec<LatestData>,
    error: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestData {
    collector_id: String,
    collector_url: String,
    data_type: String,
    item_url: String,
    project: String,
    timestamp: u64,
}
fn main() {
    env_logger::init();

    info!("getting list of most recent rib dumps from bgpkit broker...");
    let latest_files: Latest = reqwest::blocking::get("http://api.broker.bgpkit.com/v1/meta/latest_times").unwrap().json().unwrap();
    let mut ribs = vec![];
    for data in latest_files.data {
        if data.data_type.as_str() == "rib" {
            ribs.push((data.collector_id.clone(), data.item_url.clone()));
        }
    }
    info!("total of {} rib dumps found.", ribs.len());

    info!("start parsing peer index table from all the rib dump files...");
    let collect_peers: Vec<(String, HashMap<u32, Peer>)> = ribs.par_iter().filter_map(|(collector,url)| {
        info!("start parsing peer index table from {}", &url);
        let parser = bgpkit_parser::BgpkitParser::new(url).unwrap();
        let mrt_record = parser.into_record_iter().next().unwrap();
        match mrt_record.message{
            MrtMessage::TableDumpV2Message(m) => {
                match m {
                    TableDumpV2Message::PeerIndexTable(p) => {
                        Some((collector.clone(), p.peers_map))
                    }
                    _ => {None}
                }
            }
            _ => { None }
        }
    }).collect();
    info!("serializing to local json file...");
    let collect_peers_map: HashMap<String, HashMap<u32, Peer>> = HashMap::from_iter(collect_peers);
    serde_json::to_writer_pretty(&File::create("collector_peers.json").unwrap(), &json!(collect_peers_map)).unwrap();
    info!("all done!");
}
