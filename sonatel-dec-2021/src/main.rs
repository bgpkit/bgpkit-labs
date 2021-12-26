use std::fs::File;
use std::io::{Write, BufWriter};
use bgpkit_broker::{BgpkitBroker, QueryParams};
use bgpkit_parser::{BgpElem, BgpkitParser};
use flate2::Compression;
use tracing::{info, Level};
use rayon::prelude::*;

fn main() {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(Level::INFO)
        .init();

    let broker = BgpkitBroker::new_with_params("https://api.broker.bgpkit.com/v1", QueryParams{
        start_ts: Some(1640394000), // 2021-12-25T01:00:00
        end_ts: Some(1640401200), // 2021-12-25T03:00:00
        // project: Some("route-views".to_string()),
        collector: Some("route-views2".to_string()),
        data_type: Some("update".to_string()),
        page_size: 100,
        ..Default::default()
    });

    let files = broker.into_iter().map(|f| f.url.to_string()).collect::<Vec<String>>();

    let elems = files.par_iter().map(|url|{
        info!("parsing {}", url);
        let parser = BgpkitParser::new(url).unwrap().add_filter("origin_asn", "8346").unwrap();
        parser.into_elem_iter().collect::<Vec<BgpElem>>()
    }).flat_map(|x|x).collect::<Vec<BgpElem>>();

    info!("outputting to gzip file");

    let mut writer = BufWriter::new(
        flate2::write::GzEncoder::new(File::create(format!("sonatel-updates-2.gz")).unwrap(), Compression::default())
    );
    for elem in elems {
        let elem_str = elem.to_string();
        info!("{}", elem_str);
        write!(writer, "{}\n", elem_str).unwrap();
    }
}
