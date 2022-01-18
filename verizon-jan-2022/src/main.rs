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
        start_ts: Some(1642517400), // 2022-01-18T14:50:00
        end_ts: Some(1642518900), // 2022-01-18T15:15:00
        // project: Some("route-views".to_string()),
        // collector: Some("route-views2".to_string()),
        data_type: Some("update".to_string()),
        page_size: 100,
        ..Default::default()
    });

    let files = broker.into_iter().map(|f| f.url.to_string()).collect::<Vec<String>>();

    let elems = files.par_iter().map(|url|{
        info!("parsing {}", url);
        let parser = BgpkitParser::new(url).unwrap().add_filter("prefix_sub", "1.1.1.0/24").unwrap();
        parser.into_elem_iter().collect::<Vec<BgpElem>>()
    }).flat_map(|x|x).collect::<Vec<BgpElem>>();

    info!("outputting to gzip file");

    let mut writer = BufWriter::new(
        flate2::write::GzEncoder::new(File::create(format!("verizon-jan-2022-updates.gz")).unwrap(), Compression::default())
    );
    for elem in elems {
        let elem_str = elem.to_string();
        info!("{}", elem_str);
        write!(writer, "{}\n", elem_str).unwrap();
    }
}
