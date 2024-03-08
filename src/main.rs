use std::fs;
use dmfr::*;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OperatorPairInfo {
    pub operator_id: String,
    pub gtfs_agency_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Chateau {
    chateau_name: String,
    realtime_feeds: HashSet<String>,
    static_feeds: HashSet<String>
}

pub type FeedId = String;
pub type OperatorId = String;

fn main() {
    
    let dmfr_result = dmfr_folder_reader::read_folders("transitland-atlas/");
    //PRE PROCESSING DONE!!!

    //count number of feeds

    println!("size of dataset: {} feeds, {} operators", dmfr_result.feed_hashmap.len(), dmfr_result.operator_hashmap.len());

    // start by identifying set groups that do not have conflicting operators

    let mut single_lord: HashMap<String, Chateau> = HashMap::new();

    let mut counter_single_lords:usize = 0;
    let mut counter_single_lords_feeds:usize = 0;

    let mut feeds_under_single_lord: HashSet<String> = HashSet::new();
    
    for (operator_id, feed_list) in dmfr_result.operator_to_feed_hashmap {
        let mut single_lord_status: bool = true;

        for feed in feed_list.iter() {
            let feed_id = feed.feed_onestop_id.clone();

          //  println!("{}: {:?}", operator_id, feed);

             
                let operators_list = dmfr_result.feed_to_operator_pairs_hashmap.get(&feed_id);

                if let Some(operators_list) = operators_list {
                    if operators_list.len() > 1 {
                        single_lord_status = false;
                        
                       // println!("------\n{:#?}", operators_list);
                    } else {
                    }
                } else {
                    eprintln!("Unable to find the operators for the feed {}", &feed_id);
                }
        }

        if (single_lord_status == true) {
            counter_single_lords = counter_single_lords + 1;
            counter_single_lords_feeds = counter_single_lords_feeds + feed_list.len();

            for feed in feed_list.iter() {
                feeds_under_single_lord.insert(feed.feed_onestop_id.clone());

                let chateau_id = operator_id.clone();
            }
        }
    }

    println!("{} operators with single lords, a total of {} feeds ", counter_single_lords, counter_single_lords_feeds );
}
