use dmfr::*;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use dmfr_folder_reader::*;

#[derive(Debug, Clone)]
pub struct OperatorPairInfo {
    pub operator_id: String,
    pub gtfs_agency_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Chateau {
    chateau_name: String,
    realtime_feeds: HashSet<String>,
    static_feeds: HashSet<String>,
}

pub type FeedId = String;
pub type OperatorId = String;

pub struct ClassifyFeedResults {
    realtime_feeds: HashSet<String>,
    static_feeds: HashSet<String>
}

pub fn classify_feed_list(list_of_feeds: &Vec<dmfr_folder_reader::FeedPairInfo>, dmfr_result_feeds: &HashMap<FeedId, dmfr::Feed>) -> ClassifyFeedResults {
    let mut realtime_feeds: HashSet<String> = HashSet::new();
    let mut static_feeds: HashSet<String> = HashSet::new();

    for feed_pair in list_of_feeds {

        match dmfr_result_feeds.get(&feed_pair.feed_onestop_id) {
            Some(feed) => {
                match feed.spec {
                    dmfr::FeedSpec::Gtfs => {
                        static_feeds.insert(feed_pair.feed_onestop_id.clone());
                    },
                    dmfr::FeedSpec::GtfsRt => {
                        realtime_feeds.insert(feed_pair.feed_onestop_id.clone());
                    },
                    _ => {}
                }
            }, 
            None => {}
        }
    }

    ClassifyFeedResults {
        realtime_feeds, static_feeds
    }
}

pub fn chateau() -> HashMap<String, Chateau> {
    let dmfr_result = dmfr_folder_reader::read_folders("transitland-atlas/");
    //PRE PROCESSING DONE!!!

    //count number of feeds

    println!(
        "size of dataset: {} feeds, {} operators",
        dmfr_result.feed_hashmap.len(),
        dmfr_result.operator_hashmap.len()
    );

    // start by identifying set groups that do not have conflicting operators

    let mut chateaus: HashMap<String, Chateau> = HashMap::new();

    let mut counter_single_lords: usize = 0;
    let mut counter_single_lords_feeds: usize = 0;

    let mut feeds_under_single_lord: HashSet<String> = HashSet::new();
    let mut operators_single_lord: HashSet<String> = HashSet::new();

    for (operator_id, feed_list) in &dmfr_result.operator_to_feed_hashmap {
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
                operators_single_lord.insert(operator_id.clone());
                let chateau_id = operator_id.clone();

                let single_chateau_result = classify_feed_list(&feed_list, &dmfr_result.feed_hashmap);

                chateaus.insert(chateau_id, Chateau {
                    chateau_name: operator_id.clone(),
                    realtime_feeds: single_chateau_result.realtime_feeds,
                    static_feeds: single_chateau_result.static_feeds
                });
            }
        }
    }

    println!(
        "{} operators with single lords, a total of {} feeds ",
        counter_single_lords, counter_single_lords_feeds
    );

    //Perform depth first search
    //visit every tree and mark already visited feeds and operators using a stack and already seen list?

    for (operator_id, _) in &dmfr_result.operator_to_feed_hashmap {
        if (!operators_single_lord.contains(operator_id)) {
           // println!("{:?}", feed_list);
           // println!("{}", &operator_id);

           let mut current_operator_stack: HashSet<String> = HashSet::new();
           let mut current_feed_stack: HashSet<String> = HashSet::new();

           dfs_operator(&operator_id, &dmfr_result, &mut current_operator_stack, &mut current_feed_stack);
        }
    }

    chateaus
}

fn dfs_operator(operator_id: &str,dmfr_result: &ReturnDmfrAnalysis, current_operator_stack: &mut HashSet<String>, current_feed_stack: &mut HashSet<String>) {
    
}

fn dfs_feed(operator_id: &str,dmfr_result: &ReturnDmfrAnalysis, current_operator_stack: &mut HashSet<String>, current_feed_stack: &mut HashSet<String>) {
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let chateau_result = chateau();

        //println!("Chateau generation result: {:#?}", &chateau_result);
    }
}
