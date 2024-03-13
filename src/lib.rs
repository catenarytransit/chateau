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

pub fn classify_feed_list_raw(list_of_feeds: &Vec<String>, dmfr_result_feeds: &HashMap<FeedId, dmfr::Feed>) -> ClassifyFeedResults {
    let mut realtime_feeds: HashSet<String> = HashSet::new();
    let mut static_feeds: HashSet<String> = HashSet::new();

    for feed_id in list_of_feeds {

        match dmfr_result_feeds.get(feed_id) {
            Some(feed) => {
                match feed.spec {
                    dmfr::FeedSpec::Gtfs => {
                        static_feeds.insert(feed_id.clone());
                    },
                    dmfr::FeedSpec::GtfsRt => {
                        realtime_feeds.insert(feed_id.clone());
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
                let chateau_id = name_chateau_from_id(operator_id);

                let single_chateau_result = classify_feed_list(&feed_list, &dmfr_result.feed_hashmap);

                chateaus.insert(chateau_id.clone(), Chateau {
                    chateau_name: chateau_id.clone(),
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

    let mut already_assigned_operators: HashSet<String> = HashSet::new();
    let mut already_assigned_feeds: HashSet<String> = HashSet::new();

    let mut counter_dfs_chateaus:u16 = 0;
    let mut counter_dfs_cheataus_single_feed: u16 = 0;

    for (operator_id, _) in &dmfr_result.operator_to_feed_hashmap {
        if (!operators_single_lord.contains(operator_id)) && (!already_assigned_operators.contains(operator_id)) {
           // println!("{:?}", feed_list);
           // println!("{}", &operator_id);

           let mut current_operator_stack: HashSet<String> = HashSet::new();
           let mut current_feed_stack: HashSet<String> = HashSet::new();

           dfs_operator(&operator_id, &dmfr_result, &mut current_operator_stack, &mut current_feed_stack);

         //  println!("Created new chateau:");
           //println!("Operators: {:?}", current_operator_stack);
           //println!("Feeds: {:?}", current_feed_stack);

           already_assigned_operators.extend(current_operator_stack.clone());
           already_assigned_feeds.extend(current_feed_stack.clone());

           counter_dfs_chateaus = counter_dfs_chateaus + 1;

           if current_feed_stack.len() == 1 {
            counter_dfs_cheataus_single_feed = counter_dfs_cheataus_single_feed + 1;
           }

           let chateau_id = determine_chateau_name(&current_operator_stack, &current_feed_stack);

           let feed_list: Vec<String> = current_feed_stack.iter().map(|x| x.clone()).collect::<Vec<String>>();

           let classification_result = classify_feed_list_raw(&feed_list, &dmfr_result.feed_hashmap);

                chateaus.insert(chateau_id, Chateau {
                    chateau_name: operator_id.clone(),
                    realtime_feeds: classification_result.realtime_feeds,
                    static_feeds: classification_result.static_feeds
                });
        }
    }

    println!("dfs chateau count: {}", counter_dfs_chateaus);
    println!("dfs chateau with 1 feed: {}", counter_dfs_cheataus_single_feed);

    chateaus
}

fn dfs_operator(operator_id: &str,dmfr_result: &ReturnDmfrAnalysis, current_operator_stack: &mut HashSet<String>, current_feed_stack: &mut HashSet<String>) {
    if !current_operator_stack.contains(operator_id) {
        current_operator_stack.insert(String::from(operator_id));

    if let Some(feed_list) = dmfr_result.operator_to_feed_hashmap.get(operator_id) {
        for feed in feed_list {
            dfs_feed(&feed.feed_onestop_id, &dmfr_result, current_operator_stack, current_feed_stack);
        }
    }
    }
}

fn dfs_feed(feed_id: &str,dmfr_result: &ReturnDmfrAnalysis, current_operator_stack: &mut HashSet<String>, current_feed_stack: &mut HashSet<String>) {
    if !current_feed_stack.contains(feed_id) {
        current_feed_stack.insert(String::from(feed_id));

        if let Some(operator_list) = dmfr_result.feed_to_operator_pairs_hashmap.get(feed_id) {
            for operator in operator_list {
                dfs_operator(&operator.operator_id, &dmfr_result, current_operator_stack, current_feed_stack);
            }
        }
    }
}

fn determine_chateau_name(current_operator_stack: &HashSet<String>, current_feed_stack: &HashSet<String>) -> String {
    if current_operator_stack.len() == 1 {
        for operator_id in current_operator_stack.iter() {
            return name_chateau_from_id(operator_id);
        }
    }

    if current_feed_stack.len() == 1 {
        for feed_id in current_feed_stack.iter() {
            return name_chateau_from_id(feed_id);
        }
    }

    for operator_id in current_operator_stack.iter() {
        return name_chateau_from_id(operator_id);
    }

    //this will never reach as the current operator stack is by minimum 1 in length
    String::from("unknown-chateau")
}

fn name_chateau_from_id(id: &str) -> String {
    let pos = id.rfind('-');

    match pos {
        Some(pos) => {
            id.chars().skip(pos + 1).collect()
        }
        None => String::from(id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chateau_naming() {
        assert_eq!(name_chateau_from_id("f-anteaterexpress~rt"), String::from("anteaterexpress~rt"));
    }

    #[test]
    fn test() {
        let chateau_result = chateau();

        std::fs::write("./chateau-result.txt", format!("{:#?}", chateau_result)).expect("Unable to write test contents");

        //println!("Chateau generation result: {:#?}", &chateau_result);
    }
}
