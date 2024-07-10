use clap::{Arg, Command};
use mongodb::{bson::Document, Client};
use std::env;
use tokio;

mod analytics;
mod merge_users;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let matches = Command::new("mongo-utils")
        .version("1.0")
        .about("Performs different operations based on flags")
        .arg(
            Arg::new("merge")
                .short('m')
                .long("merge")
                .help("Runs the merge function")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("total")
                .short('t')
                .long("total")
                .help("Runs the total_count function")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("pipeline")
                .short('p')
                .long("pipeline")
                .help("Runs the pipeline function")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("duplicates")
                .short('d')
                .long("duplicates")
                .help("Runs the duplicates function")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let mongodb_uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let partner = env::var("PARTNER").expect("PARTNER must be set");

    let client = Client::with_uri_str(&mongodb_uri).await.unwrap();
    let database = client.database("test");
    let collection = database.collection::<Document>("users");

    if matches.get_flag("merge") {
        merge_users::merge(&collection).await.unwrap();
    } else if matches.get_flag("total") {
        analytics::total_count(&collection, &partner).await;
    } else if matches.get_flag("pipeline") {
        analytics::pipeline(&collection).await.unwrap();
    } else if matches.get_flag("duplicates") {
        analytics::duplicates(&collection).await.unwrap();
    } else {
        eprintln!("No valid flag provided. Use --help for more information.");
    }
}
