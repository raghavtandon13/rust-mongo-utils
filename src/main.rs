use mongodb::{bson::Document, Client};
use std::env;
use tokio;

mod analytics;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let mongodb_uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");

    let client = Client::with_uri_str(&mongodb_uri).await.unwrap();
    let database = client.database("test");
    let collection = database.collection::<Document>("users");

    analytics::total_count(&collection).await;
    analytics::pipeline(&collection).await.unwrap();
    analytics::duplicates(&collection).await.unwrap();
}
