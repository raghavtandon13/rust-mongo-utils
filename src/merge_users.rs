use crate::analytics::colorize_json;
use futures_util::stream::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, DateTime, Document},
    options::{AggregateOptions, FindOptions},
    Collection,
};
use serde_json::Value;
use std::error::Error;

pub async fn merge(collection: &Collection<Document>) -> Result<(), Box<dyn Error>> {
    // Define the aggregation pipeline to find duplicate phones
    let pipeline = vec![
        doc! {
            "$match": {
                "updatedAt": {
                     "$gte": DateTime::parse_rfc3339_str("2020-05-15T00:00:00Z")?,
                     "$lt": DateTime::parse_rfc3339_str("2024-05-16T00:00:00Z")?
                }
            }
        },
        doc! { "$group": { "_id": "$phone", "count": { "$sum": 1 } } },
        doc! { "$match": { "count": { "$gt": 1 } } },
        doc! { "$sort": { "count": -1 } },
        doc! { "$project": { "_id": 0, "phone": "$_id" } },
        doc! { "$limit": 1 },
    ];

    // Execute the aggregation pipeline
    let mut cursor = collection
        .aggregate(pipeline, AggregateOptions::default())
        .await?;

    let mut i = 1;
    // Iterate over the results of the aggregation
    while let Some(result) = cursor.try_next().await? {
        if let Some(phone) = result.get_str("phone").ok() {
            println!("{}: {}", i, phone);
            i += 1;

            // Find all users with the current phone number
            let filter = doc! {"phone": phone};
            let find_options = FindOptions::default();
            let users: Vec<Document> = collection
                .find(filter, find_options)
                .await?
                .try_collect()
                .await?;

            // Find the master user with the latest updatedAt
            let master_user_index = users
                .iter()
                .enumerate()
                .max_by_key(|(_, user)| user.get_datetime("updatedAt").unwrap())
                .map(|(index, _)| index)
                .unwrap_or(0);
            let muser = users[master_user_index].clone();
            let json_value: Value = serde_json::to_value(&muser)?;
            let colorized_json = colorize_json(&json_value, 0);
            println!("Master User before merging accounts:");
            println!("{}", colorized_json);
            println!("------------------------");

            // Collect all accounts from all users
            let merged_accounts: Vec<Bson> = users
                .iter()
                .flat_map(|user| {
                    user.get_array("accounts")
                        .unwrap_or(&Vec::new())
                        .iter()
                        .cloned()
                        .collect::<Vec<Bson>>()
                })
                .collect();

            // Create a new master user document with merged accounts
            let mut master_user = users[master_user_index].clone();
            master_user.insert("accounts", Bson::Array(merged_accounts));

            // Print the master user for debugging (since we're not in production mode)

            let master_json_value: Value = serde_json::to_value(&master_user)?;
            let colorized_master_json = colorize_json(&master_json_value, 0);
            println!("Master User after merging accounts:");
            println!("{}", colorized_master_json);
            println!("Save skipped");
        }
    }

    println!("Done");
    Ok(())
}
