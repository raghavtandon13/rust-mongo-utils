use futures_util::stream::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, DateTime, Document},
    options::{AggregateOptions, FindOptions},
    Collection,
};
use std::error::Error;

pub async fn merge(collection: &Collection<Document>) -> Result<(), Box<dyn Error>> {
    let pipeline = vec![
        doc! { "$match": { "updatedAt": { "$gte": DateTime::parse_rfc3339_str("2020-05-15T00:00:00Z")?, "$lt": DateTime::parse_rfc3339_str("2024-05-16T00:00:00Z")? } } },
        doc! { "$group": { "_id": "$phone", "count": { "$sum": 1 } } },
        doc! { "$match": { "count": { "$gt": 1 } } },
        doc! { "$sort": { "count": -1 } },
        doc! { "$project": { "_id": 0, "phone": "$_id" } },
        doc! { "$limit": 1000 },
    ];
    let mut cursor = collection.aggregate(pipeline, AggregateOptions::default()).await?;
    let mut i = 1;
    while let Some(result) = cursor.try_next().await? {
        if let Some(phone) = result.get_str("phone").ok() {
            println!("{}: {}", i, phone);
            i += 1;
            let filter = doc! {"phone": phone};
            let find_options = FindOptions::default();
            let users: Vec<Document> = collection.find(filter, find_options).await?.try_collect().await?;
            let mut sorted_users = users.clone();
            sorted_users.sort_by(|a, b| {
                b.get_datetime("updatedAt")
                    .unwrap()
                    .cmp(&a.get_datetime("updatedAt").unwrap())
            });
            let mut merged_user = sorted_users[0].clone();
            let mut merged_accounts: Vec<Bson> = Vec::new();
            for user in &sorted_users {
                if let Ok(accounts) = user.get_array("accounts") {
                    merged_accounts.extend(accounts.iter().cloned());
                }
            }
            merged_user.insert("accounts", Bson::Array(merged_accounts));
            for user in &sorted_users[1..] {
                for (key, value) in user.iter() {
                    if key != "accounts" && key != "_id" && key != "updatedAt" && !merged_user.contains_key(key) {
                        merged_user.insert(key, value.clone());
                    }
                }
            }
            let merged_id = merged_user.get_object_id("_id")?;
            collection
                .replace_one(doc! {"_id": merged_id}, merged_user, None)
                .await?;
            for user in &sorted_users[1..] {
                let user_id = user.get_object_id("_id")?;
                collection.delete_one(doc! {"_id": user_id}, None).await?;
            }
        }
    }
    println!("Done");
    Ok(())
}
