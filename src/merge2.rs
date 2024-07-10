use futures_util::future::join_all;
use futures_util::stream::TryStreamExt;
use mongodb::bson::{doc, Bson, DateTime, Document};
use mongodb::options::{AggregateOptions, FindOptions, ReplaceOptions};
use mongodb::Collection;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub async fn merge(collection: &Collection<Document>, limit: u32) -> Result<(), Box<dyn Error + Send + Sync>> {
    let pipeline = vec![
        doc! { "$match": {
                "updatedAt": {
                "$gte": DateTime::parse_rfc3339_str("2024-05-15T00:00:00Z")?,
                "$lt": DateTime::parse_rfc3339_str("2025-05-16T00:00:00Z")?
        } } },
        doc! { "$group": { "_id": "$phone", "count": { "$sum": 1 } } },
        doc! { "$match": { "count": { "$gt": 1 } } },
        doc! { "$sort": { "count": -1 } },
        doc! { "$project": { "_id": 0, "phone": "$_id" } },
        doc! { "$limit": limit },
    ];

    let options = AggregateOptions::builder().batch_size(100).build();
    let mut cursor = collection.aggregate(pipeline, options).await?;

    let semaphore = Arc::new(Semaphore::new(10));
    let collection = Arc::new(collection.clone());

    let mut tasks = Vec::new();
    let mut i = 1;

    while let Some(result) = cursor.try_next().await? {
        if let Some(phone) = result.get_str("phone").ok() {
            println!("{}: {}", i, phone);
            i += 1;

            let sem_clone = semaphore.clone();
            let coll_clone = collection.clone();
            let phone = phone.to_string();

            tasks.push(tokio::spawn(async move {
                let _permit = sem_clone.acquire().await.unwrap();
                if let Err(e) = process_phone(&coll_clone, &phone).await {
                    eprintln!("Error processing phone {}: {:?}", phone, e);
                }
            }));
        }
    }

    let results = join_all(tasks).await;
    for result in results {
        if let Err(e) = result {
            eprintln!("Task error: {:?}", e);
        }
    }

    println!("Done");
    Ok(())
}

async fn process_phone(collection: &Collection<Document>, phone: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = doc! {"phone": phone};
    let find_options = FindOptions::builder()
        .projection(doc! {
            "phone": 1,
            "accounts": 1,
            "updatedAt": 1,
            "_id": 1,
        })
        .build();

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

    // Update the merged user
    let replace_options = ReplaceOptions::builder().upsert(Some(true)).build();
    collection
        .replace_one(doc! {"_id": merged_id}, merged_user, Some(replace_options))
        .await?;

    // Delete other users
    for user in &sorted_users[1..] {
        let user_id = user.get_object_id("_id")?;
        collection.delete_one(doc! {"_id": user_id}, None).await?;
    }

    Ok(())
}
