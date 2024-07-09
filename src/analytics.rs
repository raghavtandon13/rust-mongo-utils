use colored::*;
use futures_util::stream::TryStreamExt;
use mongodb::{
    bson::{doc, DateTime, Document},
    Collection,
};
use serde_json::Value;
use std::error::Error;

pub async fn duplicates(collection: &Collection<Document>) -> Result<(), Box<dyn Error>> {
    let pipeline = vec![
        doc! { "$group": { "_id": "$phone", "count": { "$sum": 1 } } },
        doc! { "$match": { "count": { "$gt": 1 } } },
        doc! { "$group": { "_id": "null", "duplicatePhones": { "$sum": 1 }, "totalDuplicates": { "$sum": "$count" } } },
    ];
    let mut cursor = collection.aggregate(pipeline, None).await?;

    while let Some(doc) = cursor.try_next().await? {
        println!("{}", doc);
    }
    Ok(())
}

pub async fn total_count(collection: &Collection<Document>) {
    let count_money_tap_entries = collection
        .count_documents(doc! { "partner": "MoneyTap" }, None)
        .await
        .unwrap();
    let money_tap_entries_false = collection
        .count_documents(doc! { "partner": "MoneyTap", "partnerSent": false }, None)
        .await
        .unwrap();
    let money_tap_entries_true = collection
        .count_documents(doc! { "partner": "MoneyTap", "partnerSent": true }, None)
        .await
        .unwrap();
    let not_banned = collection
        .count_documents(
            doc! {
                "partner": "MoneyTap",
                "partnerSent": false,
                "$or": [
                    { "isBanned": false },
                    { "isBanned": { "$exists": false } }
                ]
            },
            None,
        )
        .await
        .unwrap();

    println!("MoneyTap Total: {}", count_money_tap_entries);
    println!("MoneyTap Sent: {}", money_tap_entries_true);
    println!("MoneyTap Pending: {}", money_tap_entries_false);
    println!("MoneyTap notBanned: {}", not_banned);
}

pub async fn pipeline(collection: &Collection<Document>) -> Result<(), Box<dyn Error>> {
    let match_stage = doc! {
            "$match": {
                "updatedAt": {
                    "$gte": DateTime::parse_rfc3339_str("2024-05-15T00:00:00Z")?,
                    "$lt": DateTime::parse_rfc3339_str("2024-05-16T00:00:00Z")?
                }
            }

    };
    let project_stage = doc! {
        "$project": {
            "phone": 1,
            "createdAt": 1,
            "updatedAt": 1,
            "pincode": 1,
            "partner": 1,
            "name": 1,
            "employment": 1,
            "accounts": 1,
        },
    };

    let add_fields_stage = doc! {
        "$addFields": {
            "accounts_no": {
                "$cond": {
                    "if": { "$isArray": "$accounts" },
                    "then": { "$size": "$accounts" },
                    "else": 0,
                },
            },
            /* "accounts_names": {
                "$reduce": {
                    "input": "$accounts",
                    "initialValue": "",
                    "in": { "$concat": [ "$$value", { "$cond": [ { "$eq": [ "$$value", "" ] }, "", " " ] }, "$$this.name" ] },
                },
            }, */
            "cashe_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Cashe"] },
                        },
                    },
                    0,
                ],
            },
            "fibe_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Fibe"] },
                        },
                    },
                    0,
                ],
            },
            "moneyview_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "MoneyView"] },
                        },
                    },
                    0,
                ],
            },
            "payme_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Payme"] },
                        },
                    },
                    0,
                ],
            },
            "upwards_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Upwards"] },
                        },
                    },
                    0,
                ],
            },
            "prefr_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Prefr"] },
                        },
                    },
                    0,
                ],
            },
            "lk_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "LendingKart"] },
                        },
                    },
                    0,
                ],
            },
            "faircent_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Faircent"] },
                        },
                    },
                    0,
                ],
            },
            "zype_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Zype"] },
                        },
                    },
                    0,
                ],
            },
            "loantap_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "LoanTap"] },
                        },
                    },
                    0,
                ],
            },
            "um_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Upwards MarketPlace"] },
                        },
                    },
                    0,
                ],
            },
            "mpocket_details": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": "$accounts",
                            "as": "account",
                            "cond": { "$eq": ["$$account.name", "Mpocket"] },
                        },
                    },
                    0,
                ],
            },
        },
    };

    let add_fields_stage_2 = doc! {
        "$addFields": {
            "cashe_status": {
                "$cond": {
                    "if": { "$gt": ["$cashe_details.status", null] },
                    "then": "$cashe_details.status",
                    "else": "$$REMOVE",
                }
            },
            "cashe_id": {
                "$cond": {
                    "if": { "$gt": ["$cashe_details.id", null] },
                    "then": "$cashe_details.id",
                    "else": "$$REMOVE",
                }
            },
            "cashe_loanAmount": {
                "$cond": {
                    "if": { "$gt": ["$cashe_details.amount", null] },
                    "then": "$cashe_details.amount",
                    "else": "$$REMOVE",
                }
            },
            "fibe_status": {
                "$cond": {
                    "if": { "$gt": ["$fibe_details.status", null] },
                    "then": "$fibe_details.status",
                    "else": "$$REMOVE",
                }
            },
            "fibe_id": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$fibe_details.id", null] },
                            { "$ne": ["$fibe_details.id", "null"] },
                        ]
                    },
                    "then": "$fibe_details.id",
                    "else": "$$REMOVE",
                }
            },
            "fibe_loanAmount": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$fibe_details.loanAmount", 0] },
                            { "$gt": ["$fibe_details.loanAmount", null] },
                        ]
                    },
                    "then": "$fibe_details.loanAmount",
                    "else": "$$REMOVE",
                }
            },
            "payme_status": {
                "$cond": {
                    "if": { "$gt": ["$payme_details.msg", null] },
                    "then": "$payme_details.msg",
                    "else": "$$REMOVE",
                }
            },
            "payme_id": {
                "$cond": {
                    "if": { "$gt": ["$payme_details.user_id", null] },
                    "then": "$payme_details.user_id",
                    "else": "$$REMOVE",
                }
            },
            "payme_loanAmount": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": [{ "$arrayElemAt": ["$payme_details.limit.credit_limit", 0] }, 0] },
                            { "$gt": [{ "$arrayElemAt": ["$payme_details.limit.credit_limit", 0] }, null] },
                        ]
                    },
                    "then": { "$arrayElemAt": ["$payme_details.limit.credit_limit", 0] },
                    "else": "$$REMOVE",
                }
            },
            "moneyview_status": {
                "$cond": {
                    "if": { "$gt": ["$moneyview_details.message", null] },
                    "then": "$moneyview_details.message",
                    "else": "$$REMOVE",
                }
            },
            "moneyview_id": {
                "$cond": {
                    "if": { "$gt": ["$moneyview_details.id", null] },
                    "then": "$moneyview_details.id",
                    "else": "$$REMOVE",
                }
            },
            "moneyview_loanAmount": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": [{ "$arrayElemAt": ["$moneyview_details.offers.loanAmount", 0] }, 0] },
                            { "$gt": [{ "$arrayElemAt": ["$moneyview_details.offers.loanAmount", 0] }, null] },
                        ]
                    },
                    "then": { "$arrayElemAt": ["$moneyview_details.offers.loanAmount", 0] },
                    "else": "$$REMOVE",
                }
            },
            "upwards_status": {
                "$cond": {
                    "if": { "$gt": ["$upwards_details.status", null] },
                    "then": "$upwards_details.status",
                    "else": "$$REMOVE",
                }
            },
            "upwards_id": {
                "$cond": {
                    "if": { "$gt": ["$upwards_details.id", null] },
                    "then": "$upwards_details.id",
                    "else": "$$REMOVE",
                }
            },
            "prefr_status": {
                "$cond": {
                    "if": { "$gt": ["$prefr_details.response.eventName", null] },
                    "then": "$prefr_details.response.eventName",
                    "else": "$$REMOVE",
                }
            },
            "prefr_id": {
                "$cond": {
                    "if": { "$gt": ["$prefr_details.id", null] },
                    "then": "$prefr_details.id",
                    "else": "$$REMOVE",
                }
            },
            "lendingkart_status": {
                "$cond": {
                    "if": { "$gt": ["$lk_details.message", null] },
                    "then": "$lk_details.message",
                    "else": "$$REMOVE",
                }
            },
            "lendingkart_id": {
                "$cond": {
                    "if": { "$gt": ["$lk_details.leadId", null] },
                    "then": "$lk_details.leadId",
                    "else": "$$REMOVE",
                }
            },
            "faircent_status": {
                "$cond": {
                    "if": { "$gt": ["$faircent_details.status", null] },
                    "then": "$faircent_details.status",
                    "else": "$$REMOVE",
                }
            },
            "faircent_id": {
                "$cond": {
                    "if": { "$gt": ["$faircent_details.id", null] },
                    "then": "$faircent_details.id",
                    "else": "$$REMOVE",
                }
            },
            "faircent_loanAmount": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$faircent_details.res.result.offer_amount", 0] },
                            { "$gt": ["$faircent_details.res.result.offer_amount", null] },
                        ]
                    },
                    "then": "$faircent_details.res.result.offer_amount",
                    "else": "$$REMOVE",
                }
            },
            "zype_status": {
                "$cond": {
                    "if": { "$gt": ["$zype_details.status", null] },
                    "then": "$zype_details.status",
                    "else": "$$REMOVE",
                }
            },
            "zype_loanAmount": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$zype_details.offer", 0] },
                            { "$gt": ["$zype_details.offer", null] },
                        ]
                    },
                    "then": "$zype_details.offer",
                    "else": "$$REMOVE",
                }
            },
            "loantap_status": {
                "$cond": {
                    "if": { "$gt": ["$loantap_details.message", null] },
                    "then": "$loantap_details.message",
                    "else": "$$REMOVE",
                }
            },
            "loantap_id": {
                "$cond": {
                    "if": { "$gt": ["$loantap_details.data.lapp_id", null] },
                    "then": "$loantap_details.data.lapp_id",
                    "else": "$$REMOVE",
                }
            },
            "upwards_marketplace_status": {
                "$cond": {
                    "if": { "$ifNull": ["$um_details.data.is_success", null] },
                    "then": {
                        "$cond": {
                            "if": "$um_details.data.is_success",
                            "then": "success",
                            "else": "failure",
                        }
                    },
                    "else": "$$REMOVE",
                }
            },
            "upwards_marketplace_id": {
                "$cond": {
                    "if": { "$gt": ["$um_details.data.loan_data.customer_id", null] },
                    "then": "$um_details.data.loan_data.customer_id",
                    "else": "$$REMOVE",
                }
            },
            "mpocket_status": {
                "$cond": {
                    "if": { "$ifNull": ["$mpocket_details.success", null] },
                    "then": {
                        "$cond": {
                            "if": "$mpocket_details.success",
                            "then": "success",
                            "else": "failure",
                        }
                    },
                    "else": "$$REMOVE",
                }
            },
            "mpocket_id": {
                "$cond": {
                    "if": { "$gt": ["$mpocket_details.data.requestId", null] },
                    "then": "$mpocket_details.data.requestId",
                    "else": "$$REMOVE",
                }
            },
        }
    };

    let project_stage_2 = doc! {
            "$project": {
            "_id": 0,
            "accounts": 0,
            "cashe_details": 0,
            "fibe_details": 0,
            "payme_details": 0,
            "moneyview_details": 0,
            "upwards_details": 0,
            "prefr_details": 0,
            "lk_details": 0,
            "faircent_details": 0,
            "zype_details": 0,
            "loantap_details": 0,
            "um_details": 0,
            "mpocket_details": 0,
        },
    };

    let sort_stage = doc! { "$sort": { "accounts_size": -1, "createdAt": -1 } };

    let limit_stage = doc! { "$limit": 5 };

    let pipeline: Vec<Document> = vec![
        match_stage,
        project_stage,
        add_fields_stage,
        add_fields_stage_2,
        project_stage_2,
        sort_stage,
        limit_stage,
    ];
    fn colorize_json(json: &Value, indent: usize) -> String {
        match json {
            Value::Object(map) => {
                let contents: Vec<String> = map
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}{}: {}",
                            "  ".repeat(indent + 1),
                            k.green(),
                            colorize_json(v, indent + 1)
                        )
                    })
                    .collect();
                format!("{{\n{}\n{}}}", contents.join(",\n"), "  ".repeat(indent))
            }
            Value::Array(arr) => {
                let contents: Vec<String> =
                    arr.iter().map(|v| colorize_json(v, indent + 1)).collect();
                format!("[{}]", contents.join(", "))
            }
            /* date */
            Value::String(s) => {
                if s.starts_with("20") && s.len() > 20 {
                    s.purple().to_string()
                } else {
                    format!("'{}'", s.cyan())
                }
            }
            Value::Number(n) => n.to_string().yellow().to_string(),
            Value::Bool(b) => b.to_string().yellow().to_string(),
            Value::Null => "null".bright_black().to_string(),
        }
    }

    let mut cursor = collection.aggregate(pipeline, None).await?;

    while let Some(mut doc) = cursor.try_next().await? {
        if let Some(created_at) = doc.get("createdAt").and_then(|v| v.as_datetime()) {
            if let Ok(created_at_str) = created_at.try_to_rfc3339_string() {
                doc.insert("createdAt", created_at_str);
            }
        }

        if let Some(updated_at) = doc.get("updatedAt").and_then(|v| v.as_datetime()) {
            if let Ok(updated_at_str) = updated_at.try_to_rfc3339_string() {
                doc.insert("updatedAt", updated_at_str);
            }
        }

        let json_value: Value = serde_json::to_value(&doc)?;

        let colorized_json = colorize_json(&json_value, 0);

        println!("{}", colorized_json);
    }
    Ok(())
}
