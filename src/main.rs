use std::{collections::HashMap, sync::Arc};

use anyhow::*;
use futures::{stream, StreamExt, TryStreamExt};
use redis::aio::ConnectionManager;
use store::{Key, Store, Update};
use warp::Filter;

mod store;

#[tokio::main]
async fn main() -> Result<()> {
    let client = redis::Client::open(
        std::env::var("REDIS_URL")
            .ok()
            .and_then(|s| if s == "" { None } else { Some(s) })
            .as_deref()
            .unwrap_or("redis://127.0.0.1/"),
    )?;
    let conm = ConnectionManager::new(client)
        .await
        .expect("Couldn't connect");

    println!("Connected");

    let store = Store::new(conm);

    let see = {
        let store = store.clone();

        warp::path!("see" / String).then(move |token: String| {
            let store = store.clone();

            async move {
                // let filt: Key = (token, "").try_into()?;
                // let state = store.get_state(&filt).await?;

                let mut keys = Vec::from_iter(store.get_all_keys(&token, "").await?);
                keys.sort();

                let res = stream::iter(keys)
                    .then(|key| async {
                        let key = key;
                        let state = store.get_state(&key).await?;

                        Ok(format!(
                            "<b>{}</b> <progress value='{}' max='{}'>what </progress> <i>{}</i>",
                            key.key,
                            state.current.unwrap_or(0),
                            state.max.unwrap_or(100),
                            state.state.as_deref().unwrap_or("?")
                        ))
                    })
                    .try_collect::<Vec<_>>()
                    .await?
                    .join("<br/><br/><br/>\n\n\n");

                Ok(res)
            }
        })
    };

    let hello = warp::path!("send" / String)
        .and(warp::query::<Vec<(String, String)>>())
        .then(move |token: String, query: Vec<(String, String)>| {
            let store = store.clone();

            async move {
                let updates: Result<Vec<Update>, _> = query
                    .into_iter()
                    .map(|p| Update::from_query(&token, p))
                    .collect();

                for u in updates? {
                    store.clone().update(&u).await?;
                }

                Ok("OK".to_owned())
            }
        });

    let routes = hello
        .or(see)
        .unify()
        .map(|res: anyhow::Result<String>| res.unwrap_or_else(|e| format!("Error: {:?}", e)))
        .map(warp::reply::html);

    warp::serve(routes)
        .run((
            [127, 0, 0, 1],
            std::env::var("PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(3030),
        ))
        .await;

    Ok(())
}
