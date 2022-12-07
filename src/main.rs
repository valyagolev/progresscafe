use warp::Filter;

mod store;

#[tokio::main]
async fn main() {
    let hello = warp::path!("send" / String).map(|name| format!("Hello, {}!", name));

    warp::serve(hello).run(([127, 0, 0, 1], 3030)).await;
}
