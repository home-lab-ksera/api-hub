use crate::reply::{Reply, ReplyExt, reply};

const OPENAPI_JSON: &str = include_str!("../../openapi.json");

pub fn openapi_json() -> Reply {
    reply(200)
        .header("Content-Type", "application/json")
        .with_body(OPENAPI_JSON.as_bytes().to_vec())
}
