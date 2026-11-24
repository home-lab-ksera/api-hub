use crate::reply::{Reply, ReplyExt, reply};

pub fn health() -> Reply {
    reply(200)
        .header("Content-Type", "text/plain; charset=utf-8")
        .with_body(b"ok".to_vec())
}
