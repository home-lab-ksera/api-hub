//! サーバーの起動と停止。API と通知配送の task を1つのプロセスで監督する。

use crate::config::state::AppState;

pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    use std::future::IntoFuture;
    let (stopped, waiting) = tokio::sync::oneshot::channel();
    let serve = axum::serve(listener, super::router(state))
        .with_graceful_shutdown(async move {
            shutdown.await;
            let _ = stopped.send(());
        })
        .into_future();
    tokio::pin!(serve);
    tokio::select! {
        result=&mut serve=>result,
        _=waiting=>tokio::time::timeout(std::time::Duration::from_secs(80),&mut serve).await
            .unwrap_or_else(|_|Err(std::io::Error::new(std::io::ErrorKind::TimedOut,"HTTP drain deadline exceeded"))),
    }
}

/// 同じPgPoolを共有し、APIと1本の通知配送taskを監督する。
pub async fn serve_integrated(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let pool = state
        .database
        .clone()
        .ok_or_else(|| std::io::Error::other("notification database is required"))?;
    let (stop, rx) = tokio::sync::watch::channel(false);
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        shutdown.await;
        let _ = signal_stop.send(true);
    });
    let api = tokio::spawn(serve(listener, state.clone(), stop_requested(rx.clone())));
    let worker = tokio::spawn(crate::notifications::worker(state, stop_requested(rx)));
    let result = supervise(api, worker, stop).await;
    signal.abort();
    // HTTPと進行中通知の確定後にだけ、共有poolを閉じる。
    pool.close().await;
    result
}
async fn stop_requested(mut rx: tokio::sync::watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}
async fn supervise(
    mut api: tokio::task::JoinHandle<std::io::Result<()>>,
    mut worker: tokio::task::JoinHandle<()>,
    stop: tokio::sync::watch::Sender<bool>,
) -> std::io::Result<()> {
    let (api_done, result) = tokio::select! {
        result=&mut api=>(true,match result {
            Ok(Ok(())) if *stop.borrow()=>Ok(()),
            Ok(Err(error))=>Err(error),
            other=>Err(std::io::Error::other(format!("HTTP task exited unexpectedly: {other:?}"))),
        }),
        result=&mut worker=>(false,match result {
            Ok(()) if *stop.borrow()=>Ok(()),
            other=>Err(std::io::Error::other(format!("notification task exited unexpectedly: {other:?}"))),
        }),
    };
    let _ = stop.send(true);
    // 片方の異常終了でも残ったtaskを放置せず、新規受付/claimを止める。
    let remaining = async {
        if api_done {
            (&mut worker).await.map_err(std::io::Error::other)?;
        } else {
            (&mut api).await.map_err(std::io::Error::other)??;
        }
        Ok(())
    };
    let drained = tokio::time::timeout(std::time::Duration::from_secs(85), remaining)
        .await
        .unwrap_or_else(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "process drain deadline exceeded",
            ))
        });
    if drained.is_err() {
        api.abort();
        worker.abort();
    }
    result.and(drained)
}

#[cfg(test)]
mod supervision_tests {
    use super::*;
    #[tokio::test]
    async fn unexpected_worker_exit_stops_http_and_fails_process() {
        for panic in [false, true] {
            let (stop, rx) = tokio::sync::watch::channel(false);
            let api = tokio::spawn(async move {
                stop_requested(rx).await;
                Ok(())
            });
            let worker = tokio::spawn(async move {
                assert!(!panic, "fixture worker panic");
            });
            let result = supervise(api, worker, stop.clone()).await;
            assert!(result.is_err());
            assert!(*stop.borrow());
        }
    }
    #[tokio::test]
    async fn api_failure_stops_claims_and_waits_for_worker() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let api = tokio::spawn(async { Err(std::io::Error::other("fixture HTTP failure")) });
        let worker = tokio::spawn(async move {
            stop_requested(rx).await;
        });
        assert!(supervise(api, worker, stop.clone()).await.is_err());
        assert!(*stop.borrow());
    }
}
