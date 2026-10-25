# HTTP transport

HTTPサーバーはaxum Router/Handler、外向きHTTPは共有reqwest Client。業務handlerのRequest/Responseとshiguredo_s3の署名/操作は維持し、独自socket受信・HTTP decoder・TLS・接続poolは使わない。wire decoderは試験用の模擬S3だけに残す。

redirect・暗黙再試行・proxy・圧縮/自動展開は無効。既存と同じWebPKI trust rootで証明書を検証し、接続10秒/読取停滞60秒、小さな要求の全体60秒、S3のstream/PUTは全体3600秒。署名したmethod/path/query/headers/bodyを維持する。URLのdot segment等がreqwestで正規化される場合、別objectへの要求を送らずエラーにする。

S3 uploadは8MiBずつmultipart化し、previewはreqwest body streamからaxum Bodyへbackpressureを保って渡す。JSONは1MiB、既存base64/Slack raw uploadは64MiB。streaming uploadは全量bufferを持たず、読取停滞60秒でmultipartを解除する。明示失敗・切断・完了失敗もabortする。

SIGTERMは新規受付を停止し、既存要求を最大80秒待つ。Pod猶予90秒を指定する。強制停止時も呼出元cameraのspool/固定keyを保持して再送できる。完了不明な通知は従来のuncertainとして扱い、HTTP層は再送しない。

検証: `cargo test --locked --all-targets`、隔離DBのnotifications、`python3 scripts/check-stream-memory.py target/debug/api-hub`。memory fixtureは本番へ接続せず、64MiBと256MiBのupload/downloadで/procのVmHWMを比較する。デバッグbinaryの実測uploadは58,128/66,112KiB、downloadは18,344/17,812KiBで、fileサイズに比例しない。

公開workflowは同一commitのAPI/stream/通知回復/memory fixtureの成功を要求する。Nixのstatic・CA・非root・x86-64 CPU設定は維持する。
