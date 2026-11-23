# HTTP の転送

HTTP サーバーは axum、外向きの HTTP は共有の reqwest Client です。S3 の署名と操作には shiguredo_s3 を使います。

## 外向きの接続

- redirect、暗黙の再試行、proxy、圧縮と自動展開は無効です。証明書は WebPKI の trust root で検証します。
- 接続は 10 秒、読み取りの停滞は 60 秒でタイムアウトします。全体の上限は、小さな要求が 60 秒、S3 の stream と PUT が 3600 秒です。
- 接続は共有します。待機中の接続は接続先ごとに最大 8 本、全体で最大 64 本で、30 秒使われなかった接続は捨てます。
  本文を読み切っていない接続は再利用しません。
- 再利用した接続で、応答を受け取る前に通信エラーが起きた場合、GET と HEAD だけを新しい接続で 1 回やり直します。
  アップロードなどの更新操作は自動で再送しません。
- 署名した method / path / query / headers / body をそのまま送ります。URL の dot segment などが reqwest に正規化されて
  別のオブジェクトを指すことになる場合は、送らずにエラーにします。

## 本文の大きさ

| 対象 | 上限 |
|---|---|
| JSON | 1MiB |
| Base64 の API（`put_object_and_notify` を含む）、Slack へのバイナリ upload | 64MiB |
| ストリーミング upload | 全量をメモリに持たない。8MiB ずつ multipart にする |

経路ごとの上限は `src/server/routes.rs` の表で決めます。表にない経路は JSON と同じ扱いです。

- preview は、reqwest の body stream を backpressure を保ったまま axum の Body へ渡します。
- ストリーミング upload は、読み取りが 60 秒停滞したら multipart を abort します。明示的な失敗、切断、完了の失敗でも abort します。

## 終了

SIGTERM で新しい受付を止め、進行中の要求を最大 80 秒待ちます。Pod の猶予は 90 秒にします。
強制的に止まった場合も、呼び出し側（home-camera）は spool と固定のキーを保っているので、送り直せます。
結果が分からない通知は uncertain として扱い、HTTP の層では再送しません（[通知キュー](notification-state.md)）。

## ログ

ダウンロードでは `s3::download` のログに、`upstream_headers_ms`（S3 の応答ヘッダーまで）、`transfer_ms`（クライアントへの転送時間）、
`bytes_sent`（送った本文のバイト数）を記録します。転送時間には、S3 からの読み込みとクライアント側の受信待ちの両方が含まれます。

## 検証

```bash
cargo test --locked --all-targets
cargo build --locked --example http_fixture
python3 scripts/check-stream-memory.py target/debug/examples/http_fixture
```

メモリの確認は本番には接続せず、64MiB と 256MiB の upload / download で `/proc` の VmHWM を比べ、ファイルサイズに比例しないことを確かめます。
`http_fixture` は試験専用で、配布イメージには含めません。
