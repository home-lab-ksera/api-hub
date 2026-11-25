# api-hub

Slack と S3 互換ストレージ（RustFS）への窓口になる、Rust 製の API サーバーです。
ほかのアプリは S3 や Slack に直接つながず、api-hub を呼びます。Slack への通知は DB のキューに永続化してから配送します。

- API の定義: [openapi.json](openapi.json)（`GET /openapi.json` でも配信）
- 通知キューの動き: [docs/notification-state.md](docs/notification-state.md)
- HTTP の転送と終了処理: [docs/http-transport.md](docs/http-transport.md)
- 本番の配置: [home-lab-k8s のアプリ一覧](https://github.com/home-lab-ksera/home-lab-k8s/blob/main/docs/apps.md#api-hub)

## 起動

```bash
cargo run --bin api-hub              # HTTP API と通知の配送（--serve と同じ）
cargo run --bin api-hub -- --migrate # api_hub DB の migration だけを実行して終了
```

3000 番で待ち受けます。HTTP API と通知の配送 task は同じプロセスで動きます。それ以外の引数はエラーです。

## 環境変数

| 変数 | 必須 | 内容 |
|---|---|---|
| `SLACK_BOT_TOKEN` | ○ | Slack の Bot token |
| `RUSTFS_S3_ACCESS_KEY_ID`、`RUSTFS_S3_SECRET_ACCESS_KEY` | ○ | S3 の認証情報 |
| `PGDATABASE`、`PGHOST`、`PGUSER`、`PGPASSWORD`、`PGSSLROOTCERT` | ○ | 通知キューの DB。`verify-full` で接続する。`PGPORT` は任意 |
| `DATABASE_URL` | | 上の `PG*` の代わりに使える（隔離テスト用） |
| `PGPOOL_MAX` | | 接続 pool の上限（本番は 3、migration は 1） |
| `RUSTFS_S3_ENDPOINT` | | S3 の接続先（例: `http://rustfs.example.local:9000`） |
| `RUSTFS_S3_BROWSER_ENDPOINT` | | ブラウザー向けの署名付き URL に使う接続先（本番は `https://s3.internal.qroksera.com`） |
| `RUSTFS_S3_REGION` | | 既定 `us-east-1` |
| `RUSTFS_S3_USE_PATH_STYLE` | | 既定 `true` |
| `RUSTFS_S3_SESSION_TOKEN` | | 一時認証情報を使う場合 |
| `SLACK_API_BASE_URL` | | 既定 `https://slack.com/api` |
| `RUST_LOG` | | 既定 `info`（例: `info,api_hub::service=debug`） |
| `LOG_TARGET`、`LOG_THREAD`、`LOG_LINE` | | `true` でモジュール名・スレッド・行番号を出力 |

DB の設定は必須ですが、接続は必要になったときに行います。DB が一時的に止まっていても、S3 の API は起動します。

ログは標準出力に JSONL で出します（1 行 1 JSON、リクエスト ID 付き）。`jq 'select(.level == "ERROR")'` などで絞り込めます。
すべての要求に `http::request` と `http::response` のアクセスログが出ます。API のエラーは、利用者の誤り（4xx）が `WARN`、サーバーの障害（5xx）が `ERROR` です。

## API

JSON を受け取る `POST` は `Content-Type: application/json` が必須です（それ以外は 415）。

### 状態

- `GET /health`
- `GET /openapi.json`
- `GET /metrics`: 通知キューの状態（[通知キュー](docs/notification-state.md#メトリクス)）

### 通知（キュー経由）

- `POST /notifications`: 通知を永続化して 202 を返す。配送は非同期
- `GET /notifications/{id}`: 状態を取得
- `POST /notifications/{id}/retry`: failed / uncertain を手で再試行

詳細は [通知キュー](docs/notification-state.md) を参照してください。

### Slack（直接）

- `POST /slack/message`
  - body: `{ "channel": "C123", "text": "hello" }`
  - `thread_ts` と `blocks`（最大 50 件）も指定できます。`text` は通知・読み上げ用の代替テキストとして必須です。
- `POST /slack/message/update`
  - body: `{ "channel": "C123", "ts": "1234567890.123456", "text": "処理が完了しました", "blocks": [] }`
  - Bot 自身の投稿だけが対象で、`chat:write` が必要です。`blocks` を消すときは `blocks: []` を指定します。
- `POST /slack/upload/image?channel=C123&file_name=hello.png`
  - header: `Content-Type: image/png|image/jpeg|image/webp|image/gif`、body: 画像のバイナリ
- `POST /slack/upload/pdf?channel=C123&file_name=document.pdf`
  - header: `Content-Type: application/pdf`、body: PDF のバイナリ

投稿・更新のレスポンスは、Slack のレスポンス JSON を文字列として JSON エンコードしたものです。
`ts` を取り出すときは、外側の JSON 文字列をデコードしてから、中身を JSON として解析します。

### S3: オブジェクト

- `POST /s3/put_object_base64`
  - body: `{ "bucket": "b", "key": "path/a.txt", "file_data_base64": "...", "content_type": "text/plain" }`
- `POST /s3/put_object_stream?bucket=b&key=path%2Fvideo.mp4`（下の「ストリーミングアップロード」）
- `POST /s3/put_object_and_notify`（下の「保存して Slack に通知」）
- `GET /s3/preview/{bucket}/{*key}`
  - S3 のオブジェクトをストリーミングで配信します。Range と条件付き GET に対応します。バイナリの取得には Base64 の API よりこちらを使います。
- `POST /s3/get_object_base64`、`POST /s3/head_object`、`POST /s3/delete_object`
  - body: `{ "bucket": "b", "key": "path/a.txt" }`
- `POST /s3/delete_objects`
  - body: `{ "bucket": "b", "objects": [{ "key": "path/a.txt" }, { "key": "path/b.txt", "version_id": "..." }], "quiet": false }`
- `POST /s3/list_objects_v2`
  - body: `{ "bucket": "b", "prefix": "path/", "max_keys": 1000 }`
- `POST /s3/copy_object`
  - body: `{ "bucket": "destination", "key": "backup/report.pdf", "source_bucket": "source", "source_key": "reports/report.pdf", "source_version_id": null }`
  - 元のメタデータを引き継ぎ、同名のキーは上書きします。キーは URL エンコードせずに渡します。5GB を超えるオブジェクトには対応していません。
- `POST /s3/presigned_get_object`、`POST /s3/presigned_put_object`
  - body: `{ "bucket": "b", "key": "path/a.txt", "expires_in_secs": 900 }`

### S3: ブラウザー向け

- `POST /s3/browser_get_object`: `pdfs/hitomi` の PDF・プレビュー、`pdfs/videos` の MP4、録画バケットの MP4 に限って、900 秒の GET URL を発行します。
  URL は `RUSTFS_S3_BROWSER_ENDPOINT` を向きます。
- `GET /s3/browser_open`: 同じ対象について、新しく署名した GET URL へリダイレクトします。

利用者ごとの認証はありません。内部ネットワークからの利用が前提です。

### S3: タグ

- `POST /s3/put_object_tagging`
  - body: `{ "bucket": "b", "key": "report.pdf", "tags": [{ "key": "status", "value": "done" }], "version_id": null }`
  - タグ全体を置き換えます。最大 10 件、キー 128 文字・値 256 文字まで。`tags: []` で全タグを消します。
- `POST /s3/get_object_tagging`、`POST /s3/delete_object_tagging`
  - body: `{ "bucket": "b", "key": "report.pdf", "version_id": null }`

### S3: multipart と bucket

- `POST /s3/create_multipart_upload`、`/s3/upload_part_base64`、`/s3/complete_multipart_upload`、`/s3/abort_multipart_upload`、`/s3/list_parts`、`/s3/list_multipart_uploads`
- `POST /s3/list_buckets`、`/s3/create_bucket`、`/s3/head_bucket`、`/s3/delete_bucket`

body の形は [openapi.json](openapi.json) を参照してください。

## ストリーミングアップロード

```bash
curl --fail-with-body 'http://localhost:3000/s3/put_object_stream?bucket=b&key=path%2Fvideo.mp4' \
  -H 'Content-Type: video/mp4' --data-binary @video.mp4
```

- query の bucket と key は URL エンコードします。`Content-Type` を省くと `application/octet-stream` です。
- 最大 8MiB ずつ受け取って S3 へ順に送ります。8MiB 以上は multipart upload になり、切断やエラーのときは abort を試みます。
- 1 リクエストは最大 10,000 パート（約 78GiB）です。
- プロセスが強制終了すると abort できないことがあります。未完了の multipart upload は、ストレージ側の lifecycle で消します。
- レスポンス: `{ "e_tag": "...", "version_id": null }`

リバースプロキシを通す場合は、このパスのリクエストボディのバッファリングを切り、サイズ制限とタイムアウトを動画のサイズに合わせます。

## 保存して Slack に通知

`POST /s3/put_object_and_notify` は、Base64 のデータを保存し、署名付きの GET URL を Slack に投稿します。

```json
{
  "bucket": "b",
  "key": "reports/hello.txt",
  "file_data_base64": "aGVsbG8=",
  "content_type": "text/plain",
  "channel": "C123",
  "text": "レポートを保存しました",
  "expires_in_secs": 900
}
```

- `text` の既定値は `File uploaded`、URL の有効期間は既定 900 秒（1〜604800 秒）で、署名した時点から数えます。
- URL は `RUSTFS_S3_ENDPOINT` を向くため、Slack の受信者がそのアドレスに届く必要があります。
- 保存に失敗したら Slack には投稿しません。保存に成功して通知だけ失敗した場合は HTTP 200 で `uploaded: true, notified: false` を返し、
  保存したファイルは消しません。返された URL を使って `/slack/message` で通知だけをやり直します。
- 自動の再試行はしません。再試行が要る通知には `POST /notifications` を使います。

## ブラウザーからの利用制限

api-hub に利用者認証はありません。Web ページからの不正な操作を防ぐため、次の制限があります。

- `Origin` または `Sec-Fetch-Site` が付いた更新リクエスト（ブラウザーからの呼び出し）は、
  許可した画面（`https://dashboard.internal.qroksera.com`）から `/s3/list_objects_v2`・`/s3/browser_get_object`・
  `/s3/presigned_get_object`・`/s3/put_object_stream` への呼び出しだけを受け付けます（それ以外は 403）。
  クラスタ内のクライアントはこれらのヘッダーを送らないため、影響を受けません。
- `GET /s3/preview/...` は、PDF・画像（SVG を除く）・動画・音声だけをブラウザー内で表示します。
  それ以外は `Content-Disposition: attachment` と `Content-Security-Policy: sandbox` を付けてダウンロードさせます。
- 本番の Gateway（`api-hub.internal.qroksera.com`）に通しているのは、viewer が使う読み取りの経路だけです。
  更新・削除・Slack・通知は、クラスタ内の `api-hub.sandbox.svc:3000` から呼びます
  （[アクセス経路](https://github.com/home-lab-ksera/home-lab-k8s/blob/main/docs/access.md#url)）。
  ブラウザーから使う経路を足すときは、`src/server.rs` の `is_browser_path` と Gateway の HTTPRoute の両方を直します。

## エラー

RFC 9457 の `application/problem+json` で返します。

```json
{ "type": "about:blank", "title": "Bad Request", "status": 400, "detail": "Body is not a valid PDF document" }
```

## テスト

```bash
cargo test --locked --all-targets
TEST_DATABASE_URL=postgres://.../isolated_test cargo test --locked --test notifications -- --ignored
cargo build --locked --example http_fixture
python3 scripts/check-stream-memory.py target/debug/examples/http_fixture
```

2 行目は通知キューの回復試験で、隔離した PostgreSQL が要ります。3〜4 行目は、大きなファイルの転送でメモリがファイルサイズに比例しないことを確かめます。

## CI と配布

`build-and-push-api-hub.yml` が、`ci.yml`（テスト、通知の回復試験、ストリーミングのメモリ確認）と home-lab-k8s の共通 workflow（fmt・clippy・cargo-audit）を実行し、
両方が成功したら、既定ブランチへの push のときだけイメージを Harbor へ公開します。
公開からクラスタへの反映までは [アプリ配布](https://github.com/home-lab-ksera/home-lab-k8s/blob/main/docs/app-delivery.md) を参照してください。

```bash
docker build -t api-hub .
```

`rust-toolchain.toml` と同じ版の Rust（Alpine）で静的バイナリを作り、バイナリと CA 証明書だけを scratch イメージに入れます。
実行ユーザーは 65532:65532 です。x86_64 では、AES / AVX / AVX2 を前提にしない CPU 設定でビルドします。
