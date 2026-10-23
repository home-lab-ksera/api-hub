# api-hub

Slack APIとS3互換ストレージ API へのラッパーを提供するRust製APIサーバーです。

OpenAPI definition: `openapi.yaml`

## API

- `GET /health`
  - body: なし
- `GET /openapi.json`
  - body: なし
- `POST /slack/message`
  - body: `{ "channel": "C123", "text": "hello" }`
- `POST /slack/upload/image?channel=C123&file_name=hello.png`
  - header: `Content-Type: image/png|image/jpeg|image/webp|image/gif`
  - body: 画像バイナリ
- `POST /slack/upload/pdf?channel=C123&file_name=document.pdf`
  - header: `Content-Type: application/pdf`
  - body: PDFバイナリ
- `POST /s3/put_object_base64`
  - body: `{ "bucket": "b", "key": "path/a.txt", "file_data_base64": "...", "content_type": "text/plain" }`
- `POST /s3/put_object_stream?bucket=b&key=path%2Fvideo.mp4`
  - header: `Content-Type: video/mp4`（省略時は `application/octet-stream`）
  - body: バイナリ。Content-Length / chunked に対応。
  - 最大8 MiBずつ受信してS3へ順次転送。8 MiB以上は multipart upload を使用し、切断・転送エラー時は abort を試行します。
  - response: `{ "e_tag": "...", "version_id": null }`
  - 1リクエストあたり最大10,000パート（約78 GiB）。プロセス強制終了などで abort できない場合に備え、ストレージ側の未完了 multipart upload の削除ポリシーを推奨します。
- `GET /s3/preview/{bucket}/{*key}`
  - body: なし（S3オブジェクトをストリーミング配信）。バイナリのダウンロードにはBase64 APIよりこちらを推奨。
  - Range・条件付きGETに対応。Content-Lengthがある応答は最大64 KiBずつ転送用バッファへ直接読み込み、ファイル全体の取得完了を待たずに配信します。
- `POST /s3/get_object_base64`
  - body: `{ "bucket": "b", "key": "path/a.txt" }`
- `POST /s3/head_object`
  - body: `{ "bucket": "b", "key": "path/a.txt" }`
- `POST /s3/delete_object`
  - body: `{ "bucket": "b", "key": "path/a.txt" }`
- `POST /s3/delete_objects`
  - body: `{ "bucket": "b", "objects": [{ "key": "path/a.txt" }, { "key": "path/b.txt", "version_id": "..." }], "quiet": false }`
- `POST /s3/list_objects_v2`
  - body: `{ "bucket": "b", "prefix": "path/", "max_keys": 1000 }`
- `POST /s3/create_multipart_upload`
  - body: `{ "bucket": "b", "key": "large.bin" }`
- `POST /s3/upload_part_base64`
  - body: `{ "bucket": "b", "key": "large.bin", "upload_id": "...", "part_number": 1, "part_data_base64": "..." }`
- `POST /s3/complete_multipart_upload`
  - body: `{ "bucket": "b", "key": "large.bin", "upload_id": "...", "parts": [{ "part_number": 1, "e_tag": "\"...\"" }] }`
- `POST /s3/abort_multipart_upload`
  - body: `{ "bucket": "b", "key": "large.bin", "upload_id": "..." }`
- `POST /s3/list_parts`
  - body: `{ "bucket": "b", "key": "large.bin", "upload_id": "..." }`
- `POST /s3/list_multipart_uploads`
  - body: `{ "bucket": "b" }`
- `POST /s3/presigned_get_object`
  - body: `{ "bucket": "b", "key": "path/a.txt", "expires_in_secs": 900 }`
- `POST /s3/presigned_put_object`
  - body: `{ "bucket": "b", "key": "path/a.txt", "expires_in_secs": 900 }`
- `POST /s3/list_buckets`
  - body: なし
- `POST /s3/create_bucket`
  - body: `{ "bucket": "b" }`
- `POST /s3/head_bucket`
  - body: `{ "bucket": "b" }`
- `POST /s3/delete_bucket`
  - body: `{ "bucket": "b" }`

## ストリーミングアップロード

ストリーミングアップロード例（query の bucket / key は URL エンコードしてください）:

```bash
curl --fail-with-body 'http://localhost:3000/s3/put_object_stream?bucket=b&key=path%2Fvideo.mp4' \
  -H 'Content-Type: video/mp4' --data-binary @video.mp4
```

リバースプロキシを経由する場合は、このパスのリクエストボディのバッファリングを無効化し、サイズ制限とタイムアウトも動画サイズに合わせて設定してください。

## Error response (RFC9457)

エラーレスポンスは `application/problem+json` の最小セットで返します。

```json
{
  "type": "about:blank",
  "title": "Bad Request",
  "status": 400,
  "detail": "Body is not a valid PDF document"
}
```

## 環境変数

- `SLACK_BOT_TOKEN` (必須)
- `SLACK_API_BASE_URL` (任意, デフォルト: `https://slack.com/api`)
- `RUSTFS_S3_ACCESS_KEY_ID` (必須)
- `RUSTFS_S3_SECRET_ACCESS_KEY` (必須)
- `RUSTFS_S3_REGION` (任意, デフォルト: `us-east-1`)
- `RUSTFS_S3_ENDPOINT` (任意, 例: `http://rustfs.example.local:9000`)
- `RUSTFS_S3_USE_PATH_STYLE` (任意, デフォルト: `true`)
- `RUSTFS_S3_SESSION_TOKEN` (任意)

## 起動

```bash
cargo run --bin api-hub
```

## S3通信の性能

外向きHTTPクライアントは接続を共有し、同じ接続先への連続リクエストでTCP/TLS接続の確立を省きます。
通常のS3操作とストリーミング取得の両方が対象です。待機接続は接続先ごとに最大8本、全体で最大64本です。
30秒以上使われていない接続は、次のプール操作時に破棄します。本文を読み切っていない接続は再利用しません。

再利用した接続で応答受信前に通信エラーが発生した場合、GET・HEADのみ新しい接続で1回再試行します。
アップロードなどの更新操作は自動再送しません。HEADはContent-Lengthの値にかかわらず、本文を待たずに処理します。

ダウンロードでは `s3::download` ログに `upstream_headers_ms`（S3応答ヘッダーまで）、
`transfer_ms`（クライアントへの転送時間）、`bytes_sent`（本文送信バイト数）を記録します。
転送時間にはS3からの読み込みとクライアント側の受信待ちの両方が含まれます。

実環境での速度改善は、同じオブジェクトに対する初回と連続アクセスの応答時間（p50/p95）で確認してください。
この変更による実S3/RustFSでの短縮時間は未計測です。

## テスト

```bash
cargo test --all-targets
```

## CI

GitHub Actionsで `cargo test --all-targets` を実行します。

## Slack投稿の拡張

`POST /slack/message` は既存の `channel`・`text` に加え、`thread_ts` と `blocks` を受け付けます。
`text` は通知・読み上げ用の代替テキストとして必須です。`blocks` は最大50件です。

```json
{
  "channel": "C123",
  "text": "レポートを作成しました",
  "thread_ts": "1234567890.123456",
  "blocks": [
    { "type": "section", "text": { "type": "mrkdwn", "text": "*レポート作成完了*" } }
  ]
}
```

`POST /slack/message/update` で投稿を更新できます。
`ts` は投稿時のSlackレスポンスに含まれる値を指定します。
投稿・更新のレスポンスは既存APIと同じく、SlackレスポンスJSONを文字列としてJSONエンコードしたものです。
`ts` を取得する際は外側のJSON文字列をデコードした後、その内容をJSONとして解析してください。
Bot自身の投稿が対象で、`chat:write` が必要です。
`blocks` を省略した場合の扱いはSlack APIに従います。明示的に消す場合は `blocks: []` を指定します。

```json
{ "channel": "C123", "ts": "1234567890.123456", "text": "処理が完了しました", "blocks": [] }
```

## S3コピー・タグ操作

- `POST /s3/copy_object`
  - body: `{ "bucket": "destination", "key": "backup/report.pdf", "source_bucket": "source", "source_key": "reports/report.pdf", "source_version_id": null }`
  - 同じ接続先のバケット内・バケット間でコピーします。キーはURLエンコードせず渡してください。
  - 元のメタデータを引き継ぎます。コピー先に同名キーがあれば上書きします。
  - 単一のCopyObjectを使用します。5 GBを超えるオブジェクトの自動multipart copyは未対応です。
- `POST /s3/put_object_tagging`
  - body: `{ "bucket": "b", "key": "report.pdf", "tags": [{ "key": "status", "value": "done" }], "version_id": null }`
  - タグ全体を置き換えます。最大10件、キーは重複不可。キー128文字・値256文字まで。
  - `tags: []` で全タグを削除できます。
- `POST /s3/get_object_tagging`
  - body: `{ "bucket": "b", "key": "report.pdf", "version_id": null }`
  - response: `{ "version_id": null, "tags": [{ "key": "status", "value": "done" }] }`
- `POST /s3/delete_object_tagging`
  - body: `{ "bucket": "b", "key": "report.pdf", "version_id": null }`
  - オブジェクト本体を残し、全タグを削除します。

コピーにはコピー元の読み取り・コピー先の書き込み権限、タグ操作には対応するS3タグ操作の権限が必要です。
RustFSの稼働バージョン・権限設定に対する実環境での確認は別途必要です。

## S3保存後のSlack通知

`POST /s3/put_object_and_notify` でBase64データを保存し、署名付きGET URLをSlackへ投稿できます。

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

`text` の既定値は `File uploaded`、URL有効期間の既定値は900秒です（1〜604800秒）。
有効期間はアップロード前の署名時点から始まります。
URLは `RUSTFS_S3_ENDPOINT` を使用するため、Slackの受信者がそのアドレスにアクセスできる必要があります。

レスポンスは `uploaded`・`object`（ETag・version ID）・`url`・`notified`・`notification_error` を返します。
保存成功後に通知だけが失敗した場合もHTTP 200で、`uploaded: true, notified: false` を返します。
保存済みのファイルは削除しません。再アップロードせず、返されたURLを使って `/slack/message` から通知だけを再試行してください。
URLが期限切れの場合は `/s3/presigned_get_object` で再発行できます。

保存に失敗した場合はSlackへ投稿しません。保存・通知の自動再試行は行いません。
この機能は専用エンドポイント経由の保存が対象です。既存のストリーミングアップロードや他クライアントからの保存に対するRustFS Webhook通知は含みません。
