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
  - body: なし（S3オブジェクトをプロキシ配信）
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

## テスト

```bash
cargo test --all-targets
```

## CI

GitHub Actionsで `cargo test --all-targets` を実行します。
