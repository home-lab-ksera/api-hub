# DBと通知配送

`--migrate`はapi_hub DBのSQLx migrationだけを実行する。通常起動（引数なし/`--serve`）はHTTP APIと1本の通知配送taskを同じprocessで動かす。DB設定は必須だが接続はlazyで、一時停止時もS3 APIの起動を妨げない。

本番接続はPGHOST/PGDATABASE/PGPASSWORD/PGSSLROOTCERT、PGUSER=app。verify-fullを強制し、poolはAPI/配送共有3、migration 1を配備側で設定する。
DATABASE_URLは隔離テスト用にも利用できる。

`POST /notifications` はproducer/event_id/channel/textを永続化して202を返す。同じ識別子と同じpayloadは同じIDとなり、payloadの変更は409。
任意の画像bytesをDBへ入れず、fileは `pdfs/hitomi-notifications/<sha256>.jpg|png` の参照を受ける。画像は2MiB以下、形式を検証し、Slackのfile IDを保存する。

429はRetry-Afterに従う。共有前の画像準備は再試行可能。送信・共有後の通信失敗、5xx、lease切れはuncertainとして自動再送しない。
Slackは一部のエラーで部分成功し得るため、[completeUploadExternal](https://docs.slack.dev/reference/methods/files.completeUploadExternal/) の結果不明を区別する。
`GET /notifications/{id}` で状態・識別子・ts/file IDを取得し、確認後に `POST /notifications/{id}/retry` でuncertain/failedだけを再試行する。

ブラウザ用は `RUSTFS_S3_BROWSER_ENDPOINT=https://s3.internal.qroksera.com`。
`POST /s3/browser_get_object` はpdfs/hitomiのPDF/previewとpdfs/videosのMP4だけに900秒のGET URLを発行する。既存S3操作用endpointは内部Serviceのまま。
既存の内部ネットワーク公開を前提とし、ユーザー単位の認証を追加したものではない。

```bash
cargo test --locked
TEST_DATABASE_URL=postgres://.../isolated_test cargo test --locked --test notifications -- --ignored
```

[Kubernetes切替手順](../../k8s_myHome/docs/application-postgresql-cutover.md)。

通常の終了要求では新しい通知をclaimせず、処理中の1件のSlack応答とDB確定を待つ。
DB待ちを含め1件の処理上限は75秒。Deploymentの`terminationGracePeriodSeconds`は90秒以上とする。
強制終了・75秒の上限超過・送信結果不明では、従来どおりlease回復と`uncertain`の照合を使う。

APIプロセスの`GET /metrics`は通知DBから次のgaugeを返す（queue=`notifications`）。
- `application_queue_jobs{queue,state}`: 状態ごとの件数。空の状態は0。
- `application_queue_oldest_pending_age_seconds{queue}`: pending/sending/retryの最古受付からの秒数。空なら0。
- `application_queue_last_success_timestamp_seconds{queue}`: 最後にsentとなった時刻。実績がなければ0。

SQLとpoolの取得は合わせて3秒以内。DB未設定・停止・schema未適用時は503を返し、ゼロ件として扱わない。
既存poolを共有し、資格情報・通知本文・channel・event IDはmetricsに含めない。

通知producerのhitomiは有効な202の受領で受付完了とし、accepted後に配送状態を継続GETしない。配送失敗・uncertainはこのapi-hubの監視と手動retryが管理する。hitomiに残る旧履歴のapi_idは移行時だけ一度identityを照合する。

## 起動・終了と復元

APIと1本の配送taskは上限3接続のPgPoolを共有する。migrationは設定読込・HTTP受付・配送を開始せず終了する。旧worker専用起動とAPI専用互換modeは廃止した。

一時的なDB/S3/Slack障害は既存workerの再試行と監視で扱う。workerの予期しない終了/panicではAPIも停止し、processを異常終了する。SIGTERMは新規受付・新規claimを止め、API最大80秒・通知最大75秒の進行分を待ち、最後に共有poolを閉じる。Pod猶予は90秒。

DB復元後の再送・重複を許容し、統合processの通常起動で配送を再開できる。Slackとの事前照合を再開条件にせず、復元目的のAPI専用modeや配送停止設定を持たない。通常の冪等受付とuncertain手動対応は維持する。

旧imageへの切戻しでは統合processを先に停止し、API専用へ戻した後に旧workerを再開する。逆順で二重workerを作らず、sending/uncertainを一括pendingへ戻さない。
