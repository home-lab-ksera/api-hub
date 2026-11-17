# 通知キュー

Slack への通知を DB（`api_hub`）に永続化してから配送する仕組みです。
HTTP API と 1 本の配送 task が同じプロセスで動き、上限 3 接続の pool を共有します。

## 受付

`POST /notifications` は `producer`・`event_id`・`channel`・`text` を保存して 202 を返します。202 は「受け付けた」の意味で、送信完了ではありません。

- 同じ `producer` / `event_id` で同じ内容なら、同じ ID を返します（冪等）。内容が違えば 409 です。
- 画像は DB に入れず、S3 の `pdfs/hitomi-notifications/<sha256>.jpg|png` への参照で受け取ります。2MiB 以下で、形式を検証します。
- 呼び出し側（hitomi など）は、有効な 202 を受け取った時点で役目が終わります。配送の状態を追いかけるのは api-hub です。

## 配送

| 状態 | 意味 |
|---|---|
| pending / retry | 送信待ち |
| sending | 送信中（lease を保持） |
| sent | Slack が受け付けた |
| failed | 再試行しても送れなかった |
| uncertain | 送れたかどうか分からない |

- 429 は `Retry-After` に従います。画像の準備（共有前）は再試行できます。
- 送信・共有を始めたあとの通信失敗、5xx、lease 切れは **uncertain** とし、自動では再送しません。
  Slack は一部のエラーで部分的に成功することがあるためです
  （[completeUploadExternal](https://docs.slack.dev/reference/methods/files.completeUploadExternal/)）。
- 配送 task は、キューが空のときや DB に接続できないとき、確認の間隔を 1 → 2 → 4 → 8 → 16 → 30 秒と延ばし、1 件処理すると 1 秒に戻します。
  空のキューに入った通知は、送信が始まるまで最大 30 秒ほど待ちます。

### uncertain と failed の扱い

1. `GET /notifications/{id}` で状態、識別子、Slack の `ts` や file ID を確認する。
2. Slack に実際に届いているかを見る。
3. 届いていなければ `POST /notifications/{id}/retry` で再試行する。

sending / uncertain を一括で pending に戻したり、全件を再送したりしません。

## 起動と終了

- `--migrate` は migration だけを実行して終了します。設定の読み込み、HTTP の受付、配送は始めません。
- 配送 task が予期せず終了（panic を含む）した場合は、API も止めてプロセスを異常終了します。
- SIGTERM を受けると、新しい受付と新しい claim を止め、進行中の HTTP を最大 80 秒、通知 1 件を最大 75 秒待ってから pool を閉じます。
  Pod の `terminationGracePeriodSeconds` は 90 秒以上にします。
- 強制終了や 75 秒の超過で結果が分からなくなった通知は、lease の回復と uncertain で扱います。
- 一時的な DB / S3 / Slack の障害は、再試行と監視で扱います。

## DB を復元したあと

DB が過去の時点に戻ると、その後に送った通知がもう一度送られることがあります。これは許容しています。
通常どおり起動すれば配送が再開します。Slack との事前の照合は要りません。

## メトリクス

`GET /metrics` は、通知キューの状態を返します（`queue="notifications"`）。

- `application_queue_jobs{queue,state}`: 状態ごとの件数。件数のない状態も 0 を返す
- `application_queue_oldest_pending_age_seconds{queue}`: pending / sending / retry のうち、いちばん古い受付からの秒数。なければ 0
- `application_queue_last_success_timestamp_seconds{queue}`: 最後に sent になった時刻。実績がなければ 0

SQL と pool の取得は合わせて 3 秒以内で、DB に接続できないときや schema がないときは 503 を返します（0 件としては返しません）。
通知の本文、channel、event ID、認証情報は含めません。

アラートの条件と発火時の対応は [home-lab-k8s の監視](https://github.com/home-lab-ksera/home-lab-k8s/blob/main/docs/observability.md#アプリのキュー) にあります。

## テスト

```bash
cargo test --locked
TEST_DATABASE_URL=postgres://.../isolated_test cargo test --locked --test notifications -- --ignored
```
