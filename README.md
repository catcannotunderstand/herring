# herring

Web サーバが複数の HTTP リクエストを同時にさばける仕組みを、Rust の小さなサーバ4種で実際に観察するためのリポジトリです。

- **サーバ**：同じルートを持つ4つの実装
- **計測**：サーバ内部のイベント、クライアントのイベント、外部からの `/proc` サンプリングを、1本の時間軸にそろえて記録
- **可視化**：[`docs/index.html`](docs/index.html)。リクエストごとのガントチャート、スレッドのレーン、資源のゲージを同じ時間軸に並べ、µs まで拡大できる

設計の考え方（何を、どの間隔で、どう見せるか）は [`docs/observation-design.md`](docs/observation-design.md) にまとめています。

## サーバ

| バイナリ | 方式 | 実在のサーバでいうと |
|---|---|---|
| `blocking` | ⓪ 1接続ずつ順番に処理（比較用） | — |
| `threaded` | ① 1接続 = 1スレッド | Apache prefork / worker |
| `event_loop` | ② epoll によるイベントループ（1スレッド、mio） | nginx のワーカー1つ、Node.js |
| `async_tokio` | ③ async/await（tokio、複数ワーカー） | nginx（複数ワーカー）、Rust の Web フレームワーク |

ルート:

- `/` すぐに返す
- `/wait?ms=N` N ミリ秒の I/O 待ち（DB 問い合わせの代わり。CPU は使わない）
- `/cpu?ms=N` N ミリ秒ぶんの CPU 計算

```sh
cargo run --release --bin event_loop            # http://127.0.0.1:8080
curl 'http://127.0.0.1:8080/wait?ms=500'
```

共通オプション: `--port 8080`、`--trace FILE.csv`（内部イベントを記録）、`--exit-after N`（N 接続を処理したら記録を書いて終了）、`--verbose`。`async_tokio` だけ `--workers N` も使えます。

## 観察する（Linux）

```sh
cargo run --release --bin observe -- all     # 全シナリオ × 全サーバを記録 → runs/
cargo run --release --bin report             # runs/ → docs/data.js
# docs/index.html をブラウザで開く
```

一部だけ記録し直す場合:

```sh
cargo run --release --bin observe -- list                                # シナリオ一覧
cargo run --release --bin observe -- cpu --servers event_loop,async_tokio
cargo run --release --bin observe -- io --interval-ms 5                  # サンプリング間隔を変える
```

| シナリオ | 内容 |
|---|---|
| `io` | 1秒の I/O 待ちがあるリクエストを5件同時に送る |
| `cpu` | 300ms の CPU 計算を1件送り、その最中に軽いリクエストを 50ms おきに4件送る |
| `scale` | 1秒の I/O 待ちを500件同時に送る（blocking は除外） |

`runs/` と `docs/data.js` には、Linux 6.18（x86_64、2 コア）で記録した結果が入っています。

## 記録したデータから分かったこと（一部）

| | io の完了時間 | cpu で軽いリクエストの平均応答 | scale のスレッド数 |
|---|---|---|---|
| blocking | 5.00 秒 | 約 180 ms | — |
| threaded | 1.00 秒 | 1 ms 未満 | 501 |
| event_loop | 1.00 秒 | 約 180 ms | 1 |
| async_tokio | 1.00 秒 | 1 ms 未満 | 3 |

- event_loop は1本のスレッドで、1回の epoll_wait から戻ったあと数十 µs のうちに5件を accept し、I/O 待ちを登録します。
- 500 スレッドでも RSS は 7.6 MB でした（VSZ は約 2 GB）。代わりに、CPU に載った回数（コンテキストスイッチの目安）は 1140 回で、event_loop（184 回）の約6倍でした。
- 500 件が一斉に来たとき、すべてを accept し終えるまでに、event_loop は最大 3 ms、async_tokio は 56 ms、threaded は 83 ms かかりました。threaded は accept のたびにスレッドを作るためです。その間の接続は、カーネルの accept 待ちキュー（backlog）が受け止めています。

## ファイル構成

```
src/
  lib.rs, trace.rs, http.rs, net.rs, args.rs, client.rs   共通部品
  bin/blocking.rs, threaded.rs, event_loop.rs, async_tokio.rs  サーバ
  bin/observe.rs   実験ランナー（クライアント + サンプラー）
  bin/report.rs    runs/ → docs/data.js
  bin/client.rs    手動で負荷をかけるクライアント
examples/trace_overhead.rs   計測点1回あたりの費用
docs/index.html, data.js     可視化ページ
docs/observation-design.md   観察の設計
runs/                        記録したデータ
```
