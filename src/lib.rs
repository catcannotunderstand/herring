//! herring: Web サーバが複数のリクエストを同時にさばく仕組みを観察するための小さなサーバ群と計測道具。
//!
//! - `trace`  : サーバ内部のイベントをナノ秒のタイムスタンプ付きでメモリに記録し、終了時に CSV に書く
//! - `http`   : 最小限の HTTP 解析とルーティング（`/`、`/wait?ms=N`、`/cpu?ms=N`）
//! - `net`    : 全サーバ共通の listen ソケット作成（backlog を揃えるため）
//! - `client` : 計画どおりの時刻にリクエストを投げ、クライアント側のイベントを記録する
//! - `args`   : サーバ共通のコマンドライン引数

pub mod args;
pub mod client;
pub mod http;
pub mod net;
pub mod trace;
