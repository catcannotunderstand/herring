//! listen ソケットの作成。
//!
//! std の TcpListener::bind と mio / tokio の bind では backlog（カーネルが accept 前の接続を
//! 溜めておける数）が違う。比べる条件を揃えるため、全サーバがこの関数で同じ設定のソケットを作る。

use socket2::{Domain, Socket, Type};
use std::net::{SocketAddr, TcpListener};

pub const BACKLOG: i32 = 1024;

pub fn listen(port: u16) -> std::io::Result<TcpListener> {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let s = Socket::new(Domain::IPV4, Type::STREAM, None)?;
    s.set_reuse_address(true)?; // 直前の実験の TIME_WAIT が残っていても bind できるように
    s.bind(&addr.into())?;
    s.listen(BACKLOG)?;
    Ok(s.into())
}
