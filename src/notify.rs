// TODO: network::socket::Socket needs to be reimplemented.

use bitcoin::network::constants::Network;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::InvType;
use bitcoin::network::socket::Socket;
use bitcoin::hash_types::Txid;
use bitcoin::util::Error;

use crate::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use crate::util;

fn connect() -> Result<Socket, Error> {
    let mut sock = Socket::new(Network::Bitcoin);
    sock.connect("127.0.0.1", 8333)?;
    Ok(sock)
}

fn handle(mut sock: Socket, tx: Sender<Txid>) {
    let mut outgoing = vec![sock.version_message(0).unwrap()];
    loop {
        for msg in outgoing.split_off(0) {
            trace!("send {:?}", msg);
            if let Err(e) = sock.send_message(msg.clone()) {
                warn!("failed to connect to node: {}", e);
                break;
            }
        }
        // Receive new message
        let msg = match sock.receive_message() {
            Ok(msg) => msg,
            Err(e) => {
                warn!("failed to receive p2p message: {}", e);
                break;
            }
        };
        trace!("recv {:?}", msg);
        match msg {
            NetworkMessage::Alert(_) => continue, // deprecated
            NetworkMessage::Version(_) => outgoing.push(NetworkMessage::Verack),
            NetworkMessage::Ping(nonce) => outgoing.push(NetworkMessage::Pong(nonce)),
            NetworkMessage::Inv(ref inventory) => {
                inventory
                    .iter()
                    .filter(|inv| inv.inv_type == InvType::Block)
                    .for_each(|inv| tx.send(inv.hash).expect("failed to send message"));
            }
            _ => (),
        };
    }
}

pub fn run() -> util::Channel<Txid> {
    let chan = util::Channel::new();
    let tx = chan.sender();

    util::spawn_thread("p2p", move || loop {
        // TODO: support testnet and regtest as well.
        match connect() {
            Ok(sock) => handle(sock, tx.clone()),
            Err(e) => warn!("p2p error: {}", e),
        }
        thread::sleep(Duration::from_secs(3));
    });

    chan
}
