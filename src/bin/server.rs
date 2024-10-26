use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
struct Node {
    client_port: u16,
    bus_port: u16,
    peers: Vec<String>, 
}

#[derive(Clone)]
struct ArcMutexNode {
    inner: Arc<Mutex<Node>>,
}

impl ArcMutexNode {
    pub async fn new(client_port: u16, bus_port: u16, peers: Vec<String>) -> Self {
        let node = Node {
            client_port,
            bus_port,
            peers,
        };

        ArcMutexNode {
            inner: Arc::new(Mutex::new(node))
        }
    }

    pub async fn start(self) {
        let client_port = self.inner.lock().await.client_port;
        let bus_port = self.inner.lock().await.bus_port;
        let client_listener = TcpListener::bind(format!("127.0.0.1:{}", client_port)).await.unwrap();
        let bus_listener = TcpListener::bind(format!("127.0.0.1:{}", bus_port)).await.unwrap();

        let node = self.clone();

        node.monitor_peers().await;

        let client_node = self.clone();
        tokio::spawn(async move {
            println!("listening for clients on port {0}", client_port);
            loop {
                match client_listener.accept().await {
                    Ok((socket, _)) => {
                        let client_node = client_node.clone();
                        tokio::spawn(async move {
                            client_node.handle_client_connection(socket).await;
                        });
                    }
                    Err(e) => eprintln!("scope of error: start :: client_listener :: accept :: {e}") 
                }
            }
        });

        let bus_node = self.clone();
        tokio::spawn(async move {
            println!("listening to other nodes on port {0}", bus_port);
            loop {
                match bus_listener.accept().await {
                    Ok((socket, _)) => {
                        let bus_node = bus_node.clone();
                        tokio::spawn(async move {
                            bus_node.handle_bus_connection(socket).await;
                        });
                    }
                    Err(e) => eprintln!("scope of error: start :: bus_listener :: accept :: {e}") //failed to accept connection
                }
            }
        });
    }

    pub async fn monitor_peers(&self) {
        let peers = self.inner.lock().await.peers.clone();
        
        tokio::spawn(async move {
            loop {
                for peer in &peers {
                    let peer_addr = "127.0.0.1:".to_owned() + peer;
                    let is_alive = check_peer_status(&peer_addr).await;
                    println!("status of peer {}: {}", peer, if is_alive { "alive" } else { "not alive" });
                }

                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    async fn handle_client_connection(&self, mut socket: TcpStream) {
        let mut buffer = [0; 1024];
    
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) => break, 
                Ok(n) => {
                    let request = String::from_utf8_lossy(&buffer[..n]);
                    let response = "OK\n";
    
                    if let Err(e) = socket.write_all(response.as_bytes()).await {
                        eprintln!("scope of error: handle_client_connection() :: socket.write_all() :: {e}");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("scope of error: handle_client_connection() :: socket.read() :: {e}");
                    break;
                }
            }
        }
    }
    
    async fn handle_bus_connection(self, mut socket: TcpStream) {
        let mut buffer = [0; 1024];
    
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) => break, // connection closed by client
                Ok(n) => {
                    let message = String::from_utf8_lossy(&buffer[..n]);
                    
                    if message.ends_with("ALIVE") { 
                        if let Some((port, message_type)) = parse_alive_message(&message) {
                            println!("node {port} state: ALIVE");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("scope of error: handle_client_connection() :: socket.read() :: {e}");
                    break;
                }
            }
        }
    }
}

fn parse_alive_message(message: &str) -> Option<(u16, String)> {
    let parts: Vec<&str> = message.trim().split("::").collect();
    if parts.len() == 2 {
        if let Ok(port) = parts[0].parse::<u16>() {
            let message_type = parts[1].to_string();
            return Some((port, message_type));
        }
    }
    None
}

async fn check_peer_status(peer: &str) -> bool {
    match TcpStream::connect(peer).await {
        Ok(_) => {
            true
        }
        Err(e) => {
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = std::env::args().collect();

    // todo: better validation of args
    if args.len() < 2 {
        eprintln!("usage: {} <node_addr> <peer1> <peer2> ...", args[0]);
        return Ok(());
    }

    let node_addr = args[1].clone().parse::<u16>().unwrap();
    let bus_addr = node_addr + 10000;
    let peers: Vec<String> = args[2..].to_vec();

    // todo: run node on node_addr
    let node = ArcMutexNode::new(node_addr, bus_addr, peers);
    node.await.start().await;

    tokio::signal::ctrl_c().await?;

    Ok(())
}