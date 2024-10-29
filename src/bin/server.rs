use tokio::net::{
    TcpListener, 
    TcpStream
};
use tokio::io::{
    AsyncReadExt, 
    AsyncWriteExt, 
    self, 
    BufReader, 
    AsyncBufReadExt
};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use bytes::Bytes;
use std::collections::HashMap;

use crystal_methamphetamine::{
    crc16, 
    cluster
};

#[derive(Clone)]
struct Node {
    client_port: u16,
    bus_port: u16,
    peers: Vec<String>, 
    data: HashMap<String, Bytes>,
    start_slot: u16,
    end_slot: u16,
    cluster: cluster::Cluster,
}

#[derive(Clone)]
struct ArcMutexNode {
    inner: Arc<Mutex<Node>>,
}

impl ArcMutexNode {
    pub async fn new(client_port: u16, bus_port: u16, peers: Vec<String>, start_slot: u16, end_slot: u16, cluster: cluster::Cluster) -> Self {
        let node = Node {
            client_port,
            bus_port,
            peers,
            data: HashMap::new(),
            start_slot,
            end_slot,
            cluster
        };

        println!("slot bounds: {start_slot}-{end_slot}");

        Self {
            inner: Arc::new(Mutex::new(node))
        }
    }

    async fn sync_data_with_replicas(&self, key: &str, value: &Bytes) {
        let node = self.inner.lock().await;
        let replicas = node
            .cluster
            .nodes
            .iter()
            .filter(|&n| n.replica_of == Some(node.client_port));

        for replica in replicas {
            if let Err(e) = self.forward_command(replica.addr, format!("set {} {}", key, String::from_utf8_lossy(value))).await {
                eprintln!("ERR forward to sync with replica {}: {e}", replica.addr);
            }
        }
    }

    async fn owns_slot(&self, slot: u16) -> bool{
        self.inner.lock().await.start_slot <= slot && slot <= self.inner.lock().await.end_slot
    }
    
    pub async fn start(self) {
        let client_port = self.inner.lock().await.client_port;
        let bus_port = self.inner.lock().await.bus_port;

        let check_alive_daemon = self.clone();
        tokio::spawn(async move {
            check_alive_daemon
                .check_heartbeats()
                .await
        });

        let client_node = self.clone();
        tokio::spawn(async move {
            if let Ok(client_listener) = TcpListener::bind(format!("127.0.0.1:{}", client_port)).await {
                println!("listening for clients on port {0}", client_port);
                while let Ok((socket, _)) = client_listener.accept().await {
                    let client_node = client_node.clone();
                    tokio::spawn(async move {
                        client_node.handle_client_connection(socket).await;
                    });
                }
            }
        });

        let bus_node = self.clone();
        tokio::spawn(async move {
            if let Ok(bus_listener) = TcpListener::bind(format!("127.0.0.1:{}", bus_port)).await {
                println!("listening to other nodes on port {0}", bus_port);
                while let Ok((socket, _)) = bus_listener.accept().await {
                    let bus_node = bus_node.clone();
                    tokio::spawn(async move {
                        bus_node.handle_bus_connection(socket).await;
                    });
                }
            }
        });
    }

    pub async fn check_heartbeats(&self) {
        let peers = self.inner.lock().await.peers.clone();
        
        loop {
            for peer in &peers {
                let peer_addr = format!("127.0.0.1:{}", peer);
                let is_alive = check_peer_status(&peer_addr).await;
                println!("status of peer {}: {}", peer, if is_alive { "alive" } else { "not alive" });
            }

            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn handle_client_connection(&self, mut socket: TcpStream) {
        let mut buffer = [0; 1024];
    
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) => break, 
                Ok(n) => {
                    let request = String::from_utf8_lossy(&buffer[..n]);
                    let response = self.parse_and_execute_command(&request).await;
    
                    if let Err(e) = socket.write_all(response.as_bytes()).await {
                        eprintln!("scope of error: handle_client_connection() :: socket.write_all() :: {e}");
                        break;
                    }
                    if let Err(e) = socket.flush().await {
                        eprintln!("scope of error: handle_client_connection() :: socket.flush() :: {e}");
                        break;
                    }

                    println!("sent response");
                }
                Err(e) => {
                    eprintln!("scope of error: handle_client_connection() :: socket.read() :: {e}");
                    break;
                }
            }
        }
    }

    async fn parse_and_execute_command(&self, command: &str) -> String {
        let mut parts = command.split_whitespace();

        match parts.next() {
            Some("set") => {
                if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                    let slot = crc16::crc16_1021(key.as_bytes());
                    let value_bytes = Bytes::copy_from_slice(value.as_bytes());
                    println!("key: {:?}, slot: {}", key, slot);

                    if self.owns_slot(slot).await {
                        println!("node owns slot, locked the node");
                        self
                            .inner.lock().await
                            .data.insert(
                                key.to_string(), 
                                value_bytes.clone()
                            );
                        println!("inserted data");
                        self.sync_data_with_replicas(key, &value_bytes).await;
                        println!("synced data with replicas");
                        "OK\n".to_string()
                    } else {
                        println!("node doesnt own slot");
                        self.forward_or_error(slot, command.to_string()).await
                    }
                } else {
                    "ERR wrong number of arguments for 'set' command\n".to_string()
                }
            }
            Some("get") => {
                if let Some(key) = parts.next() {
                    let slot = crc16::crc16_1021(&key.as_bytes());

                    if let Some(value) = self.get_or_forward(slot, key).await {
                        value
                    } else {
                        "(nil)\n".to_string() // if (nil) is set as value by user for any key, it shows potential point of failure, need to develop a better protocol
                    }
                } else {
                    "ERR wrong number of arguments for 'get' command\n".to_string()
                }
            }
            _ => "ERR unknown command\n".to_string()
        }
    }

    async fn forward_or_error(&self, slot: u16, command: String) -> String {
        if let Some(responsible_node) = self.inner.lock().await.cluster.find_responsible_node(slot) {
            match self.forward_command(responsible_node.addr, command).await {
                Ok(response) => response,
                Err(e) => format!("ERR forwarding error: {e}\n"),
            }
        } else {
            "ERR no node owns this slot\n".to_string()
        }
    }

    async fn get_or_forward(&self, slot: u16, key: &str) -> Option<String> {
        if self.owns_slot(slot).await {
            let value = self.inner.lock().await.data.get(key).map(|v| String::from_utf8_lossy(v).to_string());
            value.map(|v| format!("{v}\n"))
        } else {
            self.forward_or_error(slot, format!("get {key}")).await.into()
        }
    }

    async fn forward_command(&self, node_addr: u16, command: String) -> tokio::io::Result<String> {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", node_addr)).await?;
        stream.write_all(command.as_bytes()).await?;
        stream.flush().await?;

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).await?;
        Ok(response)
    }
    
    async fn handle_bus_connection(self, mut socket: TcpStream) {}
}

async fn check_peer_status(peer: &str) -> bool {
    TcpStream::connect(peer).await.is_ok()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = std::env::args().collect();

    // todo: better validation of args
    if args.len() < 2 {
        eprintln!("usage: {} <node_addr> <peer1> <peer2> ...", args[0]);
        return Ok(());
    }

    // let node_addr = args[1].clone().parse::<u16>().unwrap();
    // let bus_addr = node_addr + 10000;
    // let peers: Vec<String> = args[2..].to_vec();

    // let cluster_size = peers.len() + 1;
    // let mut cluster = Cluster::new(cluster_size);
    // cluster.build(peers.clone(), node_addr);

    // let node_metadata = cluster.get_slot_bounds_of_node(node_addr).unwrap();

    // let node = ArcMutexNode::new(node_addr, bus_addr, peers, node_metadata.start_slot, node_metadata.end_slot, cluster);
    // node.await.start().await;

    // tokio::signal::ctrl_c().await?;

    Ok(())
}