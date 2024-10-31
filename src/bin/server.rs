// dynamically add nodes to cluster, either as Master or Replica of a Master:
    // 1. start server
    // 2. existing server in cluster runs a command "CLUSTER MEET <new ip> <new port>"
    // 3. if it is a Master server, rehash slots, and move the data around 

// create a Replica by running "CLUSTER REPLICATE <master node>"

// client with proper authentication (admin client) can run these "CLUSTER" commands from cli for handling this, 
// lets say with each message from an authenticated client there is an attached encrypted password or something

// gossip protocol between nodes. each node will check a subset of nodes in the cluster, and update the nodes its pinging
// with the information of the other nodes its checked on

// Message Types:
    // PING: a node sends a PING message to another node to check its health. 
        // the PING message also contains the current state information about the sending node and other nodes it knows about.
    // PONG: The receiving node responds with a PONG message to acknowledge that it is alive. 
        // the PONG message contains the recipient node’s state and the state information it has gathered.
    // FAIL: when a node determines that another node has failed, it sends a FAIL message to inform the rest of the cluster about this failure.

// if a master node does not respond to PING messages from other nodes within the configured timeout (cluster-node-timeout), it is flagged as being in a suspected failure state.
// the suspected failure is then confirmed by a quorum (majority) of master nodes. 
// once a quorum agrees that a node is unresponsive, it is marked as failed, and a FAIL message is propagated throughout the cluster.
// the replica that meets the promotion criteria (based on replication offset and priority) broadcasts a request for votes to the other master nodes.
// each master node in the cluster evaluates the request and votes in favor of the candidate replica if it meets the requirements.
// a replica must receive votes from a majority of the cluster’s master nodes to be promoted.
// the replica changes its role from a slave to a master.
// the other nodes in the cluster are informed of this change via the gossip protocol. The newly promoted master takes ownership of the slots previously managed by the failed master.
// other nodes update their routing tables to reflect the new configuration.
use std::collections::HashMap;
use tokio::net::TcpStream;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::Duration;

#[derive(Copy, Clone)]
enum NodeRole {
    Master,
    Replica(u16)                                    // u16 holds port address of its Master
}

#[derive(Clone)]
struct Node {
    port: u16,
    bus_port:u16,
    role: NodeRole,
    cluster: Cluster
}

#[derive(Clone)]
struct Cluster {
    node_lookup: HashMap<u16, Node>,                // maps port -> Node metadata
    replica_lookup: HashMap<u16, Vec<u16>>,         // maps Master port -> collection of its Replica ports
    is_alive_lookup: HashMap<u16, u8>,              // maps port in cluster -> missed PONG count
    pong_awaited: HashMap<u16, bool>,
}

struct Server {
    inner: Arc<Mutex<Node>>
}

impl Cluster {
    fn new() -> Self {
        Self {
            node_lookup: HashMap::new(),
            replica_lookup: HashMap::new(),
            is_alive_lookup: HashMap::new(),
            pong_awaited: HashMap::new()
        }
    }

    fn mark_alive(&mut self, port: u16) {
        self.is_alive_lookup.insert(port, 0); 
        self.pong_awaited.insert(port, false);
    }

    fn increment_missed_pongs(&mut self, port: u16) -> bool {
        if *self.pong_awaited.get(&port).unwrap_or(&false) {
            let missed_count = self.is_alive_lookup.entry(port).or_insert(0);
            *missed_count += 1;
            println!("Node {} missed PONG response. Missed count: {}", port, missed_count);
            *missed_count >= 5 
        } else {
            false
        }
    }

    fn set_pong_awaited(&mut self, port: u16) {
        self.pong_awaited.insert(port, true);
    }

    fn add(&mut self, node: Node) {
        let node_port = node.port;
        let node_role = node.role;
        self.node_lookup.insert(node_port, node);
        if let NodeRole::Replica(master_port) = node_role {
            self.replica_lookup
                .entry(master_port)
                .or_insert_with(|| Vec::new())
                .push(node_port);
        }
    }
}

impl Node {
    fn new(
        port: u16,
        bus_port:u16,
        role: NodeRole,
        cluster: Cluster
    ) -> Self {
        Self {
            port,
            bus_port,
            role,
            cluster
        }
    }
}

impl Server {
    fn new(
        port: u16,
        bus_port: u16,
        role: NodeRole,
        cluster: Cluster,
    ) -> Self {
        println!("Initializing server with port: {} and bus_port: {}", port, bus_port);
        Self {
            inner: Arc::new(Mutex::new(Node::new(port, bus_port, role, cluster))),
        }
    }

    async fn start(self) {
        let self_port = self.inner.lock().await.port;
        let bus_port = self.inner.lock().await.bus_port;
        let listener = TcpListener::bind(("127.0.0.1", bus_port)).await.unwrap();
        println!("Server starting on bus_port: {}", bus_port);

        let gossip_handler = self.inner.clone();
        tokio::spawn(async move {
            println!("Starting gossip handler loop on bus_port: {}", bus_port);
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                println!("Accepted connection on bus_port: {}", bus_port);
                let gossip_handler_clone = gossip_handler.clone();
                
                tokio::spawn(async move {
                    println!("Spawning new task to handle gossip message.");
                    Server::handle_gossip(socket, gossip_handler_clone).await;
                });
            }
        });

        // Ping service
        let ping_service = self.inner.clone();
        tokio::spawn(async move {
            println!("Starting ping service on port: {}", self_port);
            loop {
                let mut node = ping_service.lock().await;
                let ports: Vec<u16> = node.cluster.node_lookup.keys().cloned().collect();

                for &port in &ports {
                    if port != self_port {
                        node.cluster.set_pong_awaited(port);
                        println!("Sending PING to node {}", port);
                        Server::send_ping(&ping_service, port).await;
                    }
                }

                tokio::time::sleep(Duration::from_secs(2)).await;

                for &port in &ports {
                    if node.cluster.increment_missed_pongs(port) {
                        eprintln!("Node {} is considered dead", port);
                    } else {
                        println!("Node {} is still alive (missed count reset)", port);
                    }
                }
            }
        });

        // Keep the main task alive
        loop {
            println!("Server running on bus_port: {}", bus_port);
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    async fn handle_gossip(mut socket: TcpStream, node: Arc<Mutex<Node>>) {
        let (reader, _) = socket.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            println!("Waiting to read message from socket...");
            line.clear();
            let bytes_read = reader.read_line(&mut line).await.unwrap();
            println!("bytes_read: {}",bytes_read);
            if bytes_read == 0 {
                println!("Connection closed by peer.");
                break;
            }

            let message = line.trim();
            println!("Received message: {}", message);

            match message.split_whitespace().collect::<Vec<&str>>().as_slice() {
                ["PING", source_port] => {
                    let response_port = node.lock().await.port;
                    println!("Received PING from {}. Responding with PONG {}", source_port, response_port);
                    let source_port_as_int = source_port.parse::<u16>().unwrap();
                    if let Ok(mut pong_stream) = TcpStream::connect(("127.0.0.1", source_port_as_int)).await {
                        let pong_message = format!("PONG {}\n", response_port);
                        println!("Sending PONG message: {}", pong_message);
                        if let Err(e) = pong_stream.write_all(pong_message.as_bytes()).await {
                            eprintln!("Failed to send PONG to {}: {}", source_port, e);
                        } else {
                            println!("Sent PONG to {}", source_port);
                        }
                        pong_stream.flush().await.unwrap();
                    } else {
                        eprintln!("Failed to connect to source_port {} to send PONG", source_port);
                    }
                }
                ["PONG", source_port] => {
                    let source_port: u16 = source_port.parse().unwrap();
                    node.lock().await.cluster.mark_alive(source_port);
                    println!("Received PONG from {}", source_port);
                }
                _ => println!("Unknown message received: {}", message),
            };
        }
    }

    async fn send_ping(server: &Arc<Mutex<Node>>, target_port: u16) {
        println!("Attempting to send PING to target port: {}", target_port);
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", target_port)).await {
            let self_port = server.lock().await.port;
            let message = format!("PING {}\n", self_port);
            println!("Sending PING message: {}", message);
            if let Err(e) = stream.write_all(message.as_bytes()).await {
                eprintln!("Failed to send PING to {}: {}", target_port, e);
            } else {
                println!("Successfully sent PING to {}", target_port);
            }
            stream.flush().await.unwrap();
        } else {
            eprintln!("Failed to connect to target port {} to send PING", target_port);
        }
    }
}

    
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!("Usage: {} <port> <bus_port>", args[0]);
        std::process::exit(1);
    }

    let port: u16 = args[1].parse().expect("Invalid port number");
    let bus_port: u16 = args[2].parse().expect("Invalid bus port number");
    let other_port: u16 = args[3].parse().expect("Invalid other node port number");
    let other_bus_port: u16 = args[4].parse().expect("Invalid other node bus port number");

    let mut cluster = Cluster {
        node_lookup: HashMap::new(),
        replica_lookup: HashMap::new(),
        is_alive_lookup: HashMap::new(),
        pong_awaited: HashMap::new(),
    };

    let other_node = Node {
        port: other_port,
        bus_port: other_bus_port,
        role: NodeRole::Master, 
        cluster: cluster.clone(),
    };
    cluster.node_lookup.insert(other_bus_port, other_node.clone());
    cluster.is_alive_lookup.insert(other_bus_port, 0);

    let node = Node {
        port,
        bus_port,
        role: NodeRole::Master, 
        cluster
    };

    let server = Server {
        inner: Arc::new(Mutex::new(node)),
    };

    server.start().await;
}
