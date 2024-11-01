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

#[derive(Copy, Clone, Debug)]
enum NodeRole {
    Master,
    Replica(u16)                                    // u16 holds port address of its Master
}

#[derive(Clone)]
struct Node {
    metadata: NodeMetadata,
    cluster: Cluster
}

#[derive(Clone, Debug)]
struct NodeMetadata {
    // port: u16,
    bus_port: u16,
    role: NodeRole
}

impl NodeMetadata {
    fn new(bus_port: u16, role: NodeRole) -> Self {
        Self {
            bus_port,
            role
        }
    }
}

#[derive(Clone, Debug)]
struct Cluster {
    node_lookup: HashMap<u16, NodeMetadata>,                // maps port -> Node metadata
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

    fn add(&mut self, node: NodeMetadata) {
        let node_port = node.bus_port;
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
        bus_port:u16,
        role: NodeRole,
        cluster: Cluster
    ) -> Self {
        Self {
            metadata: NodeMetadata::new(bus_port, role),
            cluster
        }
    }
}

impl Server {
    fn new(
        node: Node
    ) -> Self {
        println!("Initializing server with bus_port: {}", node.metadata.bus_port);
        Self {
            inner: Arc::new(Mutex::new(node)),
        }
    }

    async fn start(self) {
        let bus_port = self.inner.lock().await.metadata.bus_port;
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
            println!("Starting ping service on port: {}", bus_port);
            loop {
                let mut node = ping_service.lock().await;
                let ports: Vec<u16> = node.cluster.node_lookup.keys().cloned().collect();

                for &port in &ports {
                    if port != bus_port {
                        node.cluster.set_pong_awaited(port);
                        println!("Sending PING to node {}", port);
                        Server::send_ping(bus_port, port).await;
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
                    let response_port = node.lock().await.metadata.bus_port;
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
                ["CLUSTERMEET", incoming_port] => {
                    let incoming_port: u16 = incoming_port.parse().unwrap();
                    let role = NodeRole::Master;

                    println!("CLUSTERMEET received from {} with role {:?}", incoming_port, role);

                    {
                        let mut cluster = node.lock().await.cluster.clone();
                        let new_node = NodeMetadata::new(incoming_port, role);
                        cluster.add(new_node);  
                        node.lock().await.cluster = cluster;
                        println!("Node {} added to the cluster", incoming_port);
                    }

                    let cluster_snapshot = node.lock().await.cluster.clone();
                    println!("cluster snapshot: {:?}", cluster_snapshot);
                    for &port in cluster_snapshot.node_lookup.keys() {
                        if port != incoming_port {
                            println!("Broadcasting cluster update to node {}", port);
                            Server::send_cluster_update(&node, port).await;
                        }
                    }
                }
                ["CLUSTERMEET", incoming_port, replica_of] => {
                    let incoming_port: u16 = incoming_port.parse().unwrap();
                    let role = NodeRole::Replica(replica_of.parse().unwrap());

                    println!("CLUSTERMEET received from {} with role {:?}", incoming_port, role);

                    {
                        let mut cluster = node.lock().await.cluster.clone();
                        let new_node = NodeMetadata::new(incoming_port, role);
                        cluster.add(new_node);  
                        node.lock().await.cluster = cluster;
                        println!("Node {} added to the cluster", incoming_port);
                    }

                    let cluster_snapshot = node.lock().await.cluster.clone();
                    println!("cluster snapshot: {:?}", cluster_snapshot);
                    for &port in cluster_snapshot.node_lookup.keys() {
                        if port != incoming_port {
                            println!("Broadcasting cluster update to node {}", port);
                            Server::send_cluster_update(&node, port).await;
                        }
                    }
                }
                ["CLUSTERUPDATE", cluster_info] => {
                    println!("Received CLUSTERUPDATE with info: {}", cluster_info);

                    let incoming_nodes: Vec<(u16, NodeRole)> = cluster_info.split(',')
                        .filter_map(|node_str| {
                            let parts: Vec<&str> = node_str.split(':').collect();
                            if parts.len() == 2 {
                                let bus_port = parts[0].parse::<u16>().ok()?;
                                let role = match parts[1] {
                                    "Master" => NodeRole::Master,
                                    "Replica" => NodeRole::Replica(bus_port),
                                    _ => return None,  // Skip if role is unrecognized
                                };
                                Some((bus_port, role))
                            } else {
                                None // Skip if format is incorrect
                            }
                        })
                        .collect();

                    let mut cluster = node.lock().await.cluster.clone();

                    for (bus_port, role) in incoming_nodes {
                        if !cluster.node_lookup.contains_key(&bus_port) {
                            println!("Adding new node from CLUSTERUPDATE: bus_port {} with role {:?}", bus_port, role);

                            // Create and add a new Node entry with the specified bus_port and role
                            let new_node = NodeMetadata::new(bus_port, role);
                            cluster.add(new_node);
                        }
                    }
                    node.lock().await.cluster = cluster;
                    println!("Local cluster updated after applying CLUSTERUPDATE diff");
                }
                _ => println!("Unknown message received: {}", message),
            };
        }
    }

    async fn send_cluster_update(node: &Arc<Mutex<Node>>, target_port: u16) {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", target_port)).await {
            // Prepare cluster state message
            let cluster_state = {
                let node = node.lock().await;
                let cluster_info = node.cluster.node_lookup.iter()
                    .map(|(&bus_port, node)| {
                        // Convert role to a string
                        let role_str = match node.role {
                            NodeRole::Master => "Master",
                            NodeRole::Replica(_) => "Replica",
                        };
                        format!("{}:{}", bus_port, role_str)
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                format!("CLUSTERUPDATE {}\n", cluster_info)
            };

            println!("Sending cluster update to {}: {}", target_port, cluster_state);
            if let Err(e) = stream.write_all(cluster_state.as_bytes()).await {
                eprintln!("Failed to send cluster update to {}: {}", target_port, e);
            } else {
                println!("Cluster update sent to {}", target_port);
            }
        } else {
            eprintln!("Failed to connect to node {} for cluster update", target_port);
        }
    }

    async fn send_ping(self_port: u16, target_port: u16) {
        println!("Attempting to send PING to target port: {}", target_port);
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", target_port)).await {
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
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <bus_port>", args[0]);
        std::process::exit(1);
    }

    let bus_port: u16 = args[1].parse().expect("Invalid bus port number");
    let cluster = Cluster::new();
    let node = Node::new(bus_port, NodeRole::Master, cluster);
    let server = Server::new(node);

    server.start().await;
    tokio::signal::ctrl_c().await?;

    Ok(())
}
