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
use std::collections::Hashmap;

enum NodeRole {
    Master,
    Replica(u16)                                    // u16 holds port address of its Master
}

struct Node {
    port: u16,
    role: NodeRole,
    cluster: Cluster
}

struct Cluster {
    node_lookup: Hashmap<u16, Node>,                // maps port -> Node metadata
    replica_lookup: Hashmap<u16, Vec<u16>>          // maps Master port -> collection of its Replica ports
}

struct Server {
    inner: Arc<Mutex<Node>>
}

impl Cluster {
    fn new() -> Self {
        Self {
            node_lookup: HashMap::new(),
            replica_lookup: HashMap::new()
        }
    }

    fn add(&mut self, node: Node) {
        self.node_lookup.insert(node.port, node);
        if let NodeRole::Replica(master_port) = node.role {
            self.replica_lookup
                .entry(master_port)
                .or_insert_with(Vec::new())
                .push(node.port);
        }
    }
}

#[tokio(main)]
async fn main() {

}