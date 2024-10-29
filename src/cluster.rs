const TOTAL_SLOTS: u16 = 16384;

#[derive(Clone)]
pub struct NodeMetadata {
    pub addr: u16,
    pub start_slot: u16,
    pub end_slot: u16,
    pub role: NodeRole,            
    pub replica_of: Option<u16>, 
    pub is_alive: bool,
}

#[derive(Clone, PartialEq, Debug)]
enum NodeRole {
    Master,
    Replica,
}

#[derive(Clone)]
pub struct Cluster {
    pub nodes: Vec<NodeMetadata>,
    pub size: u16
}

impl Cluster {
    pub fn new(size: usize) -> Self {
        Cluster {
            nodes: Vec::with_capacity(size),
            size: size.try_into().unwrap()
        }
    }

    pub fn build(&mut self, nodes: Vec<NodeMetadata>) {
        self.nodes = nodes;

        let mut master_nodes: Vec<&mut NodeMetadata> = self
            .nodes
            .iter_mut()
            .filter(|node| node.role == NodeRole::Master)
            .collect();

        let master_count: u16 = master_nodes.len().try_into().unwrap();
        let slots_per_master = TOTAL_SLOTS / master_count;
        let extra_slots = TOTAL_SLOTS % master_count;

        let mut start_slot = 0;
        for (i, master) in master_nodes.iter_mut().enumerate() {
            let end_slot = start_slot + slots_per_master - 1 + (i < extra_slots as usize) as u16;

            master.start_slot = start_slot;
            master.end_slot = end_slot;

            start_slot = end_slot + 1;
        }

        let master_copies = self.nodes.clone();

        for node in &mut self.nodes {
            if let NodeRole::Replica = node.role {
                if let Some(master_addr) = node.replica_of {
                    if let Some(master) = master_copies.iter().find(|n| n.addr == master_addr) {
                        node.start_slot = master.start_slot;
                        node.end_slot = master.end_slot;
                    }
                }
            }
        }
    }

    pub fn get_slot_bounds_of_node(&self, node_addr: u16) -> Option<NodeMetadata> {
        self.nodes.iter().find(|&node| node.addr == node_addr).cloned()
    }

    pub fn find_responsible_node(&self, slot: u16) -> Option<&NodeMetadata> {
        self.nodes.iter().find(|node| node.start_slot <= slot && slot <= node.end_slot)
    }
}