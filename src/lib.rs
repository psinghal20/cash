use std::cmp::Ordering;
use std::hash::Hash;
use std::hash::{Hasher, BuildHasher};
use ahash::RandomState;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Node already present in the ring")]
    NodeAlreadyPresent,
    #[error("Node is not present in the ring")]
    NodeNotPresent,
}

pub struct HashRing<T: Hash + Eq + Ord> {
    nodes: Vec<Node<T>>,
    state: RandomState,
}

impl<T: Hash + Eq + Ord> HashRing<T> {
    pub fn new() -> Self {
        HashRing {
            nodes: vec![],
            state: RandomState::new()
        }
    }

    pub fn add_node(&mut self, node: T) -> Result<(), Error>{
        let mut hasher = self.state.build_hasher();
        node.hash(&mut hasher);
        let node_hash = hasher.finish();
        let node = Node {
            data: node,
            hash: node_hash,
        };
        let pos = match self.nodes.binary_search(&node) {
            Ok(_) => {
                return Err(Error::NodeAlreadyPresent);
            },
            Err(pos) => pos,
        };
        self.nodes.insert(pos, node);
        Ok(())
    }

    pub fn get_node<R: Hash + Eq + Ord>(&self, request: R) -> &Node<T> {
        let mut hasher = self.state.build_hasher();
        request.hash(&mut hasher);
        let req_hash = hasher.finish();
        let pos = match self.nodes.binary_search_by_key(&req_hash, |node| node.hash) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == self.nodes.len() {
                    0
                } else {
                    pos
                }
            }
        };
        &self.nodes[pos]
    }
}

#[derive(Debug, Eq)]
pub struct Node<T: Hash + Eq + Ord> {
    data: T,
    hash: u64,
}

impl<T: Hash + Eq + Ord> PartialEq for Node<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl<T: Hash + Eq + Ord> PartialOrd for Node<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.hash.cmp(&other.hash))
    }
}

impl<T: Hash + Eq + Ord> Ord for Node<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}