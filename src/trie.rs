use std::collections::{HashMap, HashSet};

struct TrieNode {
  value: HashSet<String>,
  children: HashMap<String, Self>
}

impl TrieNode {
  fn insert_helper(&mut self, topic_levels: &[&str], value: &str) {
    if topic_levels.len() == 0 {
      self.value.insert(value.to_owned());
    } else {
      let node = self.children.entry(topic_levels[0].to_owned()).or_insert(TrieNode {
        value: HashSet::new(),
        children: HashMap::new()
      });

      node.insert_helper(&topic_levels[1..], value);
    }
  }

  fn find_helper(&self, topic_levels: &[&str], topic: String, mut map: &mut HashMap<String, HashSet<String>>) {
    if topic_levels.len() == 0 {
      map.insert(topic[1..].to_owned(), self.value.clone());
    } else {
      if let Some(node) = self.children.get("#") {
        map.insert(topic[1..].to_owned() + "/" + "#", node.value.clone());
      }

      match self.children.get(topic_levels[0]) {
        Some(node) => {
          node.find_helper(&topic_levels[1..], topic.to_owned() + "/" + topic_levels[0], &mut map);
        }
        None => ()
      }
    }
  }

  fn remove(&mut self, topic_levels: &[&str], value: &str) -> bool {
    if topic_levels.len() == 0 {
      self.value.remove(value);
    } else if let Some(node) = self.children.get_mut(topic_levels[0]) {
      if node.remove(&topic_levels[1..], value) {
        self.children.remove(topic_levels[0]);
      }
    }
    // if nothing is left remove this node
    self.value.is_empty() && self.children.is_empty()
  }
}

pub struct TopicTrie {
  root: TrieNode
}

impl TopicTrie {
  pub fn new() -> TopicTrie {
    TopicTrie {
      root: TrieNode {
        value: HashSet::new(),
        children: HashMap::new()
      }
    }
  }

  pub fn insert(&mut self, topic: &str, value: &str) {
    let topic_levels: Vec<&str> = topic.split("/").collect();
    self.root.insert_helper(&topic_levels, value);
  }

  pub fn get(&self, topic: &str) -> HashMap<String, HashSet<String>> {
    let topic_levels: Vec<&str> = topic.split("/").collect();
    let mut map = HashMap::new();
    self.root.find_helper(&topic_levels, "".to_owned(), &mut map);
    map
  }

  pub fn remove(&mut self, topic: &str, value: &str) {
    let topic_levels: Vec<&str> = topic.split("/").collect();
    self.root.remove(&topic_levels, value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_helper(values: &[&str]) -> HashSet<String> {
    let mut set = HashSet::new();
    for value in values {
      set.insert(value.to_owned().to_owned());
    }
    set
  }


  #[test]
  fn insert_find_and_remove() {
    let mut trie = TopicTrie::new();
    trie.insert("test/sub1/123", "node1");
    trie.insert("test/sub2", "node3");
    trie.insert("test/#", "node2");

    let mut map = HashMap::new();
    map.insert("test/sub1/123".to_owned(), test_helper(&["node1"]));
    map.insert("test/#".to_owned(), test_helper(&["node2"]));

    assert_eq!(trie.get("test/sub1/123"), map);

    trie.insert("test/sub2", "node2");
    map.insert("test/sub2".to_owned(), test_helper(&["node2", "node3"]));
    map.remove("test/sub1/123");
    assert_eq!(trie.get("test/sub2"), map);

    trie.insert("test/sub2/#", "node2");
    map.remove("test/sub2");
    map.insert("test/sub2/#".to_owned(), test_helper(&["node2"]));
    assert_eq!(trie.get("test/sub2/123"), map);
  }
}