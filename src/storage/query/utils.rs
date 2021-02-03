pub fn get_prefix<'a>(topic: &'a str) -> &'a str {
  topic.split("/").next().unwrap_or(topic)
}
