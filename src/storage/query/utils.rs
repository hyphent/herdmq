pub fn get_prefix<'a>(topic_name: &'a str) -> &'a str {
  topic_name.split("/").next().unwrap_or(topic_name)
}
