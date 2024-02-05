pub trait Insertable {
    const INSERT_QUERY: &'static str;
    fn value(&self) -> String;
    fn remove_duplicates(_v: &mut Vec<String>) {}
}
