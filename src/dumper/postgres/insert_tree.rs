use std::collections::HashMap;

use anyhow::Result;

use super::Insertable;

type InsertFn<'a> = Box<dyn Fn(&mut Vec<String>) + Send + 'a>;
#[derive(Default)]
pub struct InsertTree<'a>(HashMap<&'static str, (Vec<String>, InsertFn<'a>)>);

impl<'a> InsertTree<'a> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<T: Insertable + 'a>(&mut self, v: &T) {
        self.0
            .entry(T::INSERT_QUERY)
            .or_insert((vec![], Box::new(T::remove_duplicates)))
            .0
            .push(T::value(v));
    }

    pub async fn execute(self, transaction: &'a tokio_postgres::Transaction<'a>) -> Result<()> {
        for (query, (mut values, remove_dup)) in self.0.into_iter() {
            remove_dup(&mut values);
            let final_query = query.replace("{values}", &values.join(","));
            transaction.execute(final_query.as_str(), &[]).await?;
        }
        Ok(())
    }
}
