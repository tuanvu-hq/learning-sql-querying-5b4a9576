#![allow(unused_variables)]

use learning_sql_querying_5b4a9576::progress::p_003::display_table;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;

    display_table().await?;

    Ok(())
}
