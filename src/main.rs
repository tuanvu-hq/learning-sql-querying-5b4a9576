#![allow(unused_variables)]

use learning_sqlx_sea_orm_5b4a9576::{
    common::connection::{get_db_sea_orm, get_db_sqlx},
    progress::p_001::{display_as_polars_table, sea_orm_query, sqlx_query},
};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;

    let db_sea_orm = get_db_sea_orm().await?;
    let db_sqlx = get_db_sqlx().await?;

    let data = sea_orm_query(&db_sea_orm).await?;
    display_as_polars_table(&data)?;

    let data = sqlx_query(&db_sqlx).await?;
    display_as_polars_table(&data)?;

    Ok(())
}
