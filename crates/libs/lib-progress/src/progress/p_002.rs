use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database::orders;

use crate::model::df_orders::df_orders;
use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT * FROM orders;
*/

/*
shape: (4, 4)
┌──────────┬─────────────┬────────────┬───────┐
│ order_id ┆ customer_id ┆ order_date ┆ sales │
│ ---      ┆ ---         ┆ ---        ┆ ---   │
│ i32      ┆ i32         ┆ date       ┆ i32   │
╞══════════╪═════════════╪════════════╪═══════╡
│ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
│ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
│ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
│ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
└──────────┴─────────────┴────────────┴───────┘
*/

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<orders::Model>> {
    orders::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<orders::Model>> {
    let query = "SELECT * FROM orders;";

    sqlx::query_as::<_, orders::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df = df_orders(&db_sea_orm)
        .await?
        .clone()
        .lazy()
        .collect()
        .map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
