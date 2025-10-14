use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::orders;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_orders;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM sales.orders;
*/

/*
shape: (10, 12)
┌─────────┬───────────┬────────────┬───────────────┬───┬───────────────┬──────────┬───────┬─────────────────────┐
│ orderid ┆ productid ┆ customerid ┆ salespersonid ┆ … ┆ billaddress   ┆ quantity ┆ sales ┆ creationtime        │
│ ---     ┆ ---       ┆ ---        ┆ ---           ┆   ┆ ---           ┆ ---      ┆ ---   ┆ ---                 │
│ i32     ┆ i32       ┆ i32        ┆ i32           ┆   ┆ str           ┆ i32      ┆ i32   ┆ datetime[ms]        │
╞═════════╪═══════════╪════════════╪═══════════════╪═══╪═══════════════╪══════════╪═══════╪═════════════════════╡
│ 1       ┆ 101       ┆ 2          ┆ 3             ┆ … ┆ 1226 Shoe St. ┆ 1        ┆ 10    ┆ 2025-01-01 12:34:56 │
│ 2       ┆ 102       ┆ 3          ┆ 3             ┆ … ┆ null          ┆ 1        ┆ 15    ┆ 2025-01-05 23:22:04 │
│ 3       ┆ 101       ┆ 1          ┆ 5             ┆ … ┆ 8157 W. Book  ┆ 2        ┆ 20    ┆ 2025-01-10 18:24:08 │
│ 4       ┆ 105       ┆ 1          ┆ 3             ┆ … ┆               ┆ 2        ┆ 60    ┆ 2025-01-20 05:50:33 │
│ 5       ┆ 104       ┆ 2          ┆ 5             ┆ … ┆ null          ┆ 1        ┆ 25    ┆ 2025-02-01 14:02:41 │
│ 6       ┆ 104       ┆ 3          ┆ 5             ┆ … ┆ null          ┆ 2        ┆ 50    ┆ 2025-02-06 15:34:57 │
│ 7       ┆ 102       ┆ 1          ┆ 1             ┆ … ┆               ┆ 2        ┆ 30    ┆ 2025-02-16 06:22:01 │
│ 8       ┆ 101       ┆ 4          ┆ 3             ┆ … ┆ 4311 Clay Rd  ┆ 3        ┆ 90    ┆ 2025-02-18 10:45:22 │
│ 9       ┆ 101       ┆ 2          ┆ 3             ┆ … ┆               ┆ 2        ┆ 20    ┆ 2025-03-10 12:59:04 │
│ 10      ┆ 102       ┆ 3          ┆ 5             ┆ … ┆ null          ┆ 0        ┆ 60    ┆ 2025-03-16 23:25:15 │
└─────────┴───────────┴────────────┴───────────────┴───┴───────────────┴──────────┴───────┴─────────────────────┘
*/

const DEBUG: bool = true;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<orders::Model>> {
    let results = orders::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<orders::Model>> {
    let query = "
    SELECT * 
    FROM sales.orders;
    ";
    let results = sqlx::query_as::<_, orders::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_orders = get_df_orders(&db_sea_orm).await?.lazy();
    let df = df_orders.collect().map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
