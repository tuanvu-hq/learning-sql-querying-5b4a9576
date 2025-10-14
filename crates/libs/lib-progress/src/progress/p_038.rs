use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::ordersarchive;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_ordersarchive;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM sales.ordersarchive;
*/

/*
shape: (10, 13)
┌───────────┬─────────┬───────────┬────────────┬───┬────────────────┬──────────┬───────┬─────────────────────┐
│ archiveid ┆ orderid ┆ productid ┆ customerid ┆ … ┆ billaddress    ┆ quantity ┆ sales ┆ creationtime        │
│ ---       ┆ ---     ┆ ---       ┆ ---        ┆   ┆ ---            ┆ ---      ┆ ---   ┆ ---                 │
│ i32       ┆ i32     ┆ i32       ┆ i32        ┆   ┆ str            ┆ i32      ┆ i32   ┆ datetime[ms]        │
╞═══════════╪═════════╪═══════════╪════════════╪═══╪════════════════╪══════════╪═══════╪═════════════════════╡
│ 1         ┆ 1       ┆ 101       ┆ 2          ┆ … ┆ 456 Billing St ┆ 1        ┆ 10    ┆ 2024-04-01 12:34:56 │
│ 2         ┆ 2       ┆ 102       ┆ 3          ┆ … ┆ 789 Billing St ┆ 1        ┆ 15    ┆ 2024-04-05 23:22:04 │
│ 3         ┆ 3       ┆ 101       ┆ 1          ┆ … ┆ 789 Maple St   ┆ 2        ┆ 20    ┆ 2024-04-10 18:24:08 │
│ 4         ┆ 4       ┆ 105       ┆ 1          ┆ … ┆                ┆ 2        ┆ 60    ┆ 2024-04-20 05:50:33 │
│ 5         ┆ 4       ┆ 105       ┆ 1          ┆ … ┆                ┆ 2        ┆ 60    ┆ 2024-04-20 14:50:33 │
│ 6         ┆ 5       ┆ 104       ┆ 2          ┆ … ┆ 678 Pine St    ┆ 1        ┆ 25    ┆ 2024-05-01 14:02:41 │
│ 7         ┆ 6       ┆ 104       ┆ 3          ┆ … ┆ null           ┆ 2        ┆ 50    ┆ 2024-05-06 15:34:57 │
│ 8         ┆ 6       ┆ 104       ┆ 3          ┆ … ┆ 3768 Door Way  ┆ 2        ┆ 50    ┆ 2024-05-07 13:22:05 │
│ 9         ┆ 6       ┆ 101       ┆ 3          ┆ … ┆ 3768 Door Way  ┆ 2        ┆ 50    ┆ 2024-05-12 20:36:55 │
│ 10        ┆ 7       ┆ 102       ┆ 3          ┆ … ┆ 222 Billing St ┆ 0        ┆ 60    ┆ 2024-06-16 23:25:15 │
└───────────┴─────────┴───────────┴────────────┴───┴────────────────┴──────────┴───────┴─────────────────────┘
*/

const DEBUG: bool = true;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<ordersarchive::Model>> {
    let results = ordersarchive::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<ordersarchive::Model>> {
    let query = "
    SELECT * 
    FROM sales.ordersarchive;
    ";
    let results = sqlx::query_as::<_, ordersarchive::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_ordersarchive = get_df_ordersarchive(&db_sea_orm).await?.lazy();
    let df = df_ordersarchive.collect().map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
