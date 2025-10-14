use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::customers;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_customers;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM sales.customers;
*/

/*
shape: (5, 5)
┌────────────┬───────────┬──────────┬─────────┬───────┐
│ customerid ┆ firstname ┆ lastname ┆ country ┆ score │
│ ---        ┆ ---       ┆ ---      ┆ ---     ┆ ---   │
│ i32        ┆ str       ┆ str      ┆ str     ┆ i32   │
╞════════════╪═══════════╪══════════╪═════════╪═══════╡
│ 1          ┆ Jossef    ┆ Goldberg ┆ Germany ┆ 350   │
│ 2          ┆ Kevin     ┆ Brown    ┆ USA     ┆ 900   │
│ 3          ┆ Mary      ┆ null     ┆ USA     ┆ 750   │
│ 4          ┆ Mark      ┆ Schwarz  ┆ Germany ┆ 500   │
│ 5          ┆ Anna      ┆ Adams    ┆ USA     ┆ null  │
└────────────┴───────────┴──────────┴─────────┴───────┘
*/

const DEBUG: bool = true;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<customers::Model>> {
    let results = customers::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<customers::Model>> {
    let query = "
    SELECT * 
    FROM sales.customers;
    ";
    let results = sqlx::query_as::<_, customers::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_customers = get_df_customers(&db_sea_orm).await?.lazy();
    let df = df_customers.collect().map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
