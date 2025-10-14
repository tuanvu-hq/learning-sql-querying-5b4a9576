use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::products;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_products;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM sales.products;
*/

/*
shape: (5, 4)
┌───────────┬─────────┬─────────────┬───────┐
│ productid ┆ product ┆ category    ┆ price │
│ ---       ┆ ---     ┆ ---         ┆ ---   │
│ i32       ┆ str     ┆ str         ┆ i32   │
╞═══════════╪═════════╪═════════════╪═══════╡
│ 101       ┆ Bottle  ┆ Accessories ┆ 10    │
│ 102       ┆ Tire    ┆ Accessories ┆ 15    │
│ 103       ┆ Socks   ┆ Clothing    ┆ 20    │
│ 104       ┆ Caps    ┆ Clothing    ┆ 25    │
│ 105       ┆ Gloves  ┆ Clothing    ┆ 30    │
└───────────┴─────────┴─────────────┴───────┘
*/

const DEBUG: bool = true;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<products::Model>> {
    let results = products::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<products::Model>> {
    let query = "
    SELECT * 
    FROM sales.products;
    ";
    let results = sqlx::query_as::<_, products::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_products = get_df_products(&db_sea_orm).await?.lazy();
    let df = df_products.collect().map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
