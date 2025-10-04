use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};
use sqlx::{Pool, Postgres};

use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

use crate::model::df_customers::df_customers;
use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM customers
ORDER BY score DESC;
*/

/*
shape: (5, 4)
┌─────┬────────────┬─────────┬───────┐
│ id  ┆ first_name ┆ country ┆ score │
│ --- ┆ ---        ┆ ---     ┆ ---   │
│ i32 ┆ str        ┆ str     ┆ i32   │
╞═════╪════════════╪═════════╪═══════╡
│ 2   ┆  John      ┆ USA     ┆ 900   │
│ 3   ┆ Georg      ┆ UK      ┆ 750   │
│ 4   ┆ Martin     ┆ Germany ┆ 500   │
│ 1   ┆ Maria      ┆ Germany ┆ 350   │
│ 5   ┆ Peter      ┆ USA     ┆ 0     │
└─────┴────────────┴─────────┴───────┘
*/

const DEBUG: bool = false;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<customers::Model>> {
    let results = customers::Entity::find()
        .order_by_desc(customers::Column::Score)
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<customers::Model>> {
    let query = "
    SELECT *
    FROM customers
    ORDER BY score DESC;
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
    let df = df_customers(&db_sea_orm)
        .await?
        .lazy()
        .sort(
            ["score"],
            SortMultipleOptions::new().with_order_descending(true),
        )
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
