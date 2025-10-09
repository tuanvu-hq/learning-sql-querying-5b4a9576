use polars::prelude::*;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use sqlx::{Pool, Postgres};

use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::debug::log_debug;
use crate::utils::dataframe::get_df_customers;

/*
# QUERY:

SELECT *
FROM customers
WHERE score != 0;
*/

/*
shape: (4, 4)
┌─────┬────────────┬─────────┬───────┐
│ id  ┆ first_name ┆ country ┆ score │
│ --- ┆ ---        ┆ ---     ┆ ---   │
│ i32 ┆ str        ┆ str     ┆ i32   │
╞═════╪════════════╪═════════╪═══════╡
│ 1   ┆ Maria      ┆ Germany ┆ 350   │
│ 2   ┆  John      ┆ USA     ┆ 900   │
│ 3   ┆ Georg      ┆ UK      ┆ 750   │
│ 4   ┆ Martin     ┆ Germany ┆ 500   │
└─────┴────────────┴─────────┴───────┘
*/

const DEBUG: bool = false;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<customers::Model>> {
    let results = customers::Entity::find()
        .filter(customers::Column::Score.ne(0))
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<customers::Model>> {
    let query = "
    SELECT * FROM 
    customers 
    WHERE score != 0;
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
    let df = df_customers
        .filter(col("score").neq(0))
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
