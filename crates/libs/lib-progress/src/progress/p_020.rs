use polars::prelude::*;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter};
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
WHERE country = 'USA' AND score > 500;
*/

/*
shape: (1, 4)
┌─────┬────────────┬─────────┬───────┐
│ id  ┆ first_name ┆ country ┆ score │
│ --- ┆ ---        ┆ ---     ┆ ---   │
│ i32 ┆ str        ┆ str     ┆ i32   │
╞═════╪════════════╪═════════╪═══════╡
│ 2   ┆  John      ┆ USA     ┆ 900   │
└─────┴────────────┴─────────┴───────┘
*/

const DEBUG: bool = false;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<customers::Model>> {
    let results = customers::Entity::find()
        .filter(
            Condition::all()
                .add(customers::Column::Country.eq("USA"))
                .add(customers::Column::Score.gt(500)),
        )
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
    WHERE country = 'USA' AND score > 500;
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
        .filter(col("country").eq(lit("USA")).and(col("score").gt(500)))
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
