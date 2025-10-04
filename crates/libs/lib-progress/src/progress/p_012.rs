use polars::prelude::*;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
use sqlx::{FromRow, Pool, Postgres};

use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

use crate::model::df_customers::df_customers;
use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    country,
    AVG(score) AS avg_score
FROM customers
WHERE score != 0
GROUP BY country
HAVING AVG(score) > 430;
*/

/*
shape: (2, 2)
┌─────────┬───────────┐
│ country ┆ avg_score │
│ ---     ┆ ---       │
│ str     ┆ f64       │
╞═════════╪═══════════╡
│ UK      ┆ 750.0     │
│ USA     ┆ 900.0     │
└─────────┴───────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, FromRow, FromQueryResult)]
struct Customer {
    country: Option<String>,
    avg_score: f64,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Customer>> {
    let results = customers::Entity::find()
        .select_only()
        .column(customers::Column::Country)
        .column_as(Expr::cust("AVG(score::FLOAT)"), "avg_score")
        .filter(customers::Column::Score.ne(0))
        .group_by(customers::Column::Country)
        .having(Expr::cust("AVG(score::FLOAT)").gt(430))
        .into_model::<Customer>()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<Customer>> {
    let query = "
    SELECT
        country,
        AVG(score)::FLOAT8 AS avg_score
    FROM customers
    WHERE score != 0
    GROUP BY country
    HAVING AVG(score) > 430;
    ";
    let results = sqlx::query_as::<_, Customer>(query)
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
        .filter(col("score").neq(0))
        .group_by(["country"])
        .agg([col("score").mean().alias("avg_score")])
        .filter(col("avg_score").gt(430))
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
