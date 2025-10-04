use polars::prelude::*;
use sea_orm::sea_query::Expr;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
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
    SUM(score) AS total_score
    COUNT(id) AS total_customers
FROM customers
GROUP BY country;
*/

/*
shape: (3, 3)
┌─────────┬─────────────┬─────────────────┐
│ country ┆ total_score ┆ total_customers │
│ ---     ┆ ---         ┆ ---             │
│ str     ┆ i32         ┆ u32             │
╞═════════╪═════════════╪═════════════════╡
│ Germany ┆ 850         ┆ 2               │
│ UK      ┆ 750         ┆ 1               │
│ USA     ┆ 900         ┆ 2               │
└─────────┴─────────────┴─────────────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, Eq, FromRow, FromQueryResult)]
struct Customer {
    country: Option<String>,
    total_score: i64,
    total_customers: i64,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Customer>> {
    let results = customers::Entity::find()
        .select_only()
        .column(customers::Column::Country)
        .column_as(Expr::cust("SUM(score)"), "total_score")
        .column_as(Expr::cust("COUNT(id)"), "total_customers")
        .group_by(customers::Column::Country)
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
        SUM(score) AS total_score,
        COUNT(id) AS total_customers
    FROM customers
    GROUP BY country;
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
        .group_by(["country"])
        .agg([
            col("score").sum().alias("total_score"),
            col("id").count().alias("total_customers"),
        ])
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
