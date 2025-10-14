use polars::prelude::*;
use sea_orm::sea_query::Expr as SeaExpr;
use sea_orm::sea_query::extension::postgres::PgExpr;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use sqlx::Pool;
use sqlx::prelude::FromRow;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::customers;

use crate::utils::compare::compare_vecs_unordered;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_customers;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    first_name,
    LOWER(first_name) AS lower_name,
    UPPER(first_name) AS upper_name
FROM customers;
*/

/*
shape: (5, 3)
┌───────────┬─────────┬────────────────┐
│ firstname ┆ country ┆ name_country   │
│ ---       ┆ ---     ┆ ---            │
│ str       ┆ str     ┆ str            │
╞═══════════╪═════════╪════════════════╡
│ Jossef    ┆ Germany ┆ Jossef-Germany │
│ Kevin     ┆ USA     ┆ Kevin-USA      │
│ Mary      ┆ USA     ┆ Mary-USA       │
│ Mark      ┆ Germany ┆ Mark-Germany   │
│ Anna      ┆ USA     ┆ Anna-USA       │
└───────────┴─────────┴────────────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult, FromRow, Hash)]
struct Person {
    firstname: Option<String>,
    lower_name: String,
    upper_name: String,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Person>> {
    let results = customers::Entity::find()
        .select_only()
        .column(customers::Column::Firstname)
        .column(customers::Column::Country)
        .column_as(
            SeaExpr::col(customers::Column::Firstname)
                .concat(SeaExpr::val("-")) // literal from sea_query
                .concat(SeaExpr::col(customers::Column::Country)),
            "name_country",
        )
        .into_model::<Person>()
        .all(db)
        .await?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<Person>> {
    let query = "
    SELECT
        firstname,
        country,
        CONCAT(firstname, '-', country) AS name_country
    FROM sales.customers;
    ";
    let results = sqlx::query_as::<_, Person>(query)
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
        .select([
            col("firstname"),
            col("country"),
            (col("firstname") + lit("-") + col("country")).alias("name_country"),
        ])
        .collect()
        .map_err(AppError::Polars)?;

    // Note: without ORDER BY, the row order in both results will vary
    if compare_vecs_unordered(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
