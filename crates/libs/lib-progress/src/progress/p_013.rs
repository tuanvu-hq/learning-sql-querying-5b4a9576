use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use sqlx::{FromRow, Pool, Postgres};

use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

use crate::utils::dataframe::get_df_customers;
use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT DISTINCT
    country
FROM customers;
*/

/*
shape: (3, 1)
┌─────────┐
│ country │
│ ---     │
│ str     │
╞═════════╡
│ USA     │
│ Germany │
│ UK      │
└─────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, FromRow, FromQueryResult)]
struct Customer {
    country: Option<String>,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Customer>> {
    let results = customers::Entity::find()
        .select_only()
        .column(customers::Column::Country)
        .distinct()
        .into_model::<Customer>()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<Customer>> {
    let query = "
    SELECT DISTINCT
        country
    FROM customers;
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
    let df_customers = get_df_customers(&db_sea_orm).await?.lazy();
    let df = df_customers
        .select([col("country")])
        .unique(None, UniqueKeepStrategy::First)
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
