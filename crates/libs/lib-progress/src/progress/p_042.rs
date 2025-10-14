use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use sqlx::Pool;
use sqlx::prelude::FromRow;
use std::collections::HashSet;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::{customers, employees};

use crate::utils::compare::compare_vecs_unordered;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::{get_df_customers, get_df_employees};
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    firstname,
    lastname
FROM sales.employees
UNION
SELECT
    firstname,
    lastname
FROM sales.customers;
*/

/*
shape: (8, 2)
┌───────────┬──────────┐
│ firstname ┆ lastname │
│ ---       ┆ ---      │
│ str       ┆ str      │
╞═══════════╪══════════╡
│ Michael   ┆ Ray      │
│ Anna      ┆ Adams    │
│ Frank     ┆ Lee      │
│ Kevin     ┆ Brown    │
│ Mary      ┆ null     │
│ Mark      ┆ Schwarz  │
│ Jossef    ┆ Goldberg │
│ Carol     ┆ Baker    │
└───────────┴──────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult, FromRow, Hash)]
struct Person {
    firstname: Option<String>,
    lastname: Option<String>,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Person>> {
    // Note: sea-orm doesn't have a union API by the time of code creation.
    let mut results = Vec::new();
    let employees_query = employees::Entity::find()
        .select_only()
        .column(employees::Column::Firstname)
        .column(employees::Column::Lastname)
        .into_model::<Person>()
        .all(db)
        .await?;
    let customers_query = customers::Entity::find()
        .select_only()
        .column(customers::Column::Firstname)
        .column(customers::Column::Lastname)
        .into_model::<Person>()
        .all(db)
        .await?;

    results.extend(employees_query);
    results.extend(customers_query);

    let mut seen = HashSet::new();

    // insert -> bool (if this is the first time this pair appears)
    results.retain(|p| seen.insert((p.firstname.clone(), p.lastname.clone())));

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<Person>> {
    let query = "
    SELECT
        firstname,
        lastname
    FROM sales.employees
    UNION
    SELECT
        firstname,
        lastname
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
    let df_employees = get_df_employees(&db_sea_orm).await?.lazy();
    let df_customers = get_df_customers(&db_sea_orm).await?.lazy();
    let df = concat(
        &[
            df_employees.select([col("firstname"), col("lastname")]),
            df_customers.select([col("firstname"), col("lastname")]),
        ],
        UnionArgs {
            parallel: false,
            rechunk: true,
            to_supertypes: true,
            diagonal: false,
            from_partitioned_ds: false,
            maintain_order: true,
        },
    )
    .map_err(AppError::Polars)?
    .select([col("firstname"), col("lastname")])
    .unique(None, UniqueKeepStrategy::First)
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
