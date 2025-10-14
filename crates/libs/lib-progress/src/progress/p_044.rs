use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use sqlx::Pool;
use sqlx::prelude::FromRow;

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
EXCEPT
SELECT
    firstname,
    lastname
FROM sales.customers;
*/

/*
shape: (3, 2)
┌───────────┬──────────┐
│ firstname ┆ lastname │
│ ---       ┆ ---      │
│ str       ┆ str      │
╞═══════════╪══════════╡
│ Frank     ┆ Lee      │
│ Michael   ┆ Ray      │
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

    results.extend(
        employees_query
            .into_iter()
            .filter(|emp| {
                !customers_query
                    .iter()
                    .any(|cust| cust.firstname == emp.firstname && cust.lastname == emp.lastname)
            })
            .collect::<Vec<Person>>(),
    );

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<Person>> {
    let query = "
    SELECT
        firstname,
        lastname
    FROM sales.employees
    EXCEPT
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
    let df = df_employees
        .join(
            df_customers,
            [col("firstname"), col("lastname")],
            [col("firstname"), col("lastname")],
            JoinType::Anti.into(),
        )
        .select([col("firstname"), col("lastname")])
        .collect()
        .map_err(AppError::Polars)?
        .lazy()
        .filter(
            col("firstname")
                .is_not_null()
                .and(col("lastname").is_not_null()),
        )
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
