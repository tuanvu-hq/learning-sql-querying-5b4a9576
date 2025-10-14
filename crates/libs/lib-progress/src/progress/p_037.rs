use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::employees;

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::get_df_employees;
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT *
FROM sales.employees;
*/

/*
shape: (5, 8)
┌────────────┬───────────┬──────────┬────────────┬────────────┬────────┬────────┬───────────┐
│ employeeid ┆ firstname ┆ lastname ┆ department ┆ birthdate  ┆ gender ┆ salary ┆ managerid │
│ ---        ┆ ---       ┆ ---      ┆ ---        ┆ ---        ┆ ---    ┆ ---    ┆ ---       │
│ i32        ┆ str       ┆ str      ┆ str        ┆ date       ┆ str    ┆ i32    ┆ i32       │
╞════════════╪═══════════╪══════════╪════════════╪════════════╪════════╪════════╪═══════════╡
│ 1          ┆ Frank     ┆ Lee      ┆ Marketing  ┆ 1988-12-05 ┆ M      ┆ 55000  ┆ null      │
│ 2          ┆ Kevin     ┆ Brown    ┆ Marketing  ┆ 1972-11-25 ┆ M      ┆ 65000  ┆ 1         │
│ 3          ┆ Mary      ┆ null     ┆ Sales      ┆ 1986-01-05 ┆ F      ┆ 75000  ┆ 1         │
│ 4          ┆ Michael   ┆ Ray      ┆ Sales      ┆ 1977-02-10 ┆ M      ┆ 90000  ┆ 2         │
│ 5          ┆ Carol     ┆ Baker    ┆ Sales      ┆ 1982-02-11 ┆ F      ┆ 55000  ┆ 3         │
└────────────┴───────────┴──────────┴────────────┴────────────┴────────┴────────┴───────────┘
*/

const DEBUG: bool = true;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<employees::Model>> {
    let results = employees::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<employees::Model>> {
    let query = "
    SELECT * 
    FROM sales.employees;
    ";
    let results = sqlx::query_as::<_, employees::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_employees = get_df_employees(&db_sea_orm).await?.lazy();
    let df = df_employees.collect().map_err(AppError::Polars)?;

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
