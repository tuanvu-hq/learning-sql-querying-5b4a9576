use polars::prelude::*;

use lib_core::error::{AppError, AppResult};

use crate::utils::database::get_database;
use crate::utils::dataframe::{get_df_customers, get_df_orders};
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    c.id,
    c.first_name,
    o.order_id,
    o.sales
FROM customers AS c
CROSS JOIN orders AS o;
*/

/*
shape: (20, 8)
┌─────┬────────────┬─────────┬───────┬──────────┬─────────────┬────────────┬───────┐
│ id  ┆ first_name ┆ country ┆ score ┆ order_id ┆ customer_id ┆ order_date ┆ sales │
│ --- ┆ ---        ┆ ---     ┆ ---   ┆ ---      ┆ ---         ┆ ---        ┆ ---   │
│ i32 ┆ str        ┆ str     ┆ i32   ┆ i32      ┆ i32         ┆ date       ┆ i32   │
╞═════╪════════════╪═════════╪═══════╪══════════╪═════════════╪════════════╪═══════╡
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
│ 2   ┆  John      ┆ USA     ┆ 900   ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
│ …   ┆ …          ┆ …       ┆ …     ┆ …        ┆ …           ┆ …          ┆ …     │
│ 4   ┆ Martin     ┆ Germany ┆ 500   ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
└─────┴────────────┴─────────┴───────┴──────────┴─────────────┴────────────┴───────┘
*/

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, _db_sqlx) = get_database().await?;
    let df_customers = get_df_customers(&db_sea_orm).await?.lazy();
    let df_orders = get_df_orders(&db_sea_orm).await?.lazy();
    let df = df_customers
        .cross_join(df_orders, None)
        .collect()
        .map_err(AppError::Polars)?;

    /*
    shape: (20, 8)
    ┌─────┬────────────┬─────────┬───────┬──────────┬─────────────┬────────────┬───────┐
    │ id  ┆ first_name ┆ country ┆ score ┆ order_id ┆ customer_id ┆ order_date ┆ sales │
    │ --- ┆ ---        ┆ ---     ┆ ---   ┆ ---      ┆ ---         ┆ ---        ┆ ---   │
    │ i32 ┆ str        ┆ str     ┆ i32   ┆ i32      ┆ i32         ┆ date       ┆ i32   │
    ╞═════╪════════════╪═════════╪═══════╪══════════╪═════════════╪════════════╪═══════╡
    │ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
    │ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
    │ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
    │ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
    │ 2   ┆  John      ┆ USA     ┆ 900   ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
    │ …   ┆ …          ┆ …       ┆ …     ┆ …        ┆ …           ┆ …          ┆ …     │
    │ 4   ┆ Martin     ┆ Germany ┆ 500   ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
    │ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
    │ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
    │ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
    │ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
    └─────┴────────────┴─────────┴───────┴──────────┴─────────────┴────────────┴───────┘
        */

    log_debug("POLARS", &df, None);

    Ok(())
}
