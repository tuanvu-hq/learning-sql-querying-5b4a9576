#![allow(unused)]

use polars::prelude::*;
use sea_orm::{DatabaseConnection, FromQueryResult};
use sqlx::{FromRow, Pool, Postgres};

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
shape: (20, 10)
┌─────┬────────────┬─────────┬───────┬───┬─────────────┬────────────┬───────┬──────────────────┐
│ id  ┆ first_name ┆ country ┆ score ┆ … ┆ customer_id ┆ order_date ┆ sales ┆ join_customer_id │
│ --- ┆ ---        ┆ ---     ┆ ---   ┆   ┆ ---         ┆ ---        ┆ ---   ┆ ---              │
│ i32 ┆ str        ┆ str     ┆ i32   ┆   ┆ i32         ┆ date       ┆ i32   ┆ i32              │
╞═════╪════════════╪═════════╪═══════╪═══╪═════════════╪════════════╪═══════╪══════════════════╡
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 1           ┆ 2021-01-11 ┆ 35    ┆ 1                │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 2           ┆ 2021-04-05 ┆ 15    ┆ 2                │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 3           ┆ 2021-06-18 ┆ 20    ┆ 3                │
│ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 6           ┆ 2021-08-31 ┆ 10    ┆ 6                │
│ 2   ┆  John      ┆ USA     ┆ 900   ┆ … ┆ 1           ┆ 2021-01-11 ┆ 35    ┆ 1                │
│ …   ┆ …          ┆ …       ┆ …     ┆ … ┆ …           ┆ …          ┆ …     ┆ …                │
│ 4   ┆ Martin     ┆ Germany ┆ 500   ┆ … ┆ 6           ┆ 2021-08-31 ┆ 10    ┆ 6                │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ 1           ┆ 2021-01-11 ┆ 35    ┆ 1                │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ 2           ┆ 2021-04-05 ┆ 15    ┆ 2                │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ 3           ┆ 2021-06-18 ┆ 20    ┆ 3                │
│ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ 6           ┆ 2021-08-31 ┆ 10    ┆ 6                │
└─────┴────────────┴─────────┴───────┴───┴─────────────┴────────────┴───────┴──────────────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, FromRow, FromQueryResult)]
struct Customer {
    // customers
    pub id: Option<i32>,
    pub first_name: Option<String>,
    // orders
    pub order_id: Option<i32>,
    pub sales: Option<i32>,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<()> {
    // #INFO - Not working. Overcomplicated without relations for entities.
    // Note: Entities have no relation. No-relation code solution.
    // let results = customers::Entity::find()
    //     .select_only()
    //     .column(customers::Column::Id)
    //     .column(customers::Column::FirstName)
    //     .column_as(orders::Column::OrderId, "order_id")
    //     .column_as(orders::Column::Sales, "sales")
    //     .join_rev(
    //         sea_orm::JoinType::CrossJoin,
    //         orders::Entity::belongs_to(customers::Entity).into(),
    //     )
    //     .into_model::<Customer>()
    //     .all(db)
    //     .await
    //     .map_err(AppError::SeaOrm)?;

    // log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(())
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<Customer>> {
    let query = "
    SELECT
        c.id,
        c.first_name,
        o.order_id,
        o.sales
    FROM customers AS c
    CROSS JOIN orders AS o;
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
    let df_orders = get_df_orders(&db_sea_orm).await?.lazy();
    let df = df_customers
        .cross_join(df_orders, None)
        // .select([col("id"), col("first_name"), col("order_id"), col("sales")])
        .collect()
        .map_err(AppError::Polars)?;

    /*
    shape: (6, 10)
    ┌──────┬────────────┬─────────┬───────┬───┬─────────────┬────────────┬───────┬──────────────────┐
    │ id   ┆ first_name ┆ country ┆ score ┆ … ┆ customer_id ┆ order_date ┆ sales ┆ join_customer_id │
    │ ---  ┆ ---        ┆ ---     ┆ ---   ┆   ┆ ---         ┆ ---        ┆ ---   ┆ ---              │
    │ i32  ┆ str        ┆ str     ┆ i32   ┆   ┆ i32         ┆ date       ┆ i32   ┆ i32              │
    ╞══════╪════════════╪═════════╪═══════╪═══╪═════════════╪════════════╪═══════╪══════════════════╡
    │ 1    ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 1           ┆ 2021-01-11 ┆ 35    ┆ 1                │
    │ 2    ┆  John      ┆ USA     ┆ 900   ┆ … ┆ 2           ┆ 2021-04-05 ┆ 15    ┆ 2                │
    │ 3    ┆ Georg      ┆ UK      ┆ 750   ┆ … ┆ 3           ┆ 2021-06-18 ┆ 20    ┆ 3                │
    │ 4    ┆ Martin     ┆ Germany ┆ 500   ┆ … ┆ null        ┆ null       ┆ null  ┆ null             │
    │ 5    ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ null        ┆ null       ┆ null  ┆ null             │
    │ null ┆ null       ┆ null    ┆ null  ┆ … ┆ 6           ┆ 2021-08-31 ┆ 10    ┆ 6                │
    └──────┴────────────┴─────────┴───────┴───┴─────────────┴────────────┴───────┴──────────────────┘
    */

    if true {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
