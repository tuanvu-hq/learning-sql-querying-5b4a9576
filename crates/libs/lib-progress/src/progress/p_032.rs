use polars::prelude::*;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
use sqlx::{FromRow, Pool, Postgres};

use lib_core::error::{AppError, AppResult};
use lib_data::database::{customers, orders};

use crate::utils::compare::compare_vecs;
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
LEFT JOIN orders AS o
ON c.id = o.customer_id
WHERE o.customer_id IS NULL;
*/

/*
shape: (2, 4)
┌─────┬────────────┬──────────┬───────┐
│ id  ┆ first_name ┆ order_id ┆ sales │
│ --- ┆ ---        ┆ ---      ┆ ---   │
│ i32 ┆ str        ┆ i32      ┆ i32   │
╞═════╪════════════╪══════════╪═══════╡
│ 4   ┆ Martin     ┆ null     ┆ null  │
│ 5   ┆ Peter      ┆ null     ┆ null  │
└─────┴────────────┴──────────┴───────┘
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

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Customer>> {
    // Note: Entities have no relation. No-relation code solution.
    let results = customers::Entity::find()
        .select_only()
        .column(customers::Column::Id)
        .column(customers::Column::FirstName)
        .column_as(orders::Column::OrderId, "order_id")
        .column_as(orders::Column::Sales, "sales")
        .join_rev(
            sea_orm::JoinType::LeftJoin,
            orders::Entity::belongs_to(customers::Entity)
                .from(orders::Column::CustomerId)
                .to(customers::Column::Id)
                .into(),
        )
        .filter(orders::Column::CustomerId.is_null())
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
        c.id,
        c.first_name,
        o.order_id,
        o.sales
    FROM customers AS c
    LEFT JOIN orders AS o
    ON c.id = o.customer_id
    WHERE o.customer_id IS NULL;
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
    let df_customers = get_df_customers(&db_sea_orm)
        .await?
        .lazy()
        .with_column(col("id").alias("join_id"));
    let df_orders = get_df_orders(&db_sea_orm)
        .await?
        .lazy()
        .with_column(col("customer_id").alias("join_customer_id"));
    let df = df_customers
        .join(
            df_orders,
            [col("join_id")],
            [col("join_customer_id")],
            JoinArgs::new(JoinType::Left),
        )
        .filter(col("customer_id").is_null())
        .select([col("id"), col("first_name"), col("order_id"), col("sales")])
        .collect()
        .map_err(AppError::Polars)?;

    /*
    shape: (5, 9)
    ┌─────┬────────────┬─────────┬───────┬───┬──────────┬─────────────┬────────────┬───────┐
    │ id  ┆ first_name ┆ country ┆ score ┆ … ┆ order_id ┆ customer_id ┆ order_date ┆ sales │
    │ --- ┆ ---        ┆ ---     ┆ ---   ┆   ┆ ---      ┆ ---         ┆ ---        ┆ ---   │
    │ i32 ┆ str        ┆ str     ┆ i32   ┆   ┆ i32      ┆ i32         ┆ date       ┆ i32   │
    ╞═════╪════════════╪═════════╪═══════╪═══╪══════════╪═════════════╪════════════╪═══════╡
    │ 1   ┆ Maria      ┆ Germany ┆ 350   ┆ … ┆ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
    │ 2   ┆  John      ┆ USA     ┆ 900   ┆ … ┆ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
    │ 3   ┆ Georg      ┆ UK      ┆ 750   ┆ … ┆ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
    │ 4   ┆ Martin     ┆ Germany ┆ 500   ┆ … ┆ null     ┆ null        ┆ null       ┆ null  │
    │ 5   ┆ Peter      ┆ USA     ┆ 0     ┆ … ┆ null     ┆ null        ┆ null       ┆ null  │
    └─────┴────────────┴─────────┴───────┴───┴──────────┴─────────────┴────────────┴───────┘
    */

    if compare_vecs(
        &sea_orm_query(&db_sea_orm).await?,
        &sqlx_query(&db_sqlx).await?,
    ) {
        log_debug("POLARS", &df, None);
    }

    Ok(())
}
