use std::collections::HashSet;

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use sqlx::Pool;
use sqlx::prelude::FromRow;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::{orders, ordersarchive};

use crate::utils::compare::compare_vecs_unordered;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::{get_df_orders, get_df_ordersarchive};
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    'Orders' AS source_table,
    orderid,
    productid,
    customerid,
    salespersonid,
    orderdate,
    shipdate,
    orderstatus,
    shipaddress,
    billaddress,
    quantity,
    sales,
    creationtime
FROM sales.orders
UNION
SELECT
    'OrdersArchive' AS source_table,
    orderid,
    productid,
    customerid,
    salespersonid,
    orderdate,
    shipdate,
    orderstatus,
    shipaddress,
    billaddress,
    quantity,
    sales,
    creationtime
FROM sales.ordersarchive;
*/

/*
shape: (20, 13)
┌───────────────┬─────────┬───────────┬────────────┬───┬────────────────┬──────────┬───────┬─────────────────────┐
│ source_table  ┆ orderid ┆ productid ┆ customerid ┆ … ┆ billaddress    ┆ quantity ┆ sales ┆ creationtime        │
│ ---           ┆ ---     ┆ ---       ┆ ---        ┆   ┆ ---            ┆ ---      ┆ ---   ┆ ---                 │
│ str           ┆ i32     ┆ i32       ┆ i32        ┆   ┆ str            ┆ i32      ┆ i32   ┆ datetime[ms]        │
╞═══════════════╪═════════╪═══════════╪════════════╪═══╪════════════════╪══════════╪═══════╪═════════════════════╡
│ Orders        ┆ 1       ┆ 101       ┆ 2          ┆ … ┆ 1226 Shoe St.  ┆ 1        ┆ 10    ┆ 2025-01-01 12:34:56 │
│ Orders        ┆ 2       ┆ 102       ┆ 3          ┆ … ┆ null           ┆ 1        ┆ 15    ┆ 2025-01-05 23:22:04 │
│ Orders        ┆ 3       ┆ 101       ┆ 1          ┆ … ┆ 8157 W. Book   ┆ 2        ┆ 20    ┆ 2025-01-10 18:24:08 │
│ Orders        ┆ 4       ┆ 105       ┆ 1          ┆ … ┆                ┆ 2        ┆ 60    ┆ 2025-01-20 05:50:33 │
│ Orders        ┆ 5       ┆ 104       ┆ 2          ┆ … ┆ null           ┆ 1        ┆ 25    ┆ 2025-02-01 14:02:41 │
│ …             ┆ …       ┆ …         ┆ …          ┆ … ┆ …              ┆ …        ┆ …     ┆ …                   │
│ OrdersArchive ┆ 5       ┆ 104       ┆ 2          ┆ … ┆ 678 Pine St    ┆ 1        ┆ 25    ┆ 2024-05-01 14:02:41 │
│ OrdersArchive ┆ 6       ┆ 104       ┆ 3          ┆ … ┆ null           ┆ 2        ┆ 50    ┆ 2024-05-06 15:34:57 │
│ OrdersArchive ┆ 6       ┆ 104       ┆ 3          ┆ … ┆ 3768 Door Way  ┆ 2        ┆ 50    ┆ 2024-05-07 13:22:05 │
│ OrdersArchive ┆ 6       ┆ 101       ┆ 3          ┆ … ┆ 3768 Door Way  ┆ 2        ┆ 50    ┆ 2024-05-12 20:36:55 │
│ OrdersArchive ┆ 7       ┆ 102       ┆ 3          ┆ … ┆ 222 Billing St ┆ 0        ┆ 60    ┆ 2024-06-16 23:25:15 │
└───────────────┴─────────┴───────────┴────────────┴───┴────────────────┴──────────┴───────┴─────────────────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult, FromRow, Hash)]
struct Order {
    source_table: String,
    orderid: Option<i32>,
    productid: Option<i32>,
    customerid: Option<i32>,
    salespersonid: Option<i32>,
    orderdate: Option<NaiveDate>,
    shipdate: Option<NaiveDate>,
    orderstatus: Option<String>,
    shipaddress: Option<String>,
    billaddress: Option<String>,
    quantity: Option<i32>,
    sales: Option<i32>,
    creationtime: Option<NaiveDateTime>,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<Order>> {
    let mut results = Vec::new();
    let orders_query = orders::Entity::find()
        .select_only()
        .column_as(sea_orm::sea_query::Expr::val("Orders"), "source_table")
        .column(orders::Column::Orderid)
        .column(orders::Column::Productid)
        .column(orders::Column::Customerid)
        .column(orders::Column::Salespersonid)
        .column(orders::Column::Orderdate)
        .column(orders::Column::Shipdate)
        .column(orders::Column::Orderstatus)
        .column(orders::Column::Shipaddress)
        .column(orders::Column::Billaddress)
        .column(orders::Column::Quantity)
        .column(orders::Column::Sales)
        .column(orders::Column::Creationtime)
        .into_model::<Order>()
        .all(db)
        .await?;
    let ordersarchive_query = ordersarchive::Entity::find()
        .select_only()
        .column_as(
            sea_orm::sea_query::Expr::val("OrdersArchive"),
            "source_table",
        )
        .column(ordersarchive::Column::Orderid)
        .column(ordersarchive::Column::Productid)
        .column(ordersarchive::Column::Customerid)
        .column(ordersarchive::Column::Salespersonid)
        .column(ordersarchive::Column::Orderdate)
        .column(ordersarchive::Column::Shipdate)
        .column(ordersarchive::Column::Orderstatus)
        .column(ordersarchive::Column::Shipaddress)
        .column(ordersarchive::Column::Billaddress)
        .column(ordersarchive::Column::Quantity)
        .column(ordersarchive::Column::Sales)
        .column(ordersarchive::Column::Creationtime)
        .into_model::<Order>()
        .all(db)
        .await?;

    results.extend(orders_query);
    results.extend(ordersarchive_query);

    let mut seen = HashSet::new();

    results.retain(|order| seen.insert(order.clone()));

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<Order>> {
    let query = "
    SELECT
        'Orders' AS source_table,
        orderid,
        productid,
        customerid,
        salespersonid,
        orderdate,
        shipdate,
        orderstatus,
        shipaddress,
        billaddress,
        quantity,
        sales,
        creationtime
    FROM sales.orders
    UNION
    SELECT
        'OrdersArchive' AS source_table,
        orderid,
        productid,
        customerid,
        salespersonid,
        orderdate,
        shipdate,
        orderstatus,
        shipaddress,
        billaddress,
        quantity,
        sales,
        creationtime
    FROM sales.ordersarchive;
    ";
    let results = sqlx::query_as::<_, Order>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_orders = get_df_orders(&db_sea_orm)
        .await?
        .lazy()
        .with_column(lit("Orders").alias("source_table"))
        .select([
            col("source_table"),
            col("orderid"),
            col("productid"),
            col("customerid"),
            col("salespersonid"),
            col("orderdate"),
            col("shipdate"),
            col("orderstatus"),
            col("shipaddress"),
            col("billaddress"),
            col("quantity"),
            col("sales"),
            col("creationtime"),
        ]);
    let df_ordersarchive = get_df_ordersarchive(&db_sea_orm)
        .await?
        .lazy()
        .with_column(lit("OrdersArchive").alias("source_table"))
        .select([
            col("source_table"),
            col("orderid"),
            col("productid"),
            col("customerid"),
            col("salespersonid"),
            col("orderdate"),
            col("shipdate"),
            col("orderstatus"),
            col("shipaddress"),
            col("billaddress"),
            col("quantity"),
            col("sales"),
            col("creationtime"),
        ]);
    let df = concat(
        &[df_orders, df_ordersarchive], // Combine DataFrames
        UnionArgs {
            parallel: false,            // Consistent with your example
            rechunk: true,              // Rechunk for consistency
            to_supertypes: true,        // Ensure compatible types
            diagonal: false,            // Not relevant for this case
            from_partitioned_ds: false, // Not partitioned
            maintain_order: true,       // Preserve order as in your example
        },
    )
    .map_err(AppError::Polars)?
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
