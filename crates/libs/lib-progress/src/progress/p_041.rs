use polars::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect, RelationTrait};
use sqlx::Pool;
use sqlx::prelude::FromRow;

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::{customers, employees, orders, products};

use crate::utils::compare::compare_vecs;
use crate::utils::database::get_database;
use crate::utils::dataframe::sales::{
    get_df_customers, get_df_employees, get_df_orders, get_df_products,
};
use crate::utils::debug::log_debug;

/*
# QUERY:

SELECT
    o.orderid,
    o.sales,
    c.firstname AS customer_firstname,
    c.lastname AS customer_lastname,
    p.product,
    p.price,
    e.firstname AS employee_firstname,
    e.lastname AS employee_lastname
FROM sales.orders AS o
LEFT JOIN sales.customers AS c
ON o.customerid = c.customerid
LEFT JOIN sales.products AS p
ON o.productid = p.productid
LEFT JOIN sales.employees AS e
ON o.salespersonid = e.employeeid;
*/

/*
shape: (10, 8)
┌──────────┬───────┬────────────────────┬───────────────────┬─────────┬───────┬────────────────────┬───────────────────┐
│ order_id ┆ sales ┆ customer_firstname ┆ customer_lastname ┆ product ┆ price ┆ employee_firstname ┆ employee_lastname │
│ ---      ┆ ---   ┆ ---                ┆ ---               ┆ ---     ┆ ---   ┆ ---                ┆ ---               │
│ i32      ┆ i32   ┆ str                ┆ str               ┆ str     ┆ i32   ┆ str                ┆ str               │
╞══════════╪═══════╪════════════════════╪═══════════════════╪═════════╪═══════╪════════════════════╪═══════════════════╡
│ 1        ┆ 10    ┆ Kevin              ┆ Brown             ┆ Bottle  ┆ 10    ┆ Kevin              ┆ Brown             │
│ 2        ┆ 15    ┆ Mary               ┆ null              ┆ Tire    ┆ 15    ┆ Mary               ┆ null              │
│ 3        ┆ 20    ┆ Jossef             ┆ Goldberg          ┆ Bottle  ┆ 10    ┆ Jossef             ┆ Goldberg          │
│ 4        ┆ 60    ┆ Jossef             ┆ Goldberg          ┆ Gloves  ┆ 30    ┆ Jossef             ┆ Goldberg          │
│ 5        ┆ 25    ┆ Kevin              ┆ Brown             ┆ Caps    ┆ 25    ┆ Kevin              ┆ Brown             │
│ 6        ┆ 50    ┆ Mary               ┆ null              ┆ Caps    ┆ 25    ┆ Mary               ┆ null              │
│ 7        ┆ 30    ┆ Jossef             ┆ Goldberg          ┆ Tire    ┆ 15    ┆ Jossef             ┆ Goldberg          │
│ 8        ┆ 90    ┆ Mark               ┆ Schwarz           ┆ Bottle  ┆ 10    ┆ Mark               ┆ Schwarz           │
│ 9        ┆ 20    ┆ Kevin              ┆ Brown             ┆ Bottle  ┆ 10    ┆ Kevin              ┆ Brown             │
│ 10       ┆ 60    ┆ Mary               ┆ null              ┆ Tire    ┆ 15    ┆ Mary               ┆ null              │
└──────────┴───────┴────────────────────┴───────────────────┴─────────┴───────┴────────────────────┴───────────────────┘
*/

const DEBUG: bool = false;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult, FromRow)]
struct OrderDetails {
    orderid: i32,
    sales: i32,
    customer_firstname: Option<String>,
    customer_lastname: Option<String>,
    product: Option<String>,
    price: Option<i32>,
    employee_firstname: Option<String>,
    employee_lastname: Option<String>,
}

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<OrderDetails>> {
    let results = orders::Entity::find()
        .select_only()
        .column(orders::Column::Orderid)
        .column(orders::Column::Sales)
        .column_as(customers::Column::Firstname, "customer_firstname")
        .column_as(customers::Column::Lastname, "customer_lastname")
        .column_as(products::Column::Product, "product")
        .column_as(products::Column::Price, "price")
        .column_as(employees::Column::Firstname, "employee_firstname")
        .column_as(employees::Column::Lastname, "employee_lastname")
        .join(
            sea_orm::JoinType::LeftJoin,
            orders::Relation::Customers.def(),
        )
        .join(
            sea_orm::JoinType::LeftJoin,
            orders::Relation::Products.def(),
        )
        .join(
            sea_orm::JoinType::LeftJoin,
            orders::Relation::Employees.def(),
        )
        .into_model::<OrderDetails>()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    log_debug("SEA ORM", &results, Some(DEBUG));

    Ok(results)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<OrderDetails>> {
    let query = "
    SELECT
        o.orderid,
        o.sales,
        c.firstname AS customer_firstname,
        c.lastname AS customer_lastname,
        p.product,
        p.price,
        e.firstname AS employee_firstname,
        e.lastname AS employee_lastname
    FROM sales.orders AS o
    LEFT JOIN sales.customers AS c
    ON o.customerid = c.customerid
    LEFT JOIN sales.products AS p
    ON o.productid = p.productid
    LEFT JOIN sales.employees AS e
    ON o.salespersonid = e.employeeid;
    ";
    let results = sqlx::query_as::<_, OrderDetails>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;

    log_debug("SQLX", &results, Some(DEBUG));

    Ok(results)
}

pub async fn display_table() -> AppResult<()> {
    let (db_sea_orm, db_sqlx) = get_database().await?;
    let df_orders = get_df_orders(&db_sea_orm).await?.lazy();
    let df_customers = get_df_customers(&db_sea_orm).await?.lazy();
    let df_products = get_df_products(&db_sea_orm).await?.lazy();
    let df_employees = get_df_employees(&db_sea_orm).await?.lazy();
    let df = df_orders
        .join(
            df_customers,
            [col("customerid")],
            [col("customerid")],
            JoinType::Left.into(),
        )
        .join(
            df_products,
            [col("productid")],
            [col("productid")],
            JoinType::Left.into(),
        )
        .join(
            df_employees,
            [col("salespersonid")],
            [col("employeeid")],
            JoinType::Left.into(),
        )
        .select(&[
            col("orderid").alias("order_id"),
            col("sales"),
            col("firstname").alias("customer_firstname"),
            col("lastname").alias("customer_lastname"),
            col("product"),
            col("price"),
            col("firstname").alias("employee_firstname"),
            col("lastname").alias("employee_lastname"),
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
