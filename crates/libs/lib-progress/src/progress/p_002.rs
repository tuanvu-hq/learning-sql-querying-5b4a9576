use chrono::NaiveDate;
use polars::prelude::*;
use polars::{frame::DataFrame, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::connection::{get_db_sea_orm, get_db_sqlx};
use lib_core::error::{AppError, AppResult};
use lib_data::database::orders;

/*
# QUERY:

SELECT * FROM orders;
*/

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<orders::Model>> {
    orders::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<orders::Model>> {
    let query = "SELECT * FROM orders;";

    sqlx::query_as::<_, orders::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)
}

async fn polars_df(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = orders::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let order_ids: Vec<i32> = iter.clone().map(|c| c.order_id).collect();
    let customer_ids: Vec<i32> = iter.clone().map(|c| c.customer_id).collect();
    let order_dates: Vec<Option<NaiveDate>> = iter.clone().map(|c| c.order_date).collect();
    let sales: Vec<Option<i32>> = iter.clone().map(|c| c.sales).collect();

    let df = DataFrame::new(vec![
        Series::new("order_id".into(), order_ids).into(),
        Series::new("customer_id".into(), customer_ids).into(),
        Series::new("order_date".into(), order_dates).into(),
        Series::new("sales".into(), sales).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}

pub async fn display_table() -> AppResult<()> {
    let db_sea_orm = get_db_sea_orm().await?;
    let db_sqlx = get_db_sqlx().await?;

    let data_sea_orm = sea_orm_query(&db_sea_orm).await?;
    let data_sqlx = sqlx_query(&db_sqlx).await?;

    let df = polars_df(&db_sea_orm)
        .await?
        .clone()
        .lazy()
        .collect()
        .map_err(AppError::Polars)?;

    let length = data_sea_orm.len() == data_sqlx.len();
    let comparison = data_sea_orm.iter().zip(data_sqlx.iter()).all(|(a, b)| {
        let order_id = a.order_id == b.order_id;
        let customer_id = a.customer_id == b.customer_id;
        let order_date = a.order_date == b.order_date;
        let sales = a.sales == b.sales;

        order_id && customer_id && order_date && sales
    });

    if length && comparison {
        println!("POLARS: ");
        println!("\n{}\n", df);
    }

    Ok(())
}

/*
shape: (4, 4)
┌──────────┬─────────────┬────────────┬───────┐
│ order_id ┆ customer_id ┆ order_date ┆ sales │
│ ---      ┆ ---         ┆ ---        ┆ ---   │
│ i32      ┆ i32         ┆ date       ┆ i32   │
╞══════════╪═════════════╪════════════╪═══════╡
│ 1001     ┆ 1           ┆ 2021-01-11 ┆ 35    │
│ 1002     ┆ 2           ┆ 2021-04-05 ┆ 15    │
│ 1003     ┆ 3           ┆ 2021-06-18 ┆ 20    │
│ 1004     ┆ 6           ┆ 2021-08-31 ┆ 10    │
└──────────┴─────────────┴────────────┴───────┘
*/
