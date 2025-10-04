use chrono::NaiveDate;
use lib_core::error::{AppError, AppResult};
use lib_data::database::orders;
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

pub async fn df_orders(db: &DatabaseConnection) -> AppResult<DataFrame> {
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
