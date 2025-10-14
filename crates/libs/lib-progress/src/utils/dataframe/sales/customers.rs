use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::customers;

pub async fn get_df_customers(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = customers::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let customerids: Vec<i32> = iter.clone().map(|c| c.customerid).collect();
    let firstnames: Vec<Option<String>> = iter.clone().map(|c| c.firstname.clone()).collect();
    let lastnames: Vec<Option<String>> = iter.clone().map(|c| c.lastname.clone()).collect();
    let countries: Vec<Option<String>> = iter.clone().map(|c| c.country.clone()).collect();
    let scores: Vec<Option<i32>> = iter.clone().map(|c| c.score).collect();

    let df = DataFrame::new(vec![
        Series::new("customerid".into(), customerids).into(),
        Series::new("firstname".into(), firstnames).into(),
        Series::new("lastname".into(), lastnames).into(),
        Series::new("country".into(), countries).into(),
        Series::new("score".into(), scores).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}
