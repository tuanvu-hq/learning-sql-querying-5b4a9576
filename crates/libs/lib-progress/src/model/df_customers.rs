use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

pub async fn df_customers(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = customers::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let ids: Vec<i32> = iter.clone().map(|c| c.id).collect();
    let first_names: Vec<String> = iter.clone().map(|c| c.first_name.clone()).collect();
    let countries: Vec<Option<String>> = iter.clone().map(|c| c.country.clone()).collect();
    let scores: Vec<Option<i32>> = iter.clone().map(|c| c.score).collect();

    let df = DataFrame::new(vec![
        Series::new("id".into(), ids).into(),
        Series::new("first_name".into(), first_names).into(),
        Series::new("country".into(), countries).into(),
        Series::new("score".into(), scores).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}
