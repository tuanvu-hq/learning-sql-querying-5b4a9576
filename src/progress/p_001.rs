use polars::prelude::*;
use polars::{error::PolarsError, frame::DataFrame, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::{Pool, Postgres};

use crate::database::customers;

pub async fn sea_orm_query(
    db: &DatabaseConnection,
) -> Result<Vec<customers::Model>, sea_orm::DbErr> {
    customers::Entity::find()
        .all(db)
        .await
        .map_err(|error| error)
}

pub async fn sqlx_query(db: &Pool<Postgres>) -> Result<Vec<customers::Model>, sqlx::Error> {
    sqlx::query_as::<_, customers::Model>("SELECT * FROM customers;")
        .fetch_all(db)
        .await
        .map_err(|error| error)
}

pub fn display_as_polars_table(data: &[customers::Model]) -> Result<(), PolarsError> {
    let ids: Vec<i32> = data.iter().map(|c| c.id).collect();
    let first_names: Vec<String> = data.iter().map(|c| c.first_name.clone()).collect();
    let countries: Vec<Option<String>> = data.iter().map(|c| c.country.clone()).collect();
    let scores: Vec<Option<i32>> = data.iter().map(|c| c.score).collect();

    let df = DataFrame::new(vec![
        Series::new("id".into(), ids).into(),
        Series::new("first_name".into(), first_names).into(),
        Series::new("country".into(), countries).into(),
        Series::new("score".into(), scores).into(),
    ])
    .map_err(|error| error)?;

    println!("{}", df);

    Ok(())
}
