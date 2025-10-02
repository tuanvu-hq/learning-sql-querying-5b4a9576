use polars::prelude::*;
use polars::{frame::DataFrame, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};
use sqlx::Pool;

use lib_core::connection::{get_db_sea_orm, get_db_sqlx};
use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

/*
# QUERY:

SELECT * FROM customers;
*/

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<customers::Model>> {
    customers::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)
}

async fn sqlx_query(db: &Pool<sqlx::Postgres>) -> AppResult<Vec<customers::Model>> {
    let query = "SELECT * FROM customers;";

    sqlx::query_as::<_, customers::Model>(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)
}

async fn polars_df(db: &DatabaseConnection) -> AppResult<DataFrame> {
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
        let id = a.id == b.id;
        let first_name = a.first_name == b.first_name;
        let country = a.country == b.country;
        let score = a.score == b.score;

        id && first_name && country && score
    });

    if length && comparison {
        println!("POLARS: ");
        println!("\n{}\n", df);
    }

    Ok(())
}

/*
shape: (5, 4)
┌─────┬────────────┬─────────┬───────┐
│ id  ┆ first_name ┆ country ┆ score │
│ --- ┆ ---        ┆ ---     ┆ ---   │
│ i32 ┆ str        ┆ str     ┆ i32   │
╞═════╪════════════╪═════════╪═══════╡
│ 1   ┆ Maria      ┆ Germany ┆ 350   │
│ 2   ┆  John      ┆ USA     ┆ 900   │
│ 3   ┆ Georg      ┆ UK      ┆ 750   │
│ 4   ┆ Martin     ┆ Germany ┆ 500   │
│ 5   ┆ Peter      ┆ USA     ┆ 0     │
└─────┴────────────┴─────────┴───────┘
*/
