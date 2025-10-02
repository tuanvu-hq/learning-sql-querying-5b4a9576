use std::collections::HashMap;

use polars::prelude::*;
use polars::{frame::DataFrame, series::Series};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};

use lib_core::connection::{get_db_sea_orm, get_db_sqlx};
use lib_core::error::{AppError, AppResult};
use lib_data::database::customers;

/*
# QUERY:

SELECT first_name, country
FROM customers
WHERE country = 'Germany';

*/

const DEBUG: bool = false;

async fn sea_orm_query(db: &DatabaseConnection) -> AppResult<Vec<HashMap<String, Value>>> {
    let rows = customers::Entity::find()
        .select_only()
        .column(customers::Column::FirstName)
        .column(customers::Column::Country)
        .filter(customers::Column::Country.eq("Germany"))
        .into_json()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;
    let results = rows
        .into_iter()
        .map(|json| serde_json::from_value::<HashMap<String, Value>>(json).unwrap())
        .collect();

    if DEBUG {
        println!("SEA ORM: ");
        println!("\n{:#?}\n", results);
    }

    Ok(results)
}

async fn sqlx_query(db: &Pool<Postgres>) -> AppResult<Vec<HashMap<String, Value>>> {
    let query = "
    SELECT first_name, country 
    FROM customers 
    WHERE country = 'Germany';
    ";
    let rows = sqlx::query(query)
        .fetch_all(db)
        .await
        .map_err(AppError::Sqlx)?;
    #[rustfmt::skip]
    let results = rows
        .into_iter()
        .map(|row| {
            let mut map = HashMap::new();
            
            map.insert("first_name".to_string(), Value::String(row.get(0)));
            map.insert("country".to_string(), Value::String(row.get(1)));

            map
        })
        .collect();

    if DEBUG {
        println!("SQLX: ");
        println!("\n{:#?}\n", results);
    }

    Ok(results)
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
        .lazy()
        .select([col("first_name"), col("country")])
        .filter(col("country").eq(lit("Germany")))
        .collect()
        .map_err(AppError::Polars)?;

    let length = data_sea_orm.len() == data_sqlx.len();
    let comparison = data_sea_orm.iter().zip(data_sqlx.iter()).all(|(a, b)| {
        let first_name = a.get("first_name") == b.get("first_name");
        let country = a.get("country") == b.get("country");

        first_name && country
    });

    if length && comparison {
        println!("POLARS: ");
        println!("\n{}\n", df);
    }

    Ok(())
}

/*
shape: (2, 2)
┌────────────┬─────────┐
│ first_name ┆ country │
│ ---        ┆ ---     │
│ str        ┆ str     │
╞════════════╪═════════╡
│ Maria      ┆ Germany │
│ Martin     ┆ Germany │
└────────────┴─────────┘
*/
