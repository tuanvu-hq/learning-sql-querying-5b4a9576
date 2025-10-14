use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::products;

pub async fn get_df_products(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = products::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let productids: Vec<i32> = iter.clone().map(|p| p.productid).collect();
    let products: Vec<Option<String>> = iter.clone().map(|p| p.product.clone()).collect();
    let categories: Vec<Option<String>> = iter.clone().map(|p| p.category.clone()).collect();
    let prices: Vec<Option<i32>> = iter.clone().map(|p| p.price).collect();

    let df = DataFrame::new(vec![
        Series::new("productid".into(), productids).into(),
        Series::new("product".into(), products).into(),
        Series::new("category".into(), categories).into(),
        Series::new("price".into(), prices).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}
