use chrono::{NaiveDate, NaiveDateTime};
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::orders;

pub async fn get_df_orders(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = orders::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let orderids: Vec<i32> = iter.clone().map(|o| o.orderid).collect();
    let productids: Vec<Option<i32>> = iter.clone().map(|o| o.productid).collect();
    let customerids: Vec<Option<i32>> = iter.clone().map(|o| o.customerid).collect();
    let salespersonids: Vec<Option<i32>> = iter.clone().map(|o| o.salespersonid).collect();
    let orderdates: Vec<Option<NaiveDate>> = iter.clone().map(|o| o.orderdate).collect();
    let shipdates: Vec<Option<NaiveDate>> = iter.clone().map(|o| o.shipdate).collect();
    let orderstatuses: Vec<Option<String>> = iter.clone().map(|o| o.orderstatus.clone()).collect();
    let shipaddresses: Vec<Option<String>> = iter.clone().map(|o| o.shipaddress.clone()).collect();
    let billaddresses: Vec<Option<String>> = iter.clone().map(|o| o.billaddress.clone()).collect();
    let quantities: Vec<Option<i32>> = iter.clone().map(|o| o.quantity).collect();
    let sales: Vec<Option<i32>> = iter.clone().map(|o| o.sales).collect();
    let creationtimes: Vec<Option<NaiveDateTime>> = iter.clone().map(|o| o.creationtime).collect();

    let df = DataFrame::new(vec![
        Series::new("orderid".into(), orderids).into(),
        Series::new("productid".into(), productids).into(),
        Series::new("customerid".into(), customerids).into(),
        Series::new("salespersonid".into(), salespersonids).into(),
        Series::new("orderdate".into(), orderdates).into(),
        Series::new("shipdate".into(), shipdates).into(),
        Series::new("orderstatus".into(), orderstatuses).into(),
        Series::new("shipaddress".into(), shipaddresses).into(),
        Series::new("billaddress".into(), billaddresses).into(),
        Series::new("quantity".into(), quantities).into(),
        Series::new("sales".into(), sales).into(),
        Series::new("creationtime".into(), creationtimes).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}
