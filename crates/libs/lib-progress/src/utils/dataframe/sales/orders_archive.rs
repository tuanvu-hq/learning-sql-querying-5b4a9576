use chrono::{NaiveDate, NaiveDateTime};
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::ordersarchive;

pub async fn get_df_ordersarchive(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = ordersarchive::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let archiveids: Vec<i32> = iter.clone().map(|c| c.archiveid).collect();
    let orderids: Vec<Option<i32>> = iter.clone().map(|c| c.orderid).collect();
    let productids: Vec<Option<i32>> = iter.clone().map(|c| c.productid).collect();
    let customerids: Vec<Option<i32>> = iter.clone().map(|c| c.customerid).collect();
    let salespersonids: Vec<Option<i32>> = iter.clone().map(|c| c.salespersonid).collect();
    let orderdates: Vec<Option<NaiveDate>> = iter.clone().map(|c| c.orderdate).collect();
    let shipdates: Vec<Option<NaiveDate>> = iter.clone().map(|c| c.shipdate).collect();
    let orderstatuses: Vec<Option<String>> = iter.clone().map(|c| c.orderstatus.clone()).collect();
    let shipaddresses: Vec<Option<String>> = iter.clone().map(|c| c.shipaddress.clone()).collect();
    let billaddresses: Vec<Option<String>> = iter.clone().map(|c| c.billaddress.clone()).collect();
    let quantities: Vec<Option<i32>> = iter.clone().map(|c| c.quantity).collect();
    let sales: Vec<Option<i32>> = iter.clone().map(|c| c.sales).collect();
    let creationtimes: Vec<Option<NaiveDateTime>> = iter.clone().map(|c| c.creationtime).collect();

    let df = DataFrame::new(vec![
        Series::new("archiveid".into(), archiveids).into(),
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
