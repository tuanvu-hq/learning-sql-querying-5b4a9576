use chrono::NaiveDate;
use polars::{frame::DataFrame, prelude::NamedFrom, series::Series};
use sea_orm::{DatabaseConnection, EntityTrait};

use lib_core::error::{AppError, AppResult};
use lib_data::database_sales::employees;

pub async fn get_df_employees(db: &DatabaseConnection) -> AppResult<DataFrame> {
    let data = employees::Entity::find()
        .all(db)
        .await
        .map_err(AppError::SeaOrm)?;

    let iter = data.iter();
    let employeeids: Vec<i32> = iter.clone().map(|e| e.employeeid).collect();
    let firstnames: Vec<Option<String>> = iter.clone().map(|e| e.firstname.clone()).collect();
    let lastnames: Vec<Option<String>> = iter.clone().map(|e| e.lastname.clone()).collect();
    let departments: Vec<Option<String>> = iter.clone().map(|e| e.department.clone()).collect();
    let birthdates: Vec<Option<NaiveDate>> = iter.clone().map(|e| e.birthdate).collect();
    let genders: Vec<Option<String>> = iter.clone().map(|e| e.gender.clone()).collect();
    let salaries: Vec<Option<i32>> = iter.clone().map(|e| e.salary).collect();
    let managerids: Vec<Option<i32>> = iter.clone().map(|e| e.managerid).collect();

    let df = DataFrame::new(vec![
        Series::new("employeeid".into(), employeeids).into(),
        Series::new("firstname".into(), firstnames).into(),
        Series::new("lastname".into(), lastnames).into(),
        Series::new("department".into(), departments).into(),
        Series::new("birthdate".into(), birthdates).into(),
        Series::new("gender".into(), genders).into(),
        Series::new("salary".into(), salaries).into(),
        Series::new("managerid".into(), managerids).into(),
    ])
    .map_err(AppError::Polars)?;

    Ok(df)
}
