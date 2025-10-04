use lib_core::{
    connection::{get_db_sea_orm, get_db_sqlx},
    error::AppResult,
};
use sea_orm::DatabaseConnection;
use sqlx::{Pool, Postgres};

pub async fn get_database() -> AppResult<(DatabaseConnection, Pool<Postgres>)> {
    let db_sea_orm = get_db_sea_orm().await?;
    let db_sqlx = get_db_sqlx().await?;

    Ok((db_sea_orm, db_sqlx))
}
