use std::{error::Error, time::Duration};

use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use sqlx::{Pool, postgres::PgPoolOptions};

pub async fn get_db_sea_orm() -> Result<DatabaseConnection, Box<dyn Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let mut db_options = ConnectOptions::new(database_url.clone());
    db_options
        .min_connections(1)
        .max_connections(5)
        .connect_timeout(Duration::from_secs(8));
    let db = Database::connect(db_options).await?;

    Ok(db)
}

pub async fn get_db_sqlx() -> Result<Pool<sqlx::Postgres>, Box<dyn Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    Ok(db)
}
