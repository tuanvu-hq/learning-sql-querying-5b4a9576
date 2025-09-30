use polars::error::PolarsError;
use thiserror::Error;

pub type AppResult<T> = core::result::Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("SeaORM error: {0}")]
    SeaOrm(#[from] sea_orm::DbErr),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Dynamic error: {0}")]
    Dynamic(#[from] Box<dyn std::error::Error>),
}
