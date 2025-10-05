use std::error::Error;

use lib_progress::progress::p_022::display_table;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;

    display_table().await?;

    Ok(())
}
