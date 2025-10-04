pub fn log_debug<T: std::fmt::Debug>(title: &str, data: &T, use_debug: Option<bool>) {
    match use_debug {
        Some(false) => {}
        Some(true) => {
            println!("{}:", title);
            println!("\n{:#?}\n", data)
        }
        None => {
            println!("{}:", title);
            println!("\n{:?}\n", data)
        }
    }
}
