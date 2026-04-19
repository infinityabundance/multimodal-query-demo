use anyhow::Result;
use mqd_core::non_claims;

pub async fn run() -> Result<()> {
    print!("{}", non_claims::print_numbered());
    Ok(())
}
