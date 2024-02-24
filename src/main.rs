use clap::Parser;

pub mod engine;
pub mod server;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    engine_type: Option<String>,
}

fn main() {
    let args = Args::parse();
    let engine_type = match args.engine_type {
        Some(engine_type) => engine_type,
        None => String::from("naive"),
    };
    let _ = server::run(engine_type);
}
