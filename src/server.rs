// Server is the file/type that sits between incoming requests and the DB/Engine itself
// Server is responsible for owning the "event loop" and passing commands to the Engine
// and then outputting the results
use log::info;
use std::io;

use crate::engine;

pub fn run(engine_type: String) -> Result<(), ()> {
    info!("Starting Dumb DB Server.");
    info!("Listening on port NONE.");
    println!("Welcome to DumbDB!");

    // TODO: maybe turn this to compiler flags if Rust supports that?
    let mut db_engine = engine::create_engine_from_string(engine_type)
        .expect("Engine type does not match any implementations");
    // Main loop
    // Eventually this will be a proper connection-handler
    loop {
        // TODO(?): beef up parsing
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Expected input but got none");

        let inp_tokens: Vec<&str> = input.trim().split(" ").collect();
        let _ = match inp_tokens.as_slice() {
            ["get", key] => {
                match db_engine.db_read(String::from(*key)) {
                    Ok(Some(val)) => println!("{}", val),
                    Ok(None) => println!("Did not find key"),
                    Err(()) => println!("There was an error when trying to read the DB"),
                };
            }
            ["put", key, val] => {
                db_engine.db_write(String::from(*key), String::from(*val));
            }
            ["quit"] | ["q"] => {
                println!("Breaking from the program.");
                let _ = db_engine.shutdown(); // gracefully shutdown (e.g write index, wrap up threads, etc.)
                break;
            }
            _ => continue,
        };
    }

    return Ok(());
}
