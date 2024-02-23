// Server is the file/type that sits between incoming requests and the DB/Engine itself
// Server is responsible for owning the "event loop" and passing commands to the Engine
// and then outputting the results
use log::info;
use std::io;

pub fn run(engine_type: String) -> Result<(), ()> {
    info!("Starting Dumb DB Server.");
    info!("Listening on port NONE.");
    println!("Welcome to DumbDB!");
    loop {
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Expected input but got none");

        let inp_tokens: Vec<&str> = input.trim().split(" ").collect();
        match inp_tokens.as_slice() {
            ["get", key] => println!("getting val for {}", key),
            ["put", key, val] => println!("putting val {} for {}", val, key),
            ["quit"] | ["q"] => {
                println!("Breaking from the program.");
                break;
            },
            _ => continue,
        }
    }

    return Ok(());
}