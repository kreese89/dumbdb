use std::str::FromStr;

// Engine represents the storage engine for our dumb DB.
// This is the thing that handles actual reads and writes to the underlying disk
enum EngineType {
    Naive,
}

impl FromStr for EngineType {
    fn from_str(input: &str) -> Result<EngineType, ()> {
        match input.to_lowercase() {
            "naive" => Ok(EngineType::Naive()),
            _ => Err(()),
        }
    }
}

struct Engine {
    // represents the "type" of engine we are using.
    // This is so we can switch between different implementations easily
    // TODO: look into refactoring this so that each engine type is its own 
    // data type but they all implement some shared functionality
    // and we can just use them generically
    engine_type: EngineType,
}