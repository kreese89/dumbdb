use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
// Engine represents the storage engine for our dumb DB.
// This is the thing that handles actual reads and writes to disk

enum EngineFileOrientation {
    Log,
    Page,
}

// Trait represents the general functionality of a dumbdb Storage Engine
pub trait Engine {
    fn read(&self, key: String) -> Result<(), ()>;
    fn write(&self, key: String, val: String) -> Result<(), ()>;
    fn shutdown(&self) -> Result<(), ()>; // used on app quit or signal handling
}

// shared config between Engine implementations
pub struct EngineConfig {
    db_directory: PathBuf, // directory where things are written to
    files: Option<Vec<File>>,
    orientation: EngineFileOrientation, // TODO: do we actually need this?
}

pub struct NaiveEngine {
    config: EngineConfig,
}

impl Engine for NaiveEngine {
    fn read(&self, key: String) -> Result<(), ()> {
        return Ok(());
    }

    fn write(&self, key: String, val: String) -> Result<(), ()> {
        return Ok(());
    }

    fn shutdown(&self) -> Result<(), ()> {
        return Ok(());
    }
}

impl NaiveEngine {
    pub fn new() -> NaiveEngine {
        let config = EngineConfig {
            // db_directory: String::from("./dumbdb/data/"),
            db_directory: PathBuf::from("./dumbdb/data/"),
            orientation: EngineFileOrientation::Log,
            files: None,
        };
        let path = Path::new("./dumbdb/data/");
        create_dir_all(path).expect("Unable to create data directory for dumbdb");

        return NaiveEngine { config };
    }
}

pub fn create_engine_from_string(engine_type: String) -> Option<Box<dyn Engine>> {
    match engine_type.to_ascii_lowercase().as_str() {
        "naive" => Some(Box::new(NaiveEngine::new())),
        _ => None,
    }
}
