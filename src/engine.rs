use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
// Engine represents the storage engine for our dumb DB.
// This is the thing that handles actual reads and writes to disk

enum EngineFileOrientation {
    Log,
    Page,
}

// Trait represents the general functionality of a dumbdb Storage Engine
pub trait Engine {
    fn read(&self, key: String) -> Result<Option<String>, ()>;
    fn db_write(&self, key: String, val: String) -> Result<Option<String>, ()>;
    fn shutdown(&self) -> Result<Option<String>, ()>; // used on app quit or signal handling
}

// shared config between Engine implementations
pub struct EngineConfig {
    db_directory: String,
    // files: Option<Vec<File>>,
    file: Option<File>, // TODO: eventually allow for multiple files?
    orientation: EngineFileOrientation, // TODO: do we actually need this?
}

pub struct NaiveEngine {
    config: EngineConfig,
}

impl Engine for NaiveEngine {
    fn read(&self, key: String) -> Result<Option<String>, ()> {
        // open directory/DB file, scan through the file in reverse order, search for key
        let log_file_path = self.config.db_directory.clone() + "/db.log";
        let mut log_file_lines = String::new();
        let mut log_file = OpenOptions::new()
            .write(true) // needed to be able to create the file if it doesn't exist
            .create(true)
            .read(true)
            .open(Path::new(log_file_path.as_str()))
            .expect("Expected file open.");

        log_file
            .read_to_string(&mut log_file_lines)
            .expect("error reading file");

        let mut val: String = String::from("");

        for line in log_file_lines.lines().rev() {
            let collected_lines: Vec<&str> = line.split(",").collect();
            match collected_lines.as_slice() {
                [fkey, fval] => {
                    if *fkey == key.as_str() {
                        val = String::from(*fval);
                        break; // break early as we just want to find the most recent entry
                    } else {
                        ()
                    }
                }
                _ => {
                    ();
                }
            };
        }
        if val.len() > 0 {
            return Ok(Some(val));
        } else {
            return Ok(None);
        }
    }

    fn db_write(&self, key: String, val: String) -> Result<Option<String>, ()> {
        // open directory/DB file (create if doesn't exist)

        let log_file_path = self.config.db_directory.clone() + "/db.log";

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path.as_str())
            .expect("Expected file to open.");

        let new_entry = format!("{key},{val}\n");

        let _ = log_file.write(new_entry.as_bytes());

        return Ok(None);
    }

    fn shutdown(&self) -> Result<Option<String>, ()> {
        return Ok(None);
    }
}

impl NaiveEngine {
    pub fn new() -> NaiveEngine {
        let config = EngineConfig {
            db_directory: String::from("./dumbdb/data/"),
            orientation: EngineFileOrientation::Log,
            // files: None,
            file: None,
        };
        // TODO: make this write to a proper directory at some point
        let path = Path::new(config.db_directory.as_str());
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
