use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use log::info;
// Engine represents the storage engine for our dumb DB.
// This is the thing that handles actual reads and writes to disk

enum EngineFileOrientation {
    Log,
    Page,
}

// Trait represents the general functionality of a dumbdb Storage Engine
pub trait Engine {
    // TODO: figure out better return types
    fn db_read(&self, key: String) -> Result<Option<String>, ()>;
    fn db_write(&mut self, key: String, val: String) -> Result<Option<String>, ()>;
    fn shutdown(&self) -> Result<Option<String>, ()>; // used on app quit or signal handling
}

// shared config between Engine implementations
pub struct EngineConfig {
    db_directory: String,
    // files: Option<Vec<File>>,
    file: Option<File>, // TODO: eventually allow for multiple files?
    orientation: EngineFileOrientation, // TODO: do we actually need this?
}

impl EngineConfig {
    pub fn get_standard_config() -> EngineConfig {
        return EngineConfig {
            db_directory: String::from("./dumbdb/data/"),
            orientation: EngineFileOrientation::Log,
            // files: None,
            file: None,
        };
    }
}

pub struct NaiveEngine {
    config: EngineConfig,
}

impl Engine for NaiveEngine {
    fn db_read(&self, key: String) -> Result<Option<String>, ()> {
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

    fn db_write(&mut self, key: String, val: String) -> Result<Option<String>, ()> {
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
        let config = EngineConfig::get_standard_config();
        // TODO: make this write to a proper directory at some point
        let path = Path::new(config.db_directory.as_str());
        create_dir_all(path).expect("Unable to create data directory for dumbdb");

        return NaiveEngine { config };
    }
}
pub struct NaiveWithHashIndexEngine {
    config: EngineConfig,
    index: HashMap<String, u64>, // stores the byte offset of keys in the underlying db file
}

impl NaiveWithHashIndexEngine {
    pub fn new() -> NaiveWithHashIndexEngine {
        let config = EngineConfig::get_standard_config();
        // TODO: make this write to a proper directory at some point
        let path = Path::new(config.db_directory.as_str());
        create_dir_all(path).expect("Unable to create data directory for dumbdb");
        let mut index = HashMap::new();

        return NaiveWithHashIndexEngine { config, index };
    }
}

impl Engine for NaiveWithHashIndexEngine {
    fn db_read(&self, key: String) -> Result<Option<String>, ()> {
        match self.index.get(&key) {
            Some(&byte_offset) => {
                // TODO: refactor this stuff
                let mut log_file_str_buf: String = String::new();
                let log_file_path = self.config.db_directory.clone() + "/db.log";
                let mut log_file = OpenOptions::new()
                    .write(true) // needed to be able to create the file if it doesn't exist
                    .create(true)
                    .read(true)
                    .open(Path::new(log_file_path.as_str()))
                    .expect("Expected file open.");

                log_file.seek(SeekFrom::Start(byte_offset));
                log_file
                    .read_to_string(&mut log_file_str_buf)
                    .expect("To be able to read from file");

                let found_line = log_file_str_buf.lines().next().unwrap(); // TODO: actually match this
                info!("Parsing line: {}", found_line);
                let mut line_tokens = found_line.split(",");

                // if somehow the key doesn't match, raise an Error
                if line_tokens.next().unwrap() != key.as_str() {
                    return Err(());
                }
                let val = String::from(line_tokens.next().unwrap());
                return Ok(Some(val));
            }
            None => {
                // TODO: if we encounter a hash miss, read the DB and try and find it
                // (or maybe not. Let's think more about db state before that);
                return Ok(None);
            }
        }
    }

    fn db_write(&mut self, key: String, val: String) -> Result<Option<String>, ()> {
        let log_file_path = self.config.db_directory.clone() + "/db.log";
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path.as_str())
            .expect("Expected file to open.");

        let new_entry = format!("{key},{val}\n");
        let existing_file_size = log_file.metadata().unwrap().len();

        let _ = log_file.write(new_entry.as_bytes());
        self.index.insert(key, existing_file_size);
        println!("{:?}", self.index);
        return Ok(None);
    }

    fn shutdown(&self) -> Result<Option<String>, ()> {
        return Ok(None);
    }
}

pub fn create_engine_from_string(engine_type: String) -> Option<Box<dyn Engine>> {
    match engine_type.to_ascii_lowercase().as_str() {
        "naive" => Some(Box::new(NaiveEngine::new())),
        "naive_index" => Some(Box::new(NaiveWithHashIndexEngine::new())),
        _ => None,
    }
}
