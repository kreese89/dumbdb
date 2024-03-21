use std::collections::HashMap;
use std::fs::{create_dir_all, remove_dir_all, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

// Engine represents the storage engine for our dumb DB.
// This is the thing that handles actual reads and writes to disk

// const MAX_LOG_FILE_SIZE: u64 = 160000000; // 160mb
const MAX_LOG_FILE_SIZE: u64 = 20;
const FILE_LIMIT: usize = 2;

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
}

impl EngineConfig {
    pub fn get_standard_config() -> EngineConfig {
        return EngineConfig {
            db_directory: String::from("./dumbdb/data"),
        };
    }
}

pub struct AppendOnlyLogEngine {
    config: EngineConfig,
    files: Vec<String>,
}

impl Engine for AppendOnlyLogEngine {
    fn db_read(&self, key: String) -> Result<Option<String>, ()> {
        // open directory/DB file, scan through the file in reverse order, search for key
        for filename in self.files.iter().rev() {
            let mut log_file_lines = String::new();
            let mut log_file = OpenOptions::new()
                .write(true) // needed to be able to create the file if it doesn't exist
                .create(true)
                .read(true)
                .open(Path::new(filename.as_str()))
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
            }
        }
        return Ok(None);
    }

    fn db_write(&mut self, key: String, val: String) -> Result<Option<String>, ()> {
        // open directory/DB file (create if doesn't exist)

        // let log_file_path = self.config.db_directory.clone() + "/db.log";
        let db_dir = &self.config.db_directory;
        let default_filename = format!("{db_dir}/db0.log");
        let log_file_path = self.files.last().unwrap_or(&default_filename); // just fetch the most recent file

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path.as_str())
            .expect("Expected file to open.");

        let new_entry = format!("{key},{val}\n");

        let existing_file_size = log_file.metadata().unwrap().len();
        if existing_file_size > MAX_LOG_FILE_SIZE {
            let files_ct = self.files.len();
            let new_filename = format!("{db_dir}/db{files_ct}.log");
            let mut new_log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(new_filename.as_str())
                .expect("Expected open and create new file");

            self.files.push(new_filename);
            let _ = new_log_file.write(new_entry.as_bytes());

            if files_ct >= FILE_LIMIT {
                let _ = self.compactify();
            }
        } else {
            let _ = log_file.write(new_entry.as_bytes());
        }

        Ok(None)
    }

    fn shutdown(&self) -> Result<Option<String>, ()> {
        return Ok(None);
    }
}

impl AppendOnlyLogEngine {
    pub fn new() -> AppendOnlyLogEngine {
        let config = EngineConfig::get_standard_config();
        // TODO: make this write to a proper directory at some point

        let db_dir = config.db_directory.clone();
        let path = Path::new(db_dir.as_str());
        create_dir_all(path).expect("Unable to create data directory for dumbdb");

        let mut files = Vec::new();

        let mut ctr = 0;
        let mut db_file_filename = format!("{db_dir}/db{ctr}.log");

        // initialize the files array for the multiple files
        while Path::new(db_file_filename.as_str()).exists() {
            files.push(db_file_filename.clone());
            ctr += 1;

            db_file_filename = format!("{db_dir}/db{ctr}.log");
        }

        return AppendOnlyLogEngine {
            config,
            files: files,
        };
    }

    pub fn compactify(&mut self) -> Result<(), ()> {
        // go through and clear old files, removing old keys as needed

        // how we'll do this: go through our files newest first
        // reach each file in reverse
        // keep hash map of the keys we've seen and their vals
        // if we see a new key, add it to the hash map

        // do this over all files
        // at the end we have a unique set of key,val pairs
        // write them to files, creating+writing new files if need be

        // TODO: figure out best way to preserve order of entries
        // look into IndexMap, BTreeMap

        // go through existing files, fetch each (key, val) pair
        let mut entries: HashMap<String, String> = HashMap::new();

        for filename in self.files.iter().rev() {
            let mut log_file_lines = String::new();
            let mut log_file = OpenOptions::new()
                .write(true) // needed to be able to create the file if it doesn't exist
                .create(true)
                .read(true)
                .open(Path::new(filename.as_str()))
                .expect("Expected file open.");

            log_file
                .read_to_string(&mut log_file_lines)
                .expect("error reading file");

            for line in log_file_lines.lines().rev() {
                let mut tokens = line.split(",");
                let key = String::from(tokens.next().unwrap());
                if !entries.contains_key(&key) {
                    let val = String::from(tokens.next().unwrap());
                    entries.insert(key, val);
                }
            }
        }
        // clear existing files
        let db_dir = &self.config.db_directory;
        remove_dir_all(&db_dir).unwrap();
        create_dir_all(&db_dir).unwrap();
        self.files.clear();

        let mut ctr = 0;
        let mut db_file_filename = format!("{db_dir}/db{ctr}.log");
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&db_file_filename)
            .expect("Expected file to open.");
        self.files.push(db_file_filename);

        let mut curr_file_size: usize = 0;

        for (key, val) in entries.iter() {
            let new_entry = format!("{key},{val}\n");
            if curr_file_size > MAX_LOG_FILE_SIZE.try_into().unwrap() {
                // TODO: refactor since this is ugly
                ctr += 1;
                db_file_filename = format!("{db_dir}/db{ctr}.log");

                log_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&db_file_filename)
                    .expect("Expected file to open.");
                self.files.push(db_file_filename);

                curr_file_size = 0;
            }
            log_file.write(new_entry.as_bytes()).unwrap();
            curr_file_size += new_entry.as_bytes().len();
        }
        Ok(())
    }
}
pub struct AppendOnlyLogWithHashIndexEngine {
    config: EngineConfig,
    index: Vec<HashMap<String, u64>>, // stores the byte offset of keys in the underlying db file
    files: Vec<String>,
}

impl AppendOnlyLogWithHashIndexEngine {
    pub fn new() -> AppendOnlyLogWithHashIndexEngine {
        let config = EngineConfig::get_standard_config();
        // TODO: make this write to a proper directory at some point
        let db_dir = config.db_directory.clone();
        let path = Path::new(db_dir.as_str());
        create_dir_all(path).expect("Unable to create data directory for dumbdb");

        let mut index = Vec::new();
        let mut files = Vec::new();

        let mut ctr = 0;
        let mut db_file_filename = format!("{db_dir}/db{ctr}.log");

        // initialize the index(es) for the multiple files
        while Path::new(db_file_filename.as_str()).exists() {
            files.push(db_file_filename.clone());
            let mut bytes: u64 = 0;
            let mut file_index: HashMap<String, u64> = HashMap::new();

            let mut log_file_lines = String::new();
            let mut log_file = OpenOptions::new()
                .read(true)
                .open(Path::new(db_file_filename.as_str()))
                .expect("Expected file open.");

            log_file
                .read_to_string(&mut log_file_lines)
                .expect("error reading file");

            log_file_lines.lines().for_each(|line| {
                let parsed_line: Vec<&str> = line.split(",").collect();
                match parsed_line.as_slice() {
                    [fkey, _] => {
                        file_index.insert(String::from(*fkey), bytes);
                        bytes += (line.len() as u64) + 1;
                    }
                    _ => {
                        ();
                    }
                }
            });

            index.push(file_index);

            ctr += 1;
            db_file_filename = format!("{db_dir}/db{ctr}.log");
        }

        return AppendOnlyLogWithHashIndexEngine {
            config,
            index,
            files,
        };
    }
}

impl Engine for AppendOnlyLogWithHashIndexEngine {
    fn db_read(&self, key: String) -> Result<Option<String>, ()> {
        // reverse since we push newer files/hashmaps to the end of the Vec
        for (i, ind) in self.index.iter().rev().enumerate() {
            match ind.get(&key) {
                Some(&byte_offset) => {
                    let db_dir = &self.config.db_directory;
                    let default_filename = format!("{db_dir}/db0.log");
                    // get the file for the corresponding index
                    let log_file_path = self
                        .files
                        .get(self.files.len() - i - 1)
                        .unwrap_or(&default_filename);
                    // TODO: refactor this stuff
                    let mut log_file_str_buf: String = String::new();
                    let mut log_file = OpenOptions::new()
                        .write(true) // needed to be able to create the file if it doesn't exist
                        .create(true)
                        .read(true)
                        .open(Path::new(log_file_path.as_str()))
                        .expect("Expected file open.");
                    log_file.seek(SeekFrom::Start(byte_offset)).unwrap();
                    log_file
                        .read_to_string(&mut log_file_str_buf)
                        .expect("To be able to read from file");

                    let found_line = log_file_str_buf.lines().next().unwrap(); // TODO: actually match this
                    let mut line_tokens = found_line.split(",");

                    // if somehow the key doesn't match, raise an Error
                    if line_tokens.next().unwrap() != key.as_str() {
                        return Err(());
                    }
                    let val = String::from(line_tokens.next().unwrap());
                    return Ok(Some(val));
                }
                None => {
                    continue; // just try the next index
                }
            }
        }
        return Ok(None);
    }

    fn db_write(&mut self, key: String, val: String) -> Result<Option<String>, ()> {
        let db_dir = &self.config.db_directory;
        let default_filename = format!("{db_dir}/db0.log");
        let log_file_path = self.files.last().unwrap_or(&default_filename); // just fetch the most recent file
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path.as_str())
            .expect("Expected file to open.");

        let new_entry = format!("{key},{val}\n");

        let existing_file_size = log_file.metadata().unwrap().len();
        if existing_file_size > MAX_LOG_FILE_SIZE {
            let files_ct = self.files.len();
            let new_filename = format!("{db_dir}/db{files_ct}.log");
            let mut new_log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(new_filename.as_str())
                .expect("Expected open and create new file");

            let new_file_size = new_log_file.metadata().unwrap().len();
            self.files.push(new_filename);
            let mut new_index = HashMap::new();
            let _ = new_log_file.write(new_entry.as_bytes());
            new_index.insert(key, new_file_size);
            self.index.push(new_index);
        } else {
            let _ = log_file.write(new_entry.as_bytes());
            match self.index.last_mut() {
                // in case the index doesn't yet exist
                Some(ind) => {
                    ind.insert(key, existing_file_size);
                }
                None => {
                    let mut ind = HashMap::new();
                    ind.insert(key, existing_file_size);
                    self.index.push(ind);
                }
            };
        }

        return Ok(None);
    }

    fn shutdown(&self) -> Result<Option<String>, ()> {
        return Ok(None);
    }
}

pub fn create_engine_from_string(engine_type: String) -> Option<Box<dyn Engine>> {
    match engine_type.to_ascii_lowercase().as_str() {
        "aol" => Some(Box::new(AppendOnlyLogEngine::new())),
        "aol_index" => Some(Box::new(AppendOnlyLogWithHashIndexEngine::new())),
        _ => None,
    }
}
