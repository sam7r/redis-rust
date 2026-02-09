use super::DataStore;
use std::{sync, time};

pub enum CleanupType {
    Scheduled(time::Duration), // Cleanup every n milliseconds
}

pub struct Governor {
    datastore: sync::Arc<DataStore>,
    cleanup_type: CleanupType,
}

pub struct Options {
    pub cleanup_type: CleanupType,
}

impl Governor {
    pub fn new(datastore: sync::Arc<DataStore>, options: Options) -> Self {
        Governor {
            datastore,
            cleanup_type: options.cleanup_type,
        }
    }

    pub fn start(self) {
        match self.cleanup_type {
            CleanupType::Scheduled(interval) => {
                std::thread::spawn(move || {
                    loop {
                        std::thread::sleep(interval);
                        self.datastore.cleanup();
                    }
                });
            }
        }
    }
}
