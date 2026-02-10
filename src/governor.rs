use super::DataStore;
use std::{sync::Arc, thread, time};

pub enum CleanupType {
    Scheduled(time::Duration),
}

pub struct Governor {
    datastore: Arc<DataStore>,
    cleanup_type: CleanupType,
}

pub struct Options {
    pub cleanup_type: CleanupType,
}

impl Governor {
    pub fn new(datastore: Arc<DataStore>, options: Options) -> Self {
        Governor {
            datastore,
            cleanup_type: options.cleanup_type,
        }
    }

    pub fn start(self) {
        match self.cleanup_type {
            CleanupType::Scheduled(interval) => {
                thread::spawn(move || {
                    loop {
                        thread::sleep(interval);
                        self.datastore.cleanup();
                    }
                });
            }
        }
    }
}
