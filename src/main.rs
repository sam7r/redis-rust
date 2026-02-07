#![allow(unused_imports)]
use std::{io::Read, io::Write, net::TcpListener, thread};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                loop {
                    let mut buffer = [0; 512];
                    let _ = stream.read(&mut buffer);
                    let _ = stream.write(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}
