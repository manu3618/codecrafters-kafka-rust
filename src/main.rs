#![allow(unused_imports)]
use anyhow::Result;
use std::io::Write;
use std::net::TcpListener;

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let m = Message {
                    message_size: 0,
                    header: MessageHeader { correlation_id: 7 },
                    body: Vec::new(),
                };
                stream.write_all(&m.to_bytes())?;
                stream.flush()?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

#[derive(Default, Clone, Debug)]
struct Message {
    message_size: i32,
    header: MessageHeader,
    body: Vec<u8>,
}

#[derive(Default, Clone, Debug)]
struct MessageHeader {
    correlation_id: i32,
}

impl Message {
    fn to_bytes(self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&self.message_size.to_be_bytes());
        m.extend_from_slice(&self.header.to_bytes());
        m.extend_from_slice(&self.body);
        m
    }
}

impl MessageHeader {
    fn to_bytes(self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&self.correlation_id.to_be_bytes());
        m
    }
}
