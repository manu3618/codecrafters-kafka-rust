#![allow(unused_imports)]
use anyhow::Result;
use std::io::{Read, Write};
use std::net::TcpListener;

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buff = [0; 128];
                let _ = stream.read(&mut buff)?;
                let m = Message::from_bytes(&buff);
                stream.write_all(&m.get_answer().to_bytes())?;
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
    request_api_key: i16,         // The API key for the request
    request_api_version: i16,     // The version of the API for the request
    correlation_id: i32,          // A unique identifier for the request
    client_id: String,            // The client ID for the request
    tag_array: CompactBuffer<u8>, // Optional tagged fields
}

#[derive(Default, Clone, Debug)]
struct CompactBuffer<T> {
    array: Vec<T>,
}

#[derive(Default, Clone, Debug)]
struct ProduceBody {}

impl Message {
    fn from_bytes(input: &[u8]) -> Self {
        Self {
            message_size: i32::from_be_bytes(input[0..4].try_into().unwrap()),
            header: MessageHeader {
                request_api_key: i16::from_be_bytes(input[4..6].try_into().unwrap()),
                request_api_version: i16::from_be_bytes(input[6..8].try_into().unwrap()),
                correlation_id: i32::from_be_bytes(input[8..12].try_into().unwrap()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn to_bytes(self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&self.message_size.to_be_bytes());
        m.extend_from_slice(&self.header.to_bytes());
        m.extend_from_slice(&self.body);
        m
    }

    fn get_answer(self) -> Self {
        let mut resp = self.clone();
        match self.header.request_api_key {
            18 => self.handle_api_versions(&mut resp),
            0..=81 => {
                //valid api request_key
                unimplemented!()
            }
            _ => {}
        }
        resp
    }

    fn handle_api_versions(self, resp: &mut Self) {
        match self.header.request_api_version {
            _ => resp.body = 35_i16.to_be_bytes().into(), // unsupported api version
        }
    }
}

impl MessageHeader {
    fn to_bytes(self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&self.correlation_id.to_be_bytes());
        m
    }
}

impl<T> CompactBuffer<T> {
    fn to_bytes(self) -> Vec<u8> {
        todo!()
    }
}

impl ProduceBody {}
