use std::net::{TcpStream};
use std::io::{self, Write, BufRead, BufReader};

fn main() {
    let server_addr = "127.0.0.1:6379";
    println!("Connecting to server at {}", server_addr);

    match TcpStream::connect(server_addr) {
        Ok(mut stream) => {
            println!("Successfully connected to server");

            let message = "hi\n";
            println!("Sending message: {}", message.trim());
            stream.write_all(message.as_bytes()).unwrap();

            let mut reader = BufReader::new(&stream);
            let mut response = String::new();
            reader.read_line(&mut response).unwrap();
            println!("Received response: {}", response.trim());
        }
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
        }
    }
}
