use std::net::{TcpStream};
use std::io::{self, Write, BufRead, BufReader};
use std::env;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let server_addr = "127.0.0.1:" + args[1].clone();
    println!("connecting to server at {}", server_addr);

    match TcpStream::connect(server_addr) {
        Ok(mut stream) => {
            println!("conencted!");

            let stdin = io::stdin();
            let mut input = String::new();

            loop {
                print!("? ");
                io::stdout().flush().unwrap();
                input.clear();
                
                if let Err(e) = stdin.read_line(&mut input) {
                    eprintln!("ERR reading input: {}", e);
                    continue;
                }

                let trimmed = input.trim();

                if trimmed.eq_ignore_ascii_case("quit") {
                    break;
                }

                if let Err(e) = stream.write_all(trimmed.as_bytes()).and_then(|_| stream.write_all(b"\n")) {
                    eprintln!("ERR sending message to server: {}", e);
                    continue;
                }

                let mut reader = BufReader::new(&stream);
                let mut response = String::new();
                match reader.read_line(&mut response) {
                    Ok(_) => {
                        if response.is_empty() {
                            println!("no response");
                        } else {
                            println!("crystal_methamphetamine> {}", response.trim());
                        }
                    }
                    Err(e) => {
                        eprintln!("ERR reading response from server: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("ERR connecting to server: {}", e);
        }
    }
}
