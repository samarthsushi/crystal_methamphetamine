use std::env;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    // Get the port and message from the command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <port> <message>", args[0]);
        std::process::exit(1);
    }

    let port: u16 = args[1].parse().expect("Invalid port number");
    let message = &args[2..].join(" "); 

    let address = format!("127.0.0.1:{}", port);
    match TcpStream::connect(&address).await {
        Ok(mut stream) => {
            println!("Connected to {}", address);
            if let Err(e) = stream.write_all(format!("{}\n", message).as_bytes()).await {
                eprintln!("Failed to send message: {}", e);
            } else {
                println!("Message sent: {}", message);
            }
        }
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", address, e);
        }
    }
}
