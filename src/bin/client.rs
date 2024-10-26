use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::io::stdin;

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_addr = "127.0.0.1:6379";
    println!("Connecting to server at {}", server_addr);

    // Connect to the server
    let stream = TcpStream::connect(server_addr).await?;
    println!("Connected!");

    // Split the stream into a read half and a write half
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // REPL loop
    loop {
        // Use a buffered reader for stdin
        let mut user_input = String::new();
        let mut stdin_reader = BufReader::new(stdin());

        // Prompt the user for input
        print!("? ");
        io::stdout().flush().await?; // Explicitly flush to ensure the prompt is displayed

        // Read the user's command asynchronously
        stdin_reader.read_line(&mut user_input).await?;

        // Trim and write the command to the server
        let command = user_input.trim().to_string();
        if command.is_empty() {
            continue;
        }

        println!("Sending command: {}", command); // Debugging info
        write_half.write_all(command.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        write_half.flush().await?; // Ensure the command is sent immediately
        println!("Command sent!");

        // Read the response from the server
        let mut response = String::new();
        match reader.read_line(&mut response).await {
            Ok(0) => {
                println!("Connection closed by server.");
                break;
            }
            Ok(_) => {
                println!("Server response: {}", response.trim());
            }
            Err(e) => {
                println!("Failed to read response: {}", e);
                break;
            }
        }
    }

    Ok(())
}
