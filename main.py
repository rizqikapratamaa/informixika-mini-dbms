import socket
import sys
import threading


def handle_client(conn, addr):
    print(f"Connected to {addr}")
    try:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            print(f"Received from {addr}: {data}")
            response = f"Processed: {data}"
            conn.send(response.encode())
    except Exception as e:
        print(f"Error with {addr}: {e}")
    finally:
        print(f"Closing connection to {addr}")
        conn.close()


def start_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("127.0.0.1", port))
    server_socket.listen(8)
    print(f"Server listening on port {port}")

    try:
        while True:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        server_socket.close()


def start_client(port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect(("127.0.0.1", port))
        print(f"Connected to server on port {port}")

        while True:
            message = input("Informixika# ")
            if message.lower() == "exit":
                break
            client_socket.send(message.encode())
            response = client_socket.recv(1024).decode()
            print(f"{response}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()


def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <server|client> <port>")
        sys.exit(1)

    mode = sys.argv[1]
    port = int(sys.argv[2])

    if mode == "server":
        start_server(port)
    elif mode == "client":
        start_client(port)
    else:
        print("Invalid mode. Use 'server' or 'client'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
