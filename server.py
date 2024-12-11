import socket
import sys
import json
import threading
from Query_Processor.classes.query_processor import QueryProcessor, ExecutionResult
from dataclasses import is_dataclass, asdict


def custom_json_serializer(obj):
    if is_dataclass(obj):
        return asdict(obj)

    if hasattr(obj, "to_dict"):
        return obj.to_dict()

    if hasattr(obj, "__dict__"):
        return obj.__dict__

    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    try:
        return str(obj)
    except Exception:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def handle_client(conn, addr):
    print(f"Connected to {addr}")
    try:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            print(f"Received from {addr}: {data}")

            try:
                processor = QueryProcessor()
                data: list[ExecutionResult] = processor.execute_query(data)
                for result in data:
                    response = json.dumps(result, default=custom_json_serializer)
                    conn.send(response.encode())

            except Exception as e:
                conn.send(str(e).encode())
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


def main():
    if len(sys.argv) < 2:
        print("Usage: python server.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    start_server(port)


if __name__ == "__main__":
    main()
