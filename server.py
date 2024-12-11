import socket
import sys
import json
import threading
from Query_Processor.classes.query_processor import QueryProcessor
from Query_Processor.classes.execution_result import ExecutionResult
from dataclasses import is_dataclass, asdict
from Failure_Recovery_Manager.classes.failure_recovery_manager import (
    Failure_recovery_manager,
)
from Utils.component_logger import log_socket


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
    log_socket(f"Connected to {addr}")
    try:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            log_socket(f"Received from {addr}: {data}")

            try:
                processor = QueryProcessor()
                data: list[ExecutionResult] = processor.execute_query(data)
                for result in data:
                    response = json.dumps(result, default=custom_json_serializer)
                    conn.send(response.encode())

            except Exception as e:
                conn.send(str(e).encode())
    except Exception as e:
        log_socket(f"Error with {addr}: {e}")
    finally:
        log_socket(f"Closing connection to {addr}")
        conn.close()


def start_server(port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("127.0.0.1", port))
    server_socket.listen(8)
    log_socket(f"Server listening on port {port}")

    try:
        while True:
            conn, addr = server_socket.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
    except KeyboardInterrupt:
        log_socket("Shutting down server...")
    finally:
        Failure_recovery_manager.enable().exit_routine()
        server_socket.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python server.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    start_server(port)


if __name__ == "__main__":
    main()
