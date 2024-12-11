import socket
import sys
import json
import threading
from Query_Processor.classes.query_processor import QueryProcessor, ExecutionResult
from Storage_Manager.classes.rows import Rows
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


def receive_full_message(client_socket):
    """Receive the full message from the server"""
    full_message = ""
    while True:
        chunk = client_socket.recv(1024).decode()
        full_message += chunk

        if len(chunk) < 1024:
            break

    return full_message


def format_table(data):
    if not data:
        return "No data to display"

    columns = list(data[0].keys())

    col_widths = {
        col: max(len(str(col)), max(len(str(row.get(col, ""))) for row in data))
        for col in columns
    }

    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    separator = "-" * len(header)

    formatted_rows = []
    for row in data:
        formatted_row = " | ".join(
            str(row.get(col, "")).ljust(col_widths[col]) for col in columns
        )
        formatted_rows.append(formatted_row)

    return "\n".join([header, separator] + formatted_rows)


def reconstruct_execution_results(item):
    if "data" in item and item["data"]:
        item["data"] = Rows(
            data=item["data"]["data"], rows_count=item["data"]["rows_count"]
        )

    return ExecutionResult(**item)


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
            try:
                response = receive_full_message(client_socket)

                if not response.strip():
                    print("No data received from server.")
                    continue

                try:
                    data = json.loads(response)
                    execution_results = reconstruct_execution_results(data)

                    if execution_results.data:
                        print(format_table(execution_results.data.data))
                        print(f"Rows: {execution_results.data.rows_count}")

                except (ValueError, SyntaxError) as parse_error:
                    print(f"Error parsing server response: {parse_error}")
                    print(f"Received response: {response}")

            except Exception as receive_error:
                print(f"Error receiving message: {receive_error}")

    except ConnectionRefusedError:
        print(f"Could not connect to server on port {port}. Is the server running?")
    except socket.error as socket_error:
        print(f"Socket error: {socket_error}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        client_socket.close()
        print("Connection closed.")


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
