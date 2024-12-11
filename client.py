import socket
import sys
import json


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

                    if "data" in data and data["data"]:
                        print(format_table(data["data"]["data"]))
                        print(f"Rows: {data['data']['rows_count']}")

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
    if len(sys.argv) < 2:
        print("Usage: python client.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    start_client(port)


if __name__ == "__main__":
    main()
