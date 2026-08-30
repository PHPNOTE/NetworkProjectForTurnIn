"""
CRTP Interactive CLI Client (client.py)
=======================================
Network Socket Programming Project - Computer Networks

This module implements the client side of the Chat & Resource Transfer Protocol (CRTP/1.0).

Key Responsibilities:
1. TCP Socket Connection:
    - Establishes connection to CRTP Server (127.0.0.1:8888).

2. Asynchronous Receive Loop (Background Receiver Thread):
    - Runs a dedicated background daemon thread (`receive_loop`) to listen for incoming
        CRTP Response and Broadcast messages from the server without blocking CLI input.

3. User Interface (Command Line Interface - CLI):
   - Provides slash commands (`/auth`, `/rooms`, `/create`, `/join`, `/leave`,
     `/msg`, `/users`, `/file`, `/download`, `/help`, `/quit`).
   - Translates CLI commands into formatted CRTP Request objects.

4. Client-Side Protocol Logging:
   - Displays color-coded terminal logs for `[SEND REQUEST]` and `[RECV RESPONSE]`
     showing exact Status Codes (e.g. 200 OK, 401 UNAUTHORIZED, 404 NOT_FOUND).

"""

import socket
import threading
import json
import os
import sys
import base64
from protocol import CRTPMessage, StatusCode, log_formatted, COLORS

# Default Server Connection Settings
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8888


class CRTPClient:
    """
    CRTP Client Application Class.
    Manages socket connection, user session state, sending requests,
    and handling asynchronous server broadcasts.
    """
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        """
        Initialize client with server IP and Port.

        Args:
            host (str): Server IP address or hostname.
            port (int): Server TCP port.
        """
        self.host = host
        self.port = port
        self.sock = None
        self.session_id = None
        self.username = None
        self.current_room = None
        self.running = True

    def connect(self) -> bool:
        """
        Establish TCP Socket connection to the CRTP Server and spawn background receiver thread.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print(f"{COLORS['GREEN']}[+] Connected to CRTP Server at {self.host}:{self.port}{COLORS['ENDC']}")
            
            # Spawn background daemon thread to handle asynchronous incoming server data
            recv_thread = threading.Thread(target=self.receive_loop, daemon=True)
            recv_thread.start()
            return True
        except Exception as e:
            print(f"{COLORS['RED']}[!] Connection failed: {e}{COLORS['ENDC']}")
            return False

    def send_request(self, message: CRTPMessage):
        """
        Attach session header (if authenticated) and send CRTP message over TCP socket.

        Args:
            message (CRTPMessage): The request message to send.
        """
        if self.session_id:
            message.headers["session_id"] = self.session_id
        
        # Log outgoing request
        log_formatted("SEND_REQ", message)
        try:
            self.sock.sendall(message.to_bytes())
        except Exception as e:
            print(f"{COLORS['RED']}[!] Error sending message: {e}{COLORS['ENDC']}")

    def receive_loop(self):
        """
        Background Receiver Thread Routine.
        Continuously buffers bytes from socket, extracts line-delimited (`\n`) JSON strings,
        parses them into CRTPMessage objects, and routes them to `handle_incoming_message`.
        """
        buffer = ""
        while self.running:
            try:
                data = self.sock.recv(4096)
                if not data:
                    print(f"\n{COLORS['YELLOW']}[!] Server disconnected.{COLORS['ENDC']}")
                    self.running = False
                    break
                
                buffer += data.decode('utf-8')
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        msg = CRTPMessage.from_json(line)
                        self.handle_incoming_message(msg)
                    except Exception as e:
                        print(f"{COLORS['RED']}[!] Failed to parse incoming message: {e}{COLORS['ENDC']}")
            except Exception:
                break

    def handle_incoming_message(self, msg: CRTPMessage):
        """
        Handle and display incoming response or broadcast messages received from server.

        Args:
            msg (CRTPMessage): Parsed incoming message object.
        """
        payload = msg.payload
        
        # Check if incoming message is an Asynchronous Broadcast (CHAT, SYSTEM, FILE_ALERT)
        if "type" in payload:
            msg_type = payload.get("type")
            if msg_type == "CHAT":
                sender = payload.get("sender")
                text = payload.get("text")
                print(f"\n{COLORS['CYAN']}[{payload.get('room')}] {COLORS['BOLD']}{sender}{COLORS['ENDC']}: {text}")
            elif msg_type == "SYSTEM":
                print(f"\n{COLORS['YELLOW']}📢 SYSTEM: {payload.get('message')}{COLORS['ENDC']}")
            elif msg_type == "FILE_ALERT":
                sender = payload.get("sender")
                fname = payload.get("filename")
                size = payload.get("size_bytes")
                print(f"\n{COLORS['GREEN']}📁 FILE SHARED by {sender}: '{fname}' ({size} bytes). Use '/download {fname}' to get it.{COLORS['ENDC']}")
            return

        # Handle Standard Synchronous Response Message
        log_formatted("RECV_RESP", msg)
        
        # Automatically update client session state on successful authentication (200 OK)
        if msg.status_code == StatusCode.OK and "session_id" in payload:
            self.session_id = payload["session_id"]
            self.username = payload.get("username")
        
        # Automatically update current room state on successful join (200 OK)
        if msg.status_code == StatusCode.OK and "current_room" in payload:
            self.current_room = payload["current_room"]

        # Handle File Download Content Payload
        if msg.status_code == StatusCode.OK and "file_data" in payload:
            filename = payload.get("filename")
            b64_data = payload.get("file_data")
            try:
                raw_bytes = base64.b64decode(b64_data)
                download_path = os.path.join(os.getcwd(), f"downloaded_{filename}")
                with open(download_path, "wb") as f:
                    f.write(raw_bytes)
                print(f"{COLORS['GREEN']}✅ Downloaded file saved as: {download_path}{COLORS['ENDC']}")
            except Exception as e:
                print(f"{COLORS['RED']}[!] Error saving downloaded file: {e}{COLORS['ENDC']}")

    def run_cli(self):
        """
        Main User Interaction Loop (CLI Prompt).
        Reads keyboard input, handles slash commands, and formats CRTP requests.
        """
        self.print_banner()
        print("Type '/help' for a list of available commands.\n")
        
        while self.running:
            try:
                # Dynamic terminal prompt displaying username and current room
                prompt_room = f" ({self.current_room})" if self.current_room else ""
                prompt_user = f"[{self.username}{prompt_room}]> " if self.username else "[Guest]> "
                
                user_input = input(prompt_user).strip()
                if not user_input:
                    continue
                
                # Check for slash commands
                if user_input.startswith("/"):
                    self.parse_command(user_input)
                else:
                    # Shortcut: plain text sends chat message to joined room
                    if not self.username:
                        print(f"{COLORS['RED']}[!] Please login first using '/auth <username>'{COLORS['ENDC']}")
                    elif not self.current_room:
                        print(f"{COLORS['YELLOW']}[!] Please join a room first using '/join <room_name>'{COLORS['ENDC']}")
                    else:
                        req = CRTPMessage.create_request("SEND_MSG", {"text": user_input})
                        self.send_request(req)
            except (KeyboardInterrupt, EOFError):
                print("\nExiting...")
                self.running = False
                break

        if self.sock:
            self.sock.close()

    def parse_command(self, user_input: str):
        """
        Parse and dispatch slash commands entered by the user.

        Args:
            user_input (str): Full command string starting with '/'
        """
        parts = user_input.split(" ", 1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if cmd == "/help":
            self.print_help()
        elif cmd == "/auth":
            if not arg:
                print("Usage: /auth <username>")
                return
            req = CRTPMessage.create_request("AUTH", {"username": arg})
            self.send_request(req)
        elif cmd == "/rooms":
            req = CRTPMessage.create_request("LIST_ROOMS")
            self.send_request(req)
        elif cmd == "/create":
            if not arg:
                print("Usage: /create <room_name>")
                return
            req = CRTPMessage.create_request("CREATE_ROOM", {"room": arg})
            self.send_request(req)
        elif cmd == "/join":
            if not arg:
                print("Usage: /join <room_name>")
                return
            req = CRTPMessage.create_request("JOIN_ROOM", {"room": arg})
            self.send_request(req)
        elif cmd == "/leave":
            req = CRTPMessage.create_request("LEAVE_ROOM")
            self.send_request(req)
            self.current_room = None
        elif cmd == "/msg":
            if not arg:
                print("Usage: /msg <text>")
                return
            req = CRTPMessage.create_request("SEND_MSG", {"text": arg})
            self.send_request(req)
        elif cmd == "/users":
            req = CRTPMessage.create_request("LIST_USERS")
            self.send_request(req)
        elif cmd == "/file":
            if not arg or not os.path.exists(arg):
                print("Usage: /file <local_filepath> (File must exist)")
                return
            self.send_file(arg)
        elif cmd == "/download":
            if not arg:
                print("Usage: /download <filename_on_server>")
                return
            req = CRTPMessage.create_request("DOWNLOAD_FILE", {"filename": arg})
            self.send_request(req)
        elif cmd in ["/quit", "/exit"]:
            print("Disconnecting...")
            self.running = False
        else:
            print(f"{COLORS['RED']}Unknown command: {cmd}. Type /help for options.{COLORS['ENDC']}")

    def send_file(self, filepath: str):
        """
        Read local file, encode content to Base64, and send SEND_FILE request.

        Args:
            filepath (str): Absolute or relative local path to file.
        """
        filename = os.path.basename(filepath)
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            b64_str = base64.b64encode(content).decode('utf-8')
            req = CRTPMessage.create_request("SEND_FILE", {
                "filename": filename,
                "file_data": b64_str
            })
            self.send_request(req)
        except Exception as e:
            print(f"{COLORS['RED']}[!] Error reading file: {e}{COLORS['ENDC']}")

    def print_banner(self):
        """Print application CLI header banner."""
        print(f"{COLORS['HEADER']}{COLORS['BOLD']}")
        print("==================================================")
        print(" CRTP Client - Chat & Resource Transfer Protocol")
        print("==================================================")
        print(f"{COLORS['ENDC']}")

    def print_help(self):
        """Print help table of available CLI commands."""
        print(f"{COLORS['BOLD']}Available Commands:{COLORS['ENDC']}")
        print("  /auth <username>      - Authenticate with the server")
        print("  /rooms                - List active rooms")
        print("  /create <room>        - Create a new room")
        print("  /join <room>          - Join a room")
        print("  /leave                - Leave current room")
        print("  /msg <text>           - Send message to room (or type text directly)")
        print("  /users                - List members in current room")
        print("  /file <path>          - Upload a file to current room")
        print("  /download <filename>  - Download a shared file from server")
        print("  /help                 - Show this help menu")
        print("  /quit                 - Exit application\n")

if __name__ == '__main__':
    client = CRTPClient()
    if client.connect():
        client.run_cli()
