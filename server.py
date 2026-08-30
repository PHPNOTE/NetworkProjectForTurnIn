"""
CRTP Multi-Threaded TCP Server (server.py)
=========================================
Network Socket Programming Project - Computer Networks

This module implements the server side of the Chat & Resource Transfer Protocol (CRTP/1.0).

Key Responsibilities:
1. Socket Binding & Connection Acceptance:
   - Binds to host 0.0.0.0 and port 8888.
   - Listens for incoming TCP connections and spawns a dedicated worker thread
     (`threading.Thread`) for each connected client.

2. State Management & Thread Safety:
   - Manages active user sessions, session IDs (UUIDs), and user-to-room mappings.
   - Protects global shared state using `threading.Lock()` to prevent race conditions.

3. Protocol Request Processing & Routing:
   - Receives byte streams from client sockets.
   - Uses line-based buffering (`\n` delimitation) to reconstruct JSON messages.
   - Parses CRTP Request objects and executes corresponding business logic:
     * AUTH          - User authentication and session assignment
     * LIST_ROOMS    - List all available chat rooms and member counts
     * CREATE_ROOM   - Dynamically create a new room
     * JOIN_ROOM     - Join a specified chat room
     * LEAVE_ROOM    - Leave the currently joined room
     * SEND_MSG      - Broadcast chat message to members of current room
     * LIST_USERS    - List usernames in current room
     * SEND_FILE     - Receive base64 encoded file data and save to disk
     * DOWNLOAD_FILE - Read requested file from disk and return base64 data

4. Status Code Logging:
   - Prints clear, colorized console logs showing every incoming Request and
     outgoing Response accompanied by its Status Code and Status Phrase.

"""

import socket
import threading
import json
import os
import uuid
import base64
from protocol import CRTPMessage, StatusCode, STATUS_PHRASES, log_formatted, COLORS

# Server Binding Configuration
HOST = '0.0.0.0'
PORT = 8888

# Storage location for uploaded files shared across clients
UPLOADS_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOADS_DIR, exist_ok=True)


class ServerState:
    """
    Thread-safe State Manager for the CRTP Server.
    Uses reentrant locking (`threading.Lock`) to safely manage online users,
    active sessions, and chat room memberships across multiple concurrent client threads.
    """
    def __init__(self):
        self.lock = threading.Lock()
        
        # Dictionary mapping session_id -> { "username": str, "socket": socket_obj, "room": str }
        self.sessions = {}
        
        # Dictionary mapping username -> session_id
        self.users = {}
        
        # Dictionary mapping room_name -> set of session_ids
        self.rooms = {
            "General": set(),
            "Tech": set(),
            "Random": set()
        }

    def register_user(self, username: str, client_socket) -> tuple:
        """
        Authenticate and register a new user.

        Args:
            username (str): Desired username.
            client_socket (socket.socket): The client's active TCP socket.

        Returns:
            tuple: (session_id, StatusCode, message_str)
                   Returns (None, 409, error) if username is already taken.
        """
        with self.lock:
            if username in self.users:
                return None, StatusCode.CONFLICT, "Username already taken"
            
            # Generate a unique 8-character session ID
            session_id = str(uuid.uuid4())[:8]
            self.users[username] = session_id
            self.sessions[session_id] = {
                "username": username,
                "socket": client_socket,
                "room": None
            }
            return session_id, StatusCode.OK, "Authentication successful"

    def remove_session(self, session_id: str) -> tuple:
        """
        Clean up user session when a client disconnects.

        Args:
            session_id (str): Session ID to remove.

        Returns:
            tuple: (username, room_name) of the removed session for notification purposes.
        """
        with self.lock:
            if session_id in self.sessions:
                session = self.sessions[session_id]
                username = session["username"]
                room = session["room"]
                
                # Remove from room membership
                if room and room in self.rooms:
                    self.rooms[room].discard(session_id)
                # Remove from user registry
                if username in self.users:
                    del self.users[username]
                # Remove session
                del self.sessions[session_id]
                return username, room
        return None, None

    def get_session(self, session_id: str) -> dict:
        """
        Retrieve session dictionary by session_id.

        Args:
            session_id (str): Target session ID.

        Returns:
            dict or None: Session details if found.
        """
        with self.lock:
            return self.sessions.get(session_id)

    def create_room(self, room_name: str) -> tuple:
        """
        Create a new chat room.

        Args:
            room_name (str): Name of the room to create.

        Returns:
            tuple: (StatusCode, message_str)
        """
        with self.lock:
            if room_name in self.rooms:
                return StatusCode.CONFLICT, f"Room '{room_name}' already exists"
            self.rooms[room_name] = set()
            return StatusCode.CREATED, f"Room '{room_name}' created successfully"

    def join_room(self, session_id: str, room_name: str) -> tuple:
        """
        Add a user session to a specified chat room.

        Args:
            session_id (str): User's session ID.
            room_name (str): Room to join.

        Returns:
            tuple: (StatusCode, message_str)
        """
        with self.lock:
            if session_id not in self.sessions:
                return StatusCode.UNAUTHORIZED, "Invalid session"
            if room_name not in self.rooms:
                return StatusCode.NOT_FOUND, f"Room '{room_name}' does not exist"
            
            # Leave old room first if currently in one
            old_room = self.sessions[session_id]["room"]
            if old_room and old_room in self.rooms:
                self.rooms[old_room].discard(session_id)

            self.sessions[session_id]["room"] = room_name
            self.rooms[room_name].add(session_id)
            return StatusCode.OK, f"Joined room '{room_name}'"

    def leave_room(self, session_id: str) -> tuple:
        """
        Remove a user session from their current room.

        Args:
            session_id (str): User's session ID.

        Returns:
            tuple: (StatusCode, message_str)
        """
        with self.lock:
            if session_id not in self.sessions:
                return StatusCode.UNAUTHORIZED, "Invalid session"
            room = self.sessions[session_id]["room"]
            if room and room in self.rooms:
                self.rooms[room].discard(session_id)
                self.sessions[session_id]["room"] = None
                return StatusCode.OK, f"Left room '{room}'"
            return StatusCode.BAD_REQUEST, "Not currently in any room"

    def get_room_members(self, room_name: str) -> list:
        """
        Get a list of usernames currently joined in a room.

        Args:
            room_name (str): Room name.

        Returns:
            list: List of active usernames in that room.
        """
        with self.lock:
            if room_name not in self.rooms:
                return None
            session_ids = self.rooms[room_name]
            return [self.sessions[sid]["username"] for sid in session_ids if sid in self.sessions]

    def get_room_sockets(self, room_name: str) -> list:
        """
        Get a list of active TCP sockets for all users in a room.

        Args:
            room_name (str): Room name.

        Returns:
            list: List of socket objects.
        """
        with self.lock:
            if room_name not in self.rooms:
                return []
            return [self.sessions[sid]["socket"] for sid in self.rooms[room_name] if sid in self.sessions]

    def list_rooms(self) -> dict:
        """
        Get all active room names and their current member counts.

        Returns:
            dict: Mapping room_name -> member_count.
        """
        with self.lock:
            return {room: len(members) for room, members in self.rooms.items()}


# Single global instance of ServerState
state = ServerState()


def broadcast_to_room(room_name: str, message: CRTPMessage, exclude_socket=None):
    """
    Broadcast a CRTP Message to all connected sockets in a specified chat room.

    Args:
        room_name (str): Target room name.
        message (CRTPMessage): Message object to broadcast.
        exclude_socket (socket.socket, optional): Socket to exclude (e.g. sender socket).
    """
    sockets = state.get_room_sockets(room_name)
    msg_bytes = message.to_bytes()
    for sock in sockets:
        if sock != exclude_socket:
            try:
                sock.sendall(msg_bytes)
            except Exception:
                # Silently ignore write failures on stale sockets; cleanup happens in worker loop
                pass


def handle_client(client_socket: socket.socket, client_address: tuple):
    """
    Worker thread routine dedicated to handling a single client connection.

    Framing & Buffer Logic:
        Reads incoming bytes into a local string buffer and processes completed
        lines separated by `\n`. This ensures full JSON message reconstruction
        regardless of TCP packet fragmentation.

    Args:
        client_socket (socket.socket): Accepted client socket.
        client_address (tuple): Client (IP, Port) address pair.
    """
    addr_str = f"{client_address[0]}:{client_address[1]}"
    print(f"{COLORS['GREEN']}[+] New Connection from {addr_str}{COLORS['ENDC']}")
    session_id = None
    buffer = ""

    try:
        while True:
            # Receive data chunk from client socket (buffer up to 4096 bytes)
            data = client_socket.recv(4096)
            if not data:
                # Empty data read indicates client closed connection cleanly
                break
            
            buffer += data.decode('utf-8')
            # Extract complete lines delimited by newline `\n`
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                
                # Parse JSON string into CRTPMessage object
                try:
                    request = CRTPMessage.from_json(line)
                except Exception as e:
                    # Return 400 Bad Request if JSON parsing fails
                    resp = CRTPMessage.create_response(StatusCode.BAD_REQUEST, {"error": "Invalid JSON format"})
                    log_formatted("SEND_RESP", resp, addr_str)
                    client_socket.sendall(resp.to_bytes())
                    continue

                # Log incoming request
                log_formatted("RECV_REQ", request, addr_str)
                
                # Execute request logic and generate response
                response = process_request(request, client_socket, addr_str)
                
                # Track session_id for cleanup in finally block
                if request.headers.get("session_id"):
                    session_id = request.headers.get("session_id")

                # Log outgoing response with Status Code and Status Phrase
                log_formatted("SEND_RESP", response, addr_str)
                client_socket.sendall(response.to_bytes())

    except ConnectionResetError:
        print(f"{COLORS['YELLOW']}[!] Connection reset by {addr_str}{COLORS['ENDC']}")
    except Exception as e:
        print(f"{COLORS['RED']}[!] Error handling {addr_str}: {e}{COLORS['ENDC']}")
    finally:
        # Perform session cleanup upon disconnect
        username, room = state.remove_session(session_id) if session_id else (None, None)
        if username and room:
            # Broadcast system notification to room members
            notify_msg = CRTPMessage.create_response(
                StatusCode.OK,
                payload={"type": "SYSTEM", "message": f"User '{username}' left the room."}
            )
            broadcast_to_room(room, notify_msg)
        client_socket.close()
        print(f"{COLORS['YELLOW']}[-] Client {addr_str} disconnected{COLORS['ENDC']}")


def process_request(req: CRTPMessage, client_socket: socket.socket, addr_str: str) -> CRTPMessage:
    """
    Route and execute a CRTP Request based on command name.

    Supported Commands:
        - AUTH          -> Authenticate user with username
        - LIST_ROOMS    -> Query available rooms and count
        - CREATE_ROOM   -> Create room
        - JOIN_ROOM     -> Join room
        - LEAVE_ROOM    -> Leave room
        - SEND_MSG      -> Broadcast message
        - LIST_USERS    -> List users in room
        - SEND_FILE     -> Upload file (base64)
        - DOWNLOAD_FILE -> Download file (base64)

    Args:
        req (CRTPMessage): Parsed request object.
        client_socket (socket.socket): Client socket.
        addr_str (str): Client address string for logging.

    Returns:
        CRTPMessage: Response message containing status_code and payload.
    """
    cmd = req.command
    payload = req.payload
    headers = req.headers
    session_id = headers.get("session_id")

    # -------------------------------------------------------------
    # 1. COMMAND: AUTH (Authentication)
    # -------------------------------------------------------------
    if cmd == "AUTH":
        username = payload.get("username", "").strip()
        if not username:
            return CRTPMessage.create_response(StatusCode.BAD_REQUEST, {"message": "Username is required"})
        
        sid, code, msg = state.register_user(username, client_socket)
        if code == StatusCode.OK:
            return CRTPMessage.create_response(code, {"message": msg, "session_id": sid, "username": username})
        return CRTPMessage.create_response(code, {"message": msg})

    # Enforce Authentication Check for all other commands
    if not session_id or not state.get_session(session_id):
        return CRTPMessage.create_response(
            StatusCode.UNAUTHORIZED,
            {"message": "Authentication required. Please perform AUTH first."}
        )

    session = state.get_session(session_id)
    username = session["username"]
    current_room = session["room"]

    # -------------------------------------------------------------
    # 2. COMMAND: LIST_ROOMS
    # -------------------------------------------------------------
    if cmd == "LIST_ROOMS":
        rooms_info = state.list_rooms()
        return CRTPMessage.create_response(StatusCode.OK, {"rooms": rooms_info})

    # -------------------------------------------------------------
    # 3. COMMAND: CREATE_ROOM
    # -------------------------------------------------------------
    elif cmd == "CREATE_ROOM":
        room_name = payload.get("room", "").strip()
        if not room_name:
            return CRTPMessage.create_response(StatusCode.BAD_REQUEST, {"message": "Room name is required"})
        code, msg = state.create_room(room_name)
        return CRTPMessage.create_response(code, {"message": msg})

    # -------------------------------------------------------------
    # 4. COMMAND: JOIN_ROOM
    # -------------------------------------------------------------
    elif cmd == "JOIN_ROOM":
        room_name = payload.get("room", "").strip()
        code, msg = state.join_room(session_id, room_name)
        if code == StatusCode.OK:
            # Broadcast notification to other members in the joined room
            notify_msg = CRTPMessage.create_response(
                StatusCode.OK,
                payload={"type": "SYSTEM", "message": f"User '{username}' joined room '{room_name}'."}
            )
            broadcast_to_room(room_name, notify_msg, exclude_socket=client_socket)
        return CRTPMessage.create_response(code, {"message": msg, "current_room": room_name})

    # -------------------------------------------------------------
    # 5. COMMAND: LEAVE_ROOM
    # -------------------------------------------------------------
    elif cmd == "LEAVE_ROOM":
        code, msg = state.leave_room(session_id)
        if code == StatusCode.OK and current_room:
            notify_msg = CRTPMessage.create_response(
                StatusCode.OK,
                payload={"type": "SYSTEM", "message": f"User '{username}' left room '{current_room}'."}
            )
            broadcast_to_room(current_room, notify_msg)
        return CRTPMessage.create_response(code, {"message": msg})

    # -------------------------------------------------------------
    # 6. COMMAND: SEND_MSG (Broadcast Text Chat)
    # -------------------------------------------------------------
    elif cmd == "SEND_MSG":
        if not current_room:
            return CRTPMessage.create_response(
                StatusCode.FORBIDDEN,
                {"message": "You must join a room before sending messages"}
            )
        text = payload.get("text", "").strip()
        if not text:
            return CRTPMessage.create_response(StatusCode.BAD_REQUEST, {"message": "Message text cannot be empty"})
        
        # Broadcast chat payload to all members in current room
        chat_broadcast = CRTPMessage.create_response(
            StatusCode.OK,
            payload={
                "type": "CHAT",
                "room": current_room,
                "sender": username,
                "text": text,
                "timestamp": headers.get("timestamp")
            }
        )
        broadcast_to_room(current_room, chat_broadcast)
        return CRTPMessage.create_response(StatusCode.OK, {"message": "Message sent"})

    # -------------------------------------------------------------
    # 7. COMMAND: LIST_USERS
    # -------------------------------------------------------------
    elif cmd == "LIST_USERS":
        if not current_room:
            return CRTPMessage.create_response(StatusCode.FORBIDDEN, {"message": "You must join a room first"})
        members = state.get_room_members(current_room)
        return CRTPMessage.create_response(StatusCode.OK, {"room": current_room, "users": members})

    # -------------------------------------------------------------
    # 8. COMMAND: SEND_FILE (Upload File)
    # -------------------------------------------------------------
    elif cmd == "SEND_FILE":
        if not current_room:
            return CRTPMessage.create_response(StatusCode.FORBIDDEN, {"message": "You must join a room to share files"})
        
        filename = os.path.basename(payload.get("filename", ""))
        file_data_b64 = payload.get("file_data", "")
        if not filename or not file_data_b64:
            return CRTPMessage.create_response(StatusCode.BAD_REQUEST, {"message": "Filename and file_data required"})
        
        try:
            # Decode base64 payload into raw binary bytes
            raw_bytes = base64.b64decode(file_data_b64)
            saved_filename = f"{username}_{filename}"
            save_path = os.path.join(UPLOADS_DIR, saved_filename)
            
            # Save file to uploads directory
            with open(save_path, "wb") as f:
                f.write(raw_bytes)
            
            # Notify members in the room about the shared file
            file_broadcast = CRTPMessage.create_response(
                StatusCode.CREATED,
                payload={
                    "type": "FILE_ALERT",
                    "room": current_room,
                    "sender": username,
                    "filename": saved_filename,
                    "size_bytes": len(raw_bytes)
                }
            )
            broadcast_to_room(current_room, file_broadcast)
            return CRTPMessage.create_response(
                StatusCode.CREATED,
                {"message": "File uploaded successfully", "saved_as": saved_filename}
            )
        except Exception as e:
            return CRTPMessage.create_response(
                StatusCode.INTERNAL_SERVER_ERROR,
                {"message": f"Failed to save file: {str(e)}"}
            )

    # -------------------------------------------------------------
    # 9. COMMAND: DOWNLOAD_FILE
    # -------------------------------------------------------------
    elif cmd == "DOWNLOAD_FILE":
        filename = os.path.basename(payload.get("filename", ""))
        target_path = os.path.join(UPLOADS_DIR, filename)
        
        # Check file existence on server
        if not os.path.exists(target_path):
            return CRTPMessage.create_response(
                StatusCode.NOT_FOUND,
                {"message": f"File '{filename}' not found on server"}
            )
        
        try:
            with open(target_path, "rb") as f:
                content = f.read()
            b64_content = base64.b64encode(content).decode('utf-8')
            return CRTPMessage.create_response(
                StatusCode.OK,
                payload={"filename": filename, "file_data": b64_content, "size_bytes": len(content)}
            )
        except Exception as e:
            return CRTPMessage.create_response(
                StatusCode.INTERNAL_SERVER_ERROR,
                {"message": f"Download error: {str(e)}"}
            )

    # Handle Unsupported Commands
    return CRTPMessage.create_response(
        StatusCode.BAD_REQUEST,
        {"message": f"Unknown command: '{cmd}'"}
    )


def start_server():
    """
    Initialize and run the TCP Socket Server.
    Binds socket, starts listening, and enters connection acceptance loop.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Enable address reuse to avoid 'Address already in use' errors during restart
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(10)
    
    print(f"{COLORS['HEADER']}{COLORS['BOLD']}==================================================")
    print(f" CRTP Server Running on {HOST}:{PORT}")
    print(f" Protocol Version: CRTP/1.0 (TCP)")
    print(f" Upload Directory: {UPLOADS_DIR}")
    print(f"=================================================={COLORS['ENDC']}\n")

    try:
        while True:
            # Block waiting for incoming TCP client connection
            client_socket, client_address = server_socket.accept()
            
            # Spawn dedicated worker thread for the accepted client
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True
            )
            client_thread.start()
    except KeyboardInterrupt:
        print(f"\n{COLORS['YELLOW']}[!] Shutting down server...{COLORS['ENDC']}")
    finally:
        server_socket.close()

if __name__ == '__main__':
    start_server()
