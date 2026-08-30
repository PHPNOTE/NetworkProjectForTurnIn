"""
CRTP Protocol Module (protocol.py)
==================================
Network Socket Programming Project - Computer Networks

This module defines the core data structures, status codes, message framing,
and logger utilities for the Chat & Resource Transfer Protocol (CRTP/1.0).

Protocol Specifications:
- Transport Layer: TCP (Transmission Control Protocol)
- Message Format: JSON String serialized as UTF-8
- Message Framing: Newline-delimited JSON (`\n`) to prevent TCP stream fragmentation
- Message Types:
    1. Request Message (Sent by Client to Server)
    2. Response Message (Sent by Server to Client in response to a Request)
    3. Broadcast Message (Sent asynchronously by Server to room members)

"""

import json
import time
from enum import IntEnum


class StatusCode(IntEnum):
    """
    Standard Status Codes used in CRTP responses.
    Categorized similarly to HTTP status codes for clarity and protocol design.
    """
    # 2xx Success - The request was successfully received, understood, and accepted
    OK = 200                    # Request processed successfully
    CREATED = 201                # Resource created (e.g. room created, file uploaded)
    ACCEPTED = 202               # Request accepted for processing

    # 4xx Client Error - The request contains bad syntax or cannot be fulfilled
    BAD_REQUEST = 400            # Malformed JSON or missing required fields
    UNAUTHORIZED = 401           # Client has not authenticated (must call AUTH first)
    FORBIDDEN = 403              # Action forbidden in current state (e.g., send message without joining room)
    NOT_FOUND = 404              # Target room or file does not exist
    CONFLICT = 409               # Username or room name already exists
    PAYLOAD_TOO_LARGE = 413      # File size exceeds server limits

    # 5xx Server Error - The server failed to fulfill an apparently valid request
    INTERNAL_SERVER_ERROR = 500  # Server-side I/O or unexpected failure


# Human-readable status phrases corresponding to status codes
STATUS_PHRASES = {
    StatusCode.OK: "OK",
    StatusCode.CREATED: "CREATED",
    StatusCode.ACCEPTED: "ACCEPTED",
    StatusCode.BAD_REQUEST: "BAD_REQUEST",
    StatusCode.UNAUTHORIZED: "UNAUTHORIZED",
    StatusCode.FORBIDDEN: "FORBIDDEN",
    StatusCode.NOT_FOUND: "NOT_FOUND",
    StatusCode.CONFLICT: "CONFLICT",
    StatusCode.PAYLOAD_TOO_LARGE: "PAYLOAD_TOO_LARGE",
    StatusCode.INTERNAL_SERVER_ERROR: "INTERNAL_SERVER_ERROR"
}

# ANSI Color Escape Codes for vibrant console log output
COLORS = {
    "HEADER": "\033[95m",
    "BLUE": "\033[94m",
    "CYAN": "\033[96m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "RED": "\033[91m",
    "ENDC": "\033[0m",
    "BOLD": "\033[1m"
}


class CRTPMessage:
    """
    Represents a CRTP (Chat & Resource Transfer Protocol) message object.

    Attributes:
        command (str, optional): The command identifier (e.g., 'AUTH', 'JOIN_ROOM', 'SEND_MSG').
        status_code (int, optional): Numeric status code (e.g., 200, 404).
        status_phrase (str, optional): Human-readable status string (e.g., 'OK', 'NOT_FOUND').
        headers (dict): Metadata dictionary containing headers like session_id, timestamp.
        payload (dict): The main message content or body parameters.
    """

    def __init__(self, command=None, status_code=None, status_phrase=None, headers=None, payload=None):
        """
        Initialize a new CRTPMessage instance.

        Args:
            command (str, optional): Command name for request messages.
            status_code (int or StatusCode, optional): Status code for response messages.
            status_phrase (str, optional): Custom status phrase; defaults to standard lookup.
            headers (dict, optional): Message headers dictionary.
            payload (dict, optional): Message payload dictionary.
        """
        self.command = command
        self.status_code = status_code
        self.status_phrase = status_phrase or (STATUS_PHRASES.get(status_code) if status_code else None)
        self.headers = headers if headers is not None else {}
        self.payload = payload if payload is not None else {}
        # Automatically attach epoch timestamp if missing
        self.headers.setdefault("timestamp", time.time())

    def to_dict(self) -> dict:
        """
        Convert the CRTPMessage object into a JSON-serializable dictionary.

        Returns:
            dict: Structured dictionary representation of the message.
        """
        data = {}
        if self.command:
            data["command"] = self.command
        if self.status_code is not None:
            data["status_code"] = int(self.status_code)
            data["status_phrase"] = self.status_phrase
        data["headers"] = self.headers
        data["payload"] = self.payload
        return data

    def to_bytes(self) -> bytes:
        """
        Serialize the message into UTF-8 encoded bytes terminated with a newline (`\n`).

        Framing Mechanism:
            TCP is a stream-based protocol that does not preserve message boundaries.
            Appending `\n` to every serialized JSON string allows receiver sockets
            to buffer incoming data and reliably split messages at line breaks.

        Returns:
            bytes: Encoded byte stream ready for socket.sendall().
        """
        json_str = json.dumps(self.to_dict())
        return (json_str + "\n").encode('utf-8')

    @classmethod
    def from_dict(cls, data: dict):
        """
        Construct a CRTPMessage instance from a Python dictionary.

        Args:
            data (dict): Dictionary parsed from JSON.

        Returns:
            CRTPMessage: Reconstructed message object.
        """
        return cls(
            command=data.get("command"),
            status_code=data.get("status_code"),
            status_phrase=data.get("status_phrase"),
            headers=data.get("headers", {}),
            payload=data.get("payload", {})
        )

    @classmethod
    def from_json(cls, json_str: str):
        """
        Construct a CRTPMessage instance from a JSON string.

        Args:
            json_str (str): Raw JSON string received over socket.

        Returns:
            CRTPMessage: Reconstructed message object.
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def create_request(cls, command: str, payload: dict = None, headers: dict = None):
        """
        Helper method to construct a Request message.

        Args:
            command (str): Target command (e.g., 'AUTH', 'SEND_MSG').
            payload (dict, optional): Parameters for the command.
            headers (dict, optional): Additional headers.

        Returns:
            CRTPMessage: Request message object.
        """
        return cls(command=command, payload=payload, headers=headers)

    @classmethod
    def create_response(cls, status_code: StatusCode, payload: dict = None, headers: dict = None, status_phrase: str = None):
        """
        Helper method to construct a Response message.

        Args:
            status_code (StatusCode): Result status code.
            payload (dict, optional): Response data or error details.
            headers (dict, optional): Response headers.
            status_phrase (str, optional): Custom status description.

        Returns:
            CRTPMessage: Response message object.
        """
        return cls(status_code=status_code, status_phrase=status_phrase, payload=payload, headers=headers)


def log_formatted(direction: str, message: CRTPMessage, client_addr: str = None):
    """
    Format and print a color-coded log line to the console.
    This fulfills Requirement #2 of the project specification to clearly display
    sent/received messages, status codes, and status phrases.

    Args:
        direction (str): Log direction ('RECV_REQ', 'SEND_RESP', 'SEND_REQ', 'RECV_RESP', 'BROADCAST').
        message (CRTPMessage): The CRTP message object being logged.
        client_addr (str, optional): Client IP:Port string for server-side logging.
    """
    addr_info = f" [{client_addr}]" if client_addr else ""

    if direction == "RECV_REQ":
        prefix = f"{COLORS['BOLD']}[RECV REQUEST]{COLORS['ENDC']}{addr_info}"
        detail = f"CMD: {COLORS['BOLD']}{COLORS['CYAN']}{message.command}{COLORS['ENDC']} | Payload: {message.payload}"
    elif direction == "SEND_RESP":
        code = message.status_code
        # Highlight success in green, client/server errors in red
        code_color = COLORS["GREEN"] if code and code < 400 else COLORS["RED"]
        prefix = f"{COLORS['BOLD']}[SEND RESPONSE]{COLORS['ENDC']}{addr_info}"
        detail = f"STATUS: {code_color}{code} {message.status_phrase}{COLORS['ENDC']} | Payload: {message.payload}"
    elif direction == "SEND_REQ":
        prefix = f"{COLORS['BOLD']}[SEND REQUEST]{COLORS['ENDC']}"
        detail = f"CMD: {COLORS['BOLD']}{COLORS['CYAN']}{message.command}{COLORS['ENDC']} | Payload: {message.payload}"
    elif direction == "RECV_RESP":
        code = message.status_code
        code_color = COLORS["GREEN"] if code and code < 400 else COLORS["RED"]
        prefix = f"{COLORS['BOLD']}[RECV RESPONSE]{COLORS['ENDC']}"
        detail = f"STATUS: {code_color}{code} {message.status_phrase}{COLORS['ENDC']} | Payload: {message.payload}"
    elif direction == "BROADCAST":
        prefix = f"{COLORS['BOLD']}[BROADCAST]{COLORS['ENDC']}"
        detail = f"Type: {message.payload.get('type')} | Content: {message.payload.get('message') or message.payload.get('text')}"
    else:
        prefix = f"[{direction}]"
        detail = str(message.to_dict())

    print(f"{prefix} {detail}")
