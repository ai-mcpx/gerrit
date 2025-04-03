#!/usr/bin/env python3
"""
MCP Server Example for Gerrit API

This example demonstrates how to create a server that uses the Message Consumer Protocol (MCP)
to listen for and process Gerrit events.
"""

import json
import asyncio
import websockets
import requests
import base64
import logging
from typing import Dict, Any, List, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mcp-gerrit-server")

# Configuration
GERRIT_URL = "https://gerrit-review.example.com"
GERRIT_USERNAME = "username"
GERRIT_PASSWORD = "password"
GERRIT_STREAM_EVENTS_ENDPOINT = "/a/events/"
MCP_PORT = 8080

class GerritClient:
    """Client for interacting with the Gerrit API."""
    
    def __init__(self, url: str, username: str, password: str):
        self.url = url.rstrip('/')
        self.auth_header = self._create_auth_header(username, password)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        })
    
    def _create_auth_header(self, username: str, password: str) -> str:
        """Create Basic Auth header."""
        auth_string = f"{username}:{password}"
        encoded = base64.b64encode(auth_string.encode()).decode()
        return f"Basic {encoded}"
    
    def get_change(self, change_id: str) -> Dict[str, Any]:
        """Get details about a specific change."""
        endpoint = f"/a/changes/{change_id}/detail"
        response = self.session.get(f"{self.url}{endpoint}")
        response.raise_for_status()
        
        # Gerrit API prepends responses with )]}' to prevent XSSI
        content = response.text
        if content.startswith(")]}'"):
            content = content[4:]
        
        return json.loads(content)
    
    def add_review(self, change_id: str, revision_id: str, 
                  message: str, labels: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """Add a review to a change."""
        endpoint = f"/a/changes/{change_id}/revisions/{revision_id}/review"
        data = {
            "message": message,
            "labels": labels or {}
        }
        
        response = self.session.post(f"{self.url}{endpoint}", json=data)
        response.raise_for_status()
        
        content = response.text
        if content.startswith(")]}'"):
            content = content[4:]
        
        return json.loads(content)


class MCPServer:
    """Server implementing the Message Consumer Protocol for Gerrit events."""
    
    def __init__(self, gerrit_client: GerritClient):
        self.gerrit_client = gerrit_client
        self.connected_clients = set()
    
    async def register_client(self, websocket):
        """Register a new client connection."""
        self.connected_clients.add(websocket)
        try:
            await self.handle_client(websocket)
        finally:
            self.connected_clients.remove(websocket)
    
    async def handle_client(self, websocket):
        """Handle messages from a client."""
        async for message in websocket:
            try:
                data = json.loads(message)
                await self.process_message(websocket, data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received: {message}")
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": str(e)
                }))
    
    async def process_message(self, websocket, data: Dict[str, Any]):
        """Process a message received from a client."""
        msg_type = data.get("type")
        
        if msg_type == "subscribe":
            await self.handle_subscribe(websocket, data)
        elif msg_type == "action":
            await self.handle_action(websocket, data)
        else:
            await websocket.send(json.dumps({
                "type": "error",
                "message": f"Unknown message type: {msg_type}"
            }))
    
    async def handle_subscribe(self, websocket, data: Dict[str, Any]):
        """Handle subscription requests."""
        topics = data.get("topics", [])
        
        # Acknowledge subscription
        await websocket.send(json.dumps({
            "type": "subscribed",
            "topics": topics
        }))
        
        logger.info(f"Client subscribed to topics: {topics}")
    
    async def handle_action(self, websocket, data: Dict[str, Any]):
        """Handle action requests."""
        action = data.get("action")
        payload = data.get("payload", {})
        
        if action == "review":
            await self.handle_review_action(websocket, payload)
        elif action == "get_change":
            await self.handle_get_change(websocket, payload)
        else:
            await websocket.send(json.dumps({
                "type": "error",
                "message": f"Unknown action: {action}"
            }))
    
    async def handle_review_action(self, websocket, payload: Dict[str, Any]):
        """Handle review action requests."""
        change_id = payload.get("change_id")
        revision_id = payload.get("revision_id")
        message = payload.get("message", "")
        labels = payload.get("labels", {})
        
        try:
            result = self.gerrit_client.add_review(change_id, revision_id, message, labels)
            await websocket.send(json.dumps({
                "type": "action_result",
                "action": "review",
                "status": "success",
                "result": result
            }))
        except Exception as e:
            logger.exception(f"Error adding review: {e}")
            await websocket.send(json.dumps({
                "type": "action_result",
                "action": "review",
                "status": "error",
                "message": str(e)
            }))
    
    async def handle_get_change(self, websocket, payload: Dict[str, Any]):
        """Handle get change requests."""
        change_id = payload.get("change_id")
        
        try:
            result = self.gerrit_client.get_change(change_id)
            await websocket.send(json.dumps({
                "type": "action_result",
                "action": "get_change",
                "status": "success",
                "result": result
            }))
        except Exception as e:
            logger.exception(f"Error getting change: {e}")
            await websocket.send(json.dumps({
                "type": "action_result",
                "action": "get_change",
                "status": "error",
                "message": str(e)
            }))
    
    async def broadcast_event(self, event: Dict[str, Any]):
        """Broadcast a Gerrit event to all connected clients."""
        if not self.connected_clients:
            return
        
        message = json.dumps({
            "type": "event",
            "source": "gerrit",
            "payload": event
        })
        
        # Send the event to all connected clients
        await asyncio.gather(
            *[client.send(message) for client in self.connected_clients],
            return_exceptions=True
        )
    
    async def start_gerrit_event_stream(self):
        """Start streaming events from Gerrit."""
        logger.info("Starting Gerrit event stream...")
        
        # This is a simplified example - in a real implementation, you would
        # use SSE (Server-Sent Events) or WebSockets to connect to Gerrit's stream-events
        # endpoint, which requires more complex authentication and connection handling.
        #
        # For this example, we'll simulate receiving events every 5 seconds
        
        while True:
            try:
                # Simulate a Gerrit event
                event = {
                    "type": "change-merged",
                    "change": {
                        "project": "example-project",
                        "branch": "main",
                        "id": "I8473b95934b5732ac55d26311a706c9c2bde9940",
                        "number": "12345",
                        "subject": "Fix bug in authentication module",
                        "owner": {
                            "name": "John Doe",
                            "email": "john.doe@example.com"
                        }
                    },
                    "patchSet": {
                        "number": "2",
                        "revision": "3d4e93e098142c161cd5c1cefbfd424a0f800368",
                        "parents": ["8c0c521a3f83d3934612f4ce35f91c1a6a92382a"]
                    },
                    "submitter": {
                        "name": "Jane Smith",
                        "email": "jane.smith@example.com"
                    }
                }
                
                # Broadcast the event to all connected clients
                await self.broadcast_event(event)
                
                # Wait 5 seconds before sending the next event
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.exception(f"Error in Gerrit event stream: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting


async def main():
    """Main function to start the MCP server."""
    # Initialize Gerrit client
    gerrit_client = GerritClient(GERRIT_URL, GERRIT_USERNAME, GERRIT_PASSWORD)
    
    # Initialize MCP server
    mcp_server = MCPServer(gerrit_client)
    
    # Start WebSocket server for MCP
    async with websockets.serve(mcp_server.register_client, "localhost", MCP_PORT):
        logger.info(f"MCP server started on port {MCP_PORT}")
        
        # Start Gerrit event stream
        gerrit_event_task = asyncio.create_task(mcp_server.start_gerrit_event_stream())
        
        try:
            # Keep the server running indefinitely
            await asyncio.Future()
        except asyncio.CancelledError:
            # Cancel the Gerrit event stream task on server shutdown
            gerrit_event_task.cancel()
            try:
                await gerrit_event_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutting down")
