#!/usr/bin/env python3
"""
MCP Client Example for Testing Gerrit API Server

This example demonstrates how to create a client that uses the Message Consumer Protocol (MCP)
to connect to and interact with the Gerrit MCP server.
"""

import json
import asyncio
import websockets
import logging
import argparse
from typing import Dict, Any, List, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mcp-gerrit-client")

class MCPClient:
    """Client implementing the Message Consumer Protocol for Gerrit events."""
    
    def __init__(self, server_url: str):
        self.server_url = server_url
        self.websocket = None
        self.connected = False
        self.event_handlers = {}
        self.action_responses = {}
        self.action_counter = 0
    
    async def connect(self):
        """Connect to the MCP server."""
        try:
            self.websocket = await websockets.connect(self.server_url)
            self.connected = True
            logger.info(f"Connected to MCP server at {self.server_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from the MCP server."""
        if self.websocket:
            await self.websocket.close()
            self.connected = False
            logger.info("Disconnected from MCP server")
    
    async def subscribe(self, topics: List[str]):
        """Subscribe to topics on the MCP server."""
        if not self.connected:
            logger.error("Not connected to MCP server")
            return False
        
        try:
            message = {
                "type": "subscribe",
                "topics": topics
            }
            await self.websocket.send(json.dumps(message))
            logger.info(f"Subscription request sent for topics: {topics}")
            return True
        except Exception as e:
            logger.error(f"Failed to send subscription request: {e}")
            return False
    
    async def send_action(self, action: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send an action request to the MCP server and wait for the response."""
        if not self.connected:
            logger.error("Not connected to MCP server")
            return None
        
        action_id = self.action_counter
        self.action_counter += 1
        
        try:
            message = {
                "type": "action",
                "action": action,
                "payload": payload,
                "id": action_id
            }
            
            # Create a future to wait for the response
            response_future = asyncio.Future()
            self.action_responses[action_id] = response_future
            
            await self.websocket.send(json.dumps(message))
            logger.info(f"Action request sent: {action} (ID: {action_id})")
            
            # Wait for the response with a timeout
            try:
                return await asyncio.wait_for(response_future, timeout=10.0)
            except asyncio.TimeoutError:
                logger.error(f"Timeout waiting for response to action {action} (ID: {action_id})")
                return None
            finally:
                if action_id in self.action_responses:
                    del self.action_responses[action_id]
        
        except Exception as e:
            logger.error(f"Failed to send action request: {e}")
            return None
    
    def register_event_handler(self, event_type: str, handler):
        """Register a handler function for specific event types."""
        self.event_handlers[event_type] = handler
    
    async def get_change(self, change_id: str) -> Optional[Dict[str, Any]]:
        """Get details about a specific change."""
        response = await self.send_action("get_change", {"change_id": change_id})
        if response and response.get("status") == "success":
            return response.get("result")
        return None
    
    async def add_review(self, change_id: str, revision_id: str, 
                        message: str, labels: Optional[Dict[str, int]] = None) -> Optional[Dict[str, Any]]:
        """Add a review to a change."""
        payload = {
            "change_id": change_id,
            "revision_id": revision_id,
            "message": message,
            "labels": labels or {}
        }
        
        response = await self.send_action("review", payload)
        if response and response.get("status") == "success":
            return response.get("result")
        return None
    
    async def listen(self):
        """Listen for messages from the MCP server."""
        if not self.connected:
            logger.error("Not connected to MCP server")
            return
        
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    await self._process_message(data)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON received: {message}")
                except Exception as e:
                    logger.exception(f"Error processing message: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.info("Connection to MCP server closed")
            self.connected = False
        except Exception as e:
            logger.exception(f"Error in message listener: {e}")
            self.connected = False
    
    async def _process_message(self, data: Dict[str, Any]):
        """Process a message received from the MCP server."""
        msg_type = data.get("type")
        
        if msg_type == "event":
            source = data.get("source")
            payload = data.get("payload", {})
            event_type = payload.get("type")
            
            logger.info(f"Received event from {source}: {event_type}")
            
            # Call the registered handler for this event type
            if event_type in self.event_handlers:
                await self.event_handlers[event_type](payload)
        
        elif msg_type == "subscribed":
            topics = data.get("topics", [])
            logger.info(f"Successfully subscribed to topics: {topics}")
        
        elif msg_type == "action_result":
            action = data.get("action")
            status = data.get("status")
            action_id = data.get("id")
            
            if action_id in self.action_responses:
                self.action_responses[action_id].set_result(data)
                logger.info(f"Received response for action {action} (ID: {action_id}): {status}")
            else:
                logger.warning(f"Received response for unknown action ID: {action_id}")
        
        elif msg_type == "error":
            message = data.get("message", "Unknown error")
            logger.error(f"Error from MCP server: {message}")
        
        else:
            logger.warning(f"Unknown message type received: {msg_type}")


async def handle_change_merged_event(event):
    """Handle change-merged events."""
    change = event.get("change", {})
    patch_set = event.get("patchSet", {})
    submitter = event.get("submitter", {})
    
    logger.info(f"Change merged: {change.get('subject')} ({change.get('id')})")
    logger.info(f"Project: {change.get('project')}, Branch: {change.get('branch')}")
    logger.info(f"Submitted by: {submitter.get('name')} <{submitter.get('email')}>")
    logger.info(f"Patch Set: {patch_set.get('number')}, Revision: {patch_set.get('revision')}")


async def demo_client():
    """Demonstrate the MCP client functionality."""
    # Create MCP client
    client = MCPClient("ws://localhost:8080")
    
    # Connect to the server
    if not await client.connect():
        return
    
    try:
        # Register event handlers
        client.register_event_handler("change-merged", handle_change_merged_event)
        
        # Subscribe to topics
        await client.subscribe(["gerrit-events", "change-merged"])
        
        # Start listening for messages in the background
        listener_task = asyncio.create_task(client.listen())
        
        # Demo: Get change details
        change_id = "I8473b95934b5732ac55d26311a706c9c2bde9940"
        logger.info(f"Getting details for change {change_id}")
        change_details = await client.get_change(change_id)
        
        if change_details:
            logger.info(f"Change details: {json.dumps(change_details, indent=2)}")
        else:
            logger.warning("Failed to get change details")
        
        # Demo: Add a review
        logger.info("Adding a review")
        review_result = await client.add_review(
            change_id=change_id,
            revision_id="3d4e93e098142c161cd5c1cefbfd424a0f800368",
            message="Looks good to me!",
            labels={"Code-Review": 1, "Verified": 1}
        )
        
        if review_result:
            logger.info(f"Review added: {json.dumps(review_result, indent=2)}")
        else:
            logger.warning("Failed to add review")
        
        # Keep the client running to receive events
        await asyncio.sleep(30)  # Listen for events for 30 seconds
        
    finally:
        # Clean up
        if listener_task and not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        
        await client.disconnect()


async def interactive_client():
    """Run an interactive MCP client."""
    # Create MCP client
    client = MCPClient("ws://localhost:8080")
    
    # Connect to the server
    if not await client.connect():
        return
    
    try:
        # Register event handlers
        client.register_event_handler("change-merged", handle_change_merged_event)
        
        # Subscribe to topics
        await client.subscribe(["gerrit-events", "change-merged"])
        
        # Start listening for messages in the background
        listener_task = asyncio.create_task(client.listen())
        
        print("\nMCP Gerrit Client Interactive Mode")
        print("----------------------------------")
        print("Available commands:")
        print("  1: Subscribe to topics")
        print("  2: Get change details")
        print("  3: Add a review")
        print("  q: Quit")
        
        while True:
            command = input("\nEnter command: ")
            
            if command == "q":
                break
            
            elif command == "1":
                topics_str = input("Enter topics (comma-separated): ")
                topics = [t.strip() for t in topics_str.split(",")]
                await client.subscribe(topics)
            
            elif command == "2":
                change_id = input("Enter change ID: ")
                change_details = await client.get_change(change_id)
                if change_details:
                    print(f"Change details: {json.dumps(change_details, indent=2)}")
                else:
                    print("Failed to get change details")
            
            elif command == "3":
                change_id = input("Enter change ID: ")
                revision_id = input("Enter revision ID: ")
                message = input("Enter review message: ")
                
                labels = {}
                add_labels = input("Add labels? (y/n): ")
                if add_labels.lower() == "y":
                    code_review = input("Code-Review score (-2 to +2): ")
                    verified = input("Verified score (-1 to +1): ")
                    
                    if code_review:
                        labels["Code-Review"] = int(code_review)
                    if verified:
                        labels["Verified"] = int(verified)
                
                review_result = await client.add_review(change_id, revision_id, message, labels)
                if review_result:
                    print(f"Review added: {json.dumps(review_result, indent=2)}")
                else:
                    print("Failed to add review")
            
            else:
                print("Unknown command")
    
    finally:
        # Clean up
        if listener_task and not listener_task.done():
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass
        
        await client.disconnect()


def main():
    """Parse command line arguments and run the appropriate client mode."""
    parser = argparse.ArgumentParser(description="MCP Client for Gerrit API Server")
    parser.add_argument(
        "--server", default="ws://localhost:8080",
        help="WebSocket URL of the MCP server (default: ws://localhost:8080)"
    )
    parser.add_argument(
        "--interactive", action="store_true",
        help="Run in interactive mode"
    )
    
    args = parser.parse_args()
    
    try:
        if args.interactive:
            asyncio.run(interactive_client())
        else:
            asyncio.run(demo_client())
    except KeyboardInterrupt:
        logger.info("Client shutting down")


if __name__ == "__main__":
    main()
