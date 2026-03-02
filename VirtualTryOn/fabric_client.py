"""
Fabric Event Hub Client
Sends order and combination data to Microsoft Fabric via Azure Event Hubs.
Uses connection strings for authentication (required for external apps).
"""

import os
import json
import uuid
import hashlib
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

# Event Hub connection strings from environment variables
EH_SALES_CONNECTION_STRING = os.environ.get("FABRIC_EH_SALES_CONNECTION_STRING", "")
EH_COMBINATIONS_CONNECTION_STRING = os.environ.get("FABRIC_EH_COMBINATIONS_CONNECTION_STRING", "")


class FabricClient:
    """Client for sending data to Microsoft Fabric Event Hubs."""
    
    def __init__(self):
        self._sales_producer = None
        self._combinations_producer = None
        self._initialized = False
    
    def _initialize(self):
        """Initialize the Event Hub producers using connection strings."""
        if self._initialized:
            return
        
        try:
            # Create Event Hub producers using connection strings
            if EH_SALES_CONNECTION_STRING:
                self._sales_producer = EventHubProducerClient.from_connection_string(
                    conn_str=EH_SALES_CONNECTION_STRING
                )
            
            if EH_COMBINATIONS_CONNECTION_STRING:
                self._combinations_producer = EventHubProducerClient.from_connection_string(
                    conn_str=EH_COMBINATIONS_CONNECTION_STRING
                )
            
            self._initialized = True
            print("✅ Fabric Event Hub client initialized successfully")
            
        except Exception as e:
            print(f"⚠️ Failed to initialize Fabric client: {e}")
            raise
    
    def _generate_combination_id(self, items: list) -> str:
        """
        Generate a deterministic combination_id based on the items.
        Same items will always produce the same combination_id.
        """
        # Sort items by product_id to ensure consistent ordering
        sorted_ids = sorted([item.get("id", "") for item in items])
        # Create a hash of the sorted product IDs
        items_string = "|".join(sorted_ids)
        hash_digest = hashlib.sha256(items_string.encode()).hexdigest()
        # Return first 32 chars as a UUID-like string
        return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"
    
    def send_combination(self, user_id: str, items: list) -> str:
        """
        Send a combination (outfit) to the Combinations event stream.
        
        Args:
            user_id: The user identifier
            items: List of items with product_id, name, price, color
            
        Returns:
            The generated combination_id (deterministic based on items)
        """
        self._initialize()
        
        # Generate deterministic combination_id based on items
        combination_id = self._generate_combination_id(items)
        
        combination_data = {
            "combination_id": combination_id,
            "user_id": user_id,
            "items": [
                {
                    "product_id": item.get("id", ""),
                    "name": item.get("name", ""),
                    "price": float(item.get("price", 0)),
                    "color": item.get("color", "")
                }
                for item in items
            ]
        }
        
        try:
            event_data_batch = self._combinations_producer.create_batch()
            event_data_batch.add(EventData(json.dumps(combination_data)))
            self._combinations_producer.send_batch(event_data_batch)
            print(f"📤 Sent combination {combination_id} to Fabric")
            return combination_id
        except Exception as e:
            print(f"❌ Failed to send combination: {e}")
            raise
    
    def send_order(self, user_id: str, combination_id: str, items: list) -> str:
        """
        Send an order to the Sales event stream.
        
        Args:
            user_id: The user identifier
            combination_id: The combination ID this order is based on
            items: List of items with product_id, name, price, color
            
        Returns:
            The generated order_id
        """
        self._initialize()
        
        order_id = str(uuid.uuid4())
        
        order_data = {
            "order_id": order_id,
            "combination_id": combination_id,
            "user_id": user_id,
            "items": [
                {
                    "product_id": item.get("id", ""),
                    "name": item.get("name", ""),
                    "price": float(item.get("price", 0)),
                    "color": item.get("color", "")
                }
                for item in items
            ]
        }
        
        try:
            event_data_batch = self._sales_producer.create_batch()
            event_data_batch.add(EventData(json.dumps(order_data)))
            self._sales_producer.send_batch(event_data_batch)
            print(f"📤 Sent order {order_id} to Fabric")
            return order_id
        except Exception as e:
            print(f"❌ Failed to send order: {e}")
            raise
    
    def close(self):
        """Close the Event Hub producers."""
        if self._sales_producer:
            self._sales_producer.close()
        if self._combinations_producer:
            self._combinations_producer.close()
        self._initialized = False


# Singleton instance
_fabric_client = None


def get_fabric_client() -> FabricClient:
    """Get the singleton Fabric client instance."""
    global _fabric_client
    if _fabric_client is None:
        _fabric_client = FabricClient()
    return _fabric_client
