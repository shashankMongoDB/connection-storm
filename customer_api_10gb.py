#!/usr/bin/env python3
"""
Customer API Microservice - 10GB Data Edition

A standalone FastAPI application that provides CRUD operations for customer data
stored in MongoDB. Specifically designed to work with the 10GB customer dataset.

Features:
- Complete CRUD operations for customer data
- Connection pooling for MongoDB
- Error handling and logging
- Health check endpoints
- Self-contained configuration management
- Optimized for high-volume operations
"""

import os
import sys
import time
import json
import uuid
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# FastAPI imports
from fastapi import FastAPI, HTTPException, Query, Path, Body, Depends, status, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, EmailStr

# MongoDB imports
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError, ServerSelectionTimeoutError
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("customer-api-10gb")

# -------------------------------------------------------------------------
# Configuration Management - Self-contained
# -------------------------------------------------------------------------

def load_config(config_file="config_10gb.properties"):
    """
    Load configuration from a properties file.
    If file doesn't exist, use environment variables or defaults.
    """
    config = {
        # Default configuration values
        "connection_uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        "database_name": os.getenv("MONGO_DB", "customers_db"),
        "collection_name": os.getenv("MONGO_COLLECTION", "customers"),
        "username": os.getenv("MONGO_USERNAME", ""),
        "password": os.getenv("MONGO_PASSWORD", ""),
        "tls_enabled": os.getenv("MONGO_TLS_ENABLED", "false").lower() == "true",
        "tls_ca_file_path": os.getenv("MONGO_TLS_CA_FILE", ""),
        "customer_api_port": int(os.getenv("CUSTOMER_API_PORT", "8000")),
        "write_concern": os.getenv("MONGO_WRITE_CONCERN", "majority"),
        "max_pool_size": int(os.getenv("MONGO_MAX_POOL_SIZE", "100")),
        "min_pool_size": int(os.getenv("MONGO_MIN_POOL_SIZE", "10")),
        "max_idle_time_ms": int(os.getenv("MONGO_MAX_IDLE_TIME_MS", "30000")),
    }
    
    # Try to load from properties file if it exists
    if os.path.exists(config_file):
        logger.info(f"Loading configuration from {config_file}")
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Convert boolean strings
                    if value.lower() in ["true", "false"]:
                        value = value.lower() == "true"
                    
                    # Try to convert to int if it looks like a number
                    if isinstance(value, str) and value.isdigit():
                        value = int(value)
                    
                    config[key] = value
    else:
        logger.warning(f"Configuration file {config_file} not found, using environment variables and defaults")
    
    return config

# Load configuration
CONFIG = load_config()

# -------------------------------------------------------------------------
# MongoDB Connection Management
# -------------------------------------------------------------------------

class MongoDBManager:
    _instance = None
    _lock = threading.Lock()
    client = None
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(MongoDBManager, cls).__new__(cls)
                cls._instance._connect()
            return cls._instance
    
    def _connect(self):
        """Connect to MongoDB with connection pooling"""
        try:
            # Configure connection pool
            connection_params = {
                "maxPoolSize": CONFIG.get("max_pool_size", 100),
                "minPoolSize": CONFIG.get("min_pool_size", 10),
                "maxIdleTimeMS": CONFIG.get("max_idle_time_ms", 30000),
                "waitQueueTimeoutMS": 10000,
                "connectTimeoutMS": 5000,
                "serverSelectionTimeoutMS": 5000,
                "retryWrites": True,
                # Enforce majority write concern for stronger durability guarantees
                "w": CONFIG.get("write_concern", "majority"),
            }
            
            # Add TLS settings if configured
            if CONFIG.get("tls_enabled", False):
                connection_params["tls"] = True
                if CONFIG.get("tls_ca_file_path"):
                    connection_params["tlsCAFile"] = CONFIG.get("tls_ca_file_path")
            
            # Add credentials if provided and not in URI
            if CONFIG.get("username") and CONFIG.get("password"):
                connection_params["username"] = CONFIG.get("username")
                connection_params["password"] = CONFIG.get("password")
            
            # Create client with connection pool
            self.client = MongoClient(
                CONFIG["connection_uri"], 
                **connection_params
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            logger.info(f"Connected to MongoDB ({CONFIG['connection_uri']})")
            logger.info(f"Using database: {CONFIG['database_name']}, collection: {CONFIG['collection_name']}")
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            sys.exit(1)
    
    def get_db(self):
        """Get database instance"""
        return self.client[CONFIG["database_name"]]
    
    def get_collection(self):
        """Get collection instance"""
        return self.get_db()[CONFIG["collection_name"]]
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

# -------------------------------------------------------------------------
# Pydantic Models
# -------------------------------------------------------------------------

class Address(BaseModel):
    """Customer address model"""
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

class CustomerBase(BaseModel):
    """Base customer model with common fields"""
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    address: Optional[Address] = None
    tags: List[str] = Field(default_factory=list)
    account_status: Optional[str] = "active"

class CustomerCreate(CustomerBase):
    """Model for creating a new customer"""
    pass

class CustomerUpdate(BaseModel):
    """Model for updating an existing customer"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[Address] = None
    tags: Optional[List[str]] = None
    account_status: Optional[str] = None

class CustomerResponse(CustomerBase):
    """Model for customer response"""
    id: str
    customer_id: str
    created_at: str
    updated_at: str

    class Config:
        orm_mode = True

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    version: str
    database_connected: bool
    database_name: str
    collection_name: str
    uptime_seconds: float

# -------------------------------------------------------------------------
# FastAPI Application
# -------------------------------------------------------------------------

app = FastAPI(
    title="Customer API - 10GB Dataset",
    description="API for managing customer data from the 10GB dataset",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize MongoDB connection
mongo_manager = MongoDBManager()

# Track application start time for uptime metric
start_time = time.time()

# -------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------

def get_current_timestamp():
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()

def format_customer_response(customer):
    """Format customer document for API response"""
    if not customer:
        return None
    
    # Ensure ID is properly formatted
    customer_id = str(customer.get("_id", ""))
    if "_id" in customer:
        del customer["_id"]
    
    # Add ID field for API response
    customer["id"] = customer_id
    
    # Ensure timestamps are strings
    for ts_field in ["created_at", "updated_at"]:
        if ts_field in customer and not isinstance(customer[ts_field], str):
            customer[ts_field] = str(customer[ts_field])
    
    return customer

# -------------------------------------------------------------------------
# Routes - Health Check
# -------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo_manager.client.admin.command('ping')
        
        return {
            "status": "ok",
            "timestamp": get_current_timestamp(),
            "version": "1.0.0",
            "database_connected": True,
            "database_name": CONFIG["database_name"],
            "collection_name": CONFIG["collection_name"],
            "uptime_seconds": time.time() - start_time
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "timestamp": get_current_timestamp(),
            "version": "1.0.0",
            "database_connected": False,
            "database_name": CONFIG["database_name"],
            "collection_name": CONFIG["collection_name"],
            "uptime_seconds": time.time() - start_time
        }

# -------------------------------------------------------------------------
# Routes - Customer Management
# -------------------------------------------------------------------------

@app.post("/customers", response_model=CustomerResponse, status_code=status.HTTP_201_CREATED, tags=["Customers"])
async def create_customer(customer: CustomerCreate):
    """Create a new customer"""
    try:
        collection = mongo_manager.get_collection()
        
        # Generate customer ID and timestamps
        customer_id = f"CUST{uuid.uuid4().hex[:8].upper()}"
        timestamp = get_current_timestamp()
        
        # Create customer document
        customer_dict = customer.dict()
        customer_doc = {
            "customer_id": customer_id,
            "created_at": timestamp,
            "updated_at": timestamp,
            **customer_dict
        }
        
        # Insert into MongoDB
        result = collection.insert_one(customer_doc)
        
        # Return created customer
        customer_doc["id"] = str(result.inserted_id)
        return customer_doc
    
    except PyMongoError as e:
        logger.error(f"MongoDB error creating customer: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error creating customer: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/customers", tags=["Customers"])
async def list_customers(
    skip: int = Query(0, ge=0, description="Number of customers to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of customers to return"),
    status: Optional[str] = Query(None, description="Filter by account status"),
    sort_by: str = Query("created_at", description="Field to sort by"),
    sort_order: int = Query(-1, ge=-1, le=1, description="Sort order (-1 for descending, 1 for ascending)")
):
    """List customers with pagination, filtering and sorting"""
    try:
        collection = mongo_manager.get_collection()
        
        # Build query filter
        query_filter = {}
        if status:
            query_filter["account_status"] = status
        
        # Execute query with pagination and sorting
        customers = list(
            collection.find(query_filter)
            .sort(sort_by, sort_order)
            .skip(skip)
            .limit(limit)
        )
        
        # Format response
        formatted_customers = [format_customer_response(c) for c in customers]
        
        # Get total count (with same filter)
        total_count = collection.count_documents(query_filter)
        
        return {
            "customers": formatted_customers,
            "total": total_count,
            "skip": skip,
            "limit": limit
        }
    
    except PyMongoError as e:
        logger.error(f"MongoDB error listing customers: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error listing customers: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/customers/{customer_id}", response_model=CustomerResponse, tags=["Customers"])
async def get_customer(customer_id: str = Path(..., description="Customer ID")):
    """Get a specific customer by ID"""
    try:
        collection = mongo_manager.get_collection()
        
        # Try to find by MongoDB ObjectId first
        customer = None
        if len(customer_id) == 24:  # Possible MongoDB ObjectId
            try:
                from bson.objectid import ObjectId
                customer = collection.find_one({"_id": ObjectId(customer_id)})
            except:
                pass
        
        # If not found, try by customer_id field
        if not customer:
            customer = collection.find_one({"customer_id": customer_id})
        
        if not customer:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        
        # Format response
        return format_customer_response(customer)
    
    except HTTPException:
        raise
    
    except PyMongoError as e:
        logger.error(f"MongoDB error getting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error getting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.put("/customers/{customer_id}", response_model=CustomerResponse, tags=["Customers"])
async def update_customer(
    customer_id: str = Path(..., description="Customer ID"),
    customer_update: CustomerUpdate = Body(...)
):
    """Update a specific customer"""
    try:
        collection = mongo_manager.get_collection()
        
        # Check if customer exists
        existing = None
        if len(customer_id) == 24:  # Possible MongoDB ObjectId
            try:
                from bson.objectid import ObjectId
                existing = collection.find_one({"_id": ObjectId(customer_id)})
            except:
                pass
        
        # If not found, try by customer_id field
        if not existing:
            existing = collection.find_one({"customer_id": customer_id})
        
        if not existing:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        
        # Prepare update document
        update_dict = {k: v for k, v in customer_update.dict().items() if v is not None}
        update_dict["updated_at"] = get_current_timestamp()
        
        # Update in MongoDB
        result = collection.update_one(
            {"_id": existing["_id"]},
            {"$set": update_dict}
        )
        
        if result.modified_count == 0:
            logger.warning(f"Customer {customer_id} update had no effect")
        
        # Get updated customer
        updated = collection.find_one({"_id": existing["_id"]})
        
        # Format response
        return format_customer_response(updated)
    
    except HTTPException:
        raise
    
    except PyMongoError as e:
        logger.error(f"MongoDB error updating customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error updating customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.delete("/customers/{customer_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Customers"])
async def delete_customer(customer_id: str = Path(..., description="Customer ID")):
    """Delete a specific customer"""
    try:
        collection = mongo_manager.get_collection()
        
        # Check if customer exists
        existing = None
        if len(customer_id) == 24:  # Possible MongoDB ObjectId
            try:
                from bson.objectid import ObjectId
                existing = collection.find_one({"_id": ObjectId(customer_id)})
            except:
                pass
        
        # If not found, try by customer_id field
        if not existing:
            existing = collection.find_one({"customer_id": customer_id})
        
        if not existing:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
        
        # Delete from MongoDB
        result = collection.delete_one({"_id": existing["_id"]})
        
        if result.deleted_count == 0:
            logger.warning(f"Customer {customer_id} deletion had no effect")
            raise HTTPException(status_code=500, detail="Failed to delete customer")
        
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    
    except HTTPException:
        raise
    
    except PyMongoError as e:
        logger.error(f"MongoDB error deleting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error deleting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/customers/search", tags=["Customers"])
async def search_customers(
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results to return")
):
    """Search customers by name, email, or phone"""
    try:
        collection = mongo_manager.get_collection()
        
        # Build search query
        search_query = {
            "$or": [
                {"first_name": {"$regex": query, "$options": "i"}},
                {"last_name": {"$regex": query, "$options": "i"}},
                {"email": {"$regex": query, "$options": "i"}},
                {"phone": {"$regex": query, "$options": "i"}}
            ]
        }
        
        # Execute search
        results = list(collection.find(search_query).limit(limit))
        
        # Format response
        formatted_results = [format_customer_response(c) for c in results]
        
        return {
            "results": formatted_results,
            "count": len(formatted_results),
            "query": query
        }
    
    except PyMongoError as e:
        logger.error(f"MongoDB error searching customers: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error searching customers: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/customers/stats", tags=["Customers"])
async def customer_statistics():
    """Get statistics about customers"""
    try:
        collection = mongo_manager.get_collection()
        
        # Get total count
        total_count = collection.count_documents({})
        
        # Get status distribution
        status_pipeline = [
            {"$group": {"_id": "$account_status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        status_results = list(collection.aggregate(status_pipeline))
        
        # Get country distribution
        country_pipeline = [
            {"$match": {"address.country": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        country_results = list(collection.aggregate(country_pipeline))
        
        # Get registration date distribution (by month)
        date_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "month": {"$substr": ["$created_at", 0, 7]}  # Get YYYY-MM part
            }},
            {"$group": {"_id": "$month", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        date_results = list(collection.aggregate(date_pipeline))
        
        return {
            "total_customers": total_count,
            "status_distribution": status_results,
            "top_countries": country_results,
            "registration_by_month": date_results
        }
    
    except PyMongoError as e:
        logger.error(f"MongoDB error getting customer statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error getting customer statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# -------------------------------------------------------------------------
# Application Shutdown
# -------------------------------------------------------------------------

@app.on_event("shutdown")
def shutdown_event():
    """Close MongoDB connection when the application shuts down"""
    try:
        mongo_manager.close()
    except:
        pass

# -------------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    
    port = CONFIG.get("customer_api_port", 8000)
    logger.info(f"Starting Customer API on port {port}")
    
    uvicorn.run(app, host="0.0.0.0", port=port)
