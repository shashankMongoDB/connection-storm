#!/usr/bin/env python3
"""
Customer API Microservice

A standalone Flask application that provides CRUD operations for customer data
stored in MongoDB. Designed to work with the 10GB customer dataset and
optimized for high-volume load testing across multiple VMs.

Features:
- Complete CRUD operations for customer data
- Connection pooling for MongoDB with singleton pattern
- Large payload generation for stress testing
- Heavy computation endpoints for load testing
- Health check and monitoring endpoints
- Self-contained configuration management
- Proper error handling and logging
"""

import os
import sys
import time
import json
import uuid
import random
import string
import logging
import threading
import atexit
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

# Flask imports
from flask import Flask, request, jsonify, Response, make_response, g
from flask_cors import CORS

# MongoDB imports
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError, ServerSelectionTimeoutError
from bson.objectid import ObjectId
from bson.json_util import dumps, loads

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("customer-api")

# -------------------------------------------------------------------------
# Configuration Management
# -------------------------------------------------------------------------

def load_config(config_file="config.properties"):
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
        "api_port": int(os.getenv("API_PORT", "5000")),
        "write_concern": os.getenv("MONGO_WRITE_CONCERN", "majority"),
        "max_pool_size": int(os.getenv("MONGO_MAX_POOL_SIZE", "100")),
        "min_pool_size": int(os.getenv("MONGO_MIN_POOL_SIZE", "10")),
        "max_idle_time_ms": int(os.getenv("MONGO_MAX_IDLE_TIME_MS", "30000")),
        "cache_ttl_seconds": int(os.getenv("CACHE_TTL_SECONDS", "300")),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "debug_mode": os.getenv("DEBUG_MODE", "false").lower() == "true",
        "max_payload_size_kb": int(os.getenv("MAX_PAYLOAD_SIZE_KB", "1024")),
        "instance_id": os.getenv("INSTANCE_ID", f"instance-{uuid.uuid4().hex[:8]}"),
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
    
    # Set log level based on configuration
    log_level = getattr(logging, config["log_level"].upper(), logging.INFO)
    logger.setLevel(log_level)
    
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
    
    def get_connection_stats(self):
        """Get connection pool statistics"""
        try:
            server_status = self.client.admin.command('serverStatus')
            connections = server_status.get('connections', {})
            return {
                'current': connections.get('current', 0),
                'available': connections.get('available', 0),
                'total_created': connections.get('totalCreated', 0),
                'active': connections.get('active', 0),
                'max_pool_size': CONFIG.get("max_pool_size", 100),
                'min_pool_size': CONFIG.get("min_pool_size", 10),
            }
        except Exception as e:
            logger.error(f"Error getting connection stats: {e}")
            return {
                'error': str(e)
            }
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

# -------------------------------------------------------------------------
# Simple Cache Implementation
# -------------------------------------------------------------------------

class SimpleCache:
    def __init__(self, ttl_seconds=300):
        self.cache = {}
        self.ttl_seconds = ttl_seconds
        self.lock = threading.Lock()
    
    def get(self, key):
        """Get value from cache if not expired"""
        with self.lock:
            if key in self.cache:
                timestamp, value = self.cache[key]
                if datetime.now() < timestamp:
                    return value
                # Remove expired entry
                del self.cache[key]
            return None
    
    def set(self, key, value):
        """Set value in cache with expiration"""
        expiry = datetime.now() + timedelta(seconds=self.ttl_seconds)
        with self.lock:
            self.cache[key] = (expiry, value)
    
    def clear(self):
        """Clear all cache entries"""
        with self.lock:
            self.cache.clear()
    
    def cleanup(self):
        """Remove expired entries"""
        now = datetime.now()
        with self.lock:
            expired_keys = [k for k, (exp, _) in self.cache.items() if now >= exp]
            for key in expired_keys:
                del self.cache[key]

# Initialize cache
cache = SimpleCache(ttl_seconds=CONFIG.get("cache_ttl_seconds", 300))

# -------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------

def get_current_timestamp():
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()

def generate_cache_key(endpoint, params=None):
    """Generate a cache key based on endpoint and parameters"""
    if params:
        return f"{endpoint}:{json.dumps(params, sort_keys=True)}"
    return endpoint

def json_response(data):
    """Convert MongoDB BSON to JSON response"""
    return Response(
        dumps(data, default=str),
        mimetype='application/json'
    )

def generate_random_string(length=10):
    """Generate a random string of fixed length"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_email():
    """Generate a random email address"""
    domains = ['example.com', 'test.org', 'demo.net', 'sample.io', 'mock.dev']
    username = generate_random_string(8).lower()
    domain = random.choice(domains)
    return f"{username}@{domain}"

def generate_random_phone():
    """Generate a random phone number"""
    area_code = random.randint(100, 999)
    prefix = random.randint(100, 999)
    line = random.randint(1000, 9999)
    return f"{area_code}-{prefix}-{line}"

def generate_random_address():
    """Generate a random address"""
    street_number = random.randint(1, 9999)
    street_names = ['Main', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm', 'Washington', 'Park', 'Lake', 'Hill']
    street_types = ['St', 'Ave', 'Blvd', 'Rd', 'Dr', 'Ln', 'Way', 'Pl', 'Ct']
    cities = ['Springfield', 'Franklin', 'Greenville', 'Bristol', 'Clinton', 'Salem', 'Madison', 'Georgetown', 'Arlington', 'Fairview']
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD']
    countries = ['USA', 'Canada', 'Mexico', 'UK', 'France', 'Germany', 'Australia', 'Japan', 'Brazil', 'India']
    
    return {
        'street': f"{street_number} {random.choice(street_names)} {random.choice(street_types)}",
        'city': random.choice(cities),
        'state': random.choice(states),
        'postal_code': f"{random.randint(10000, 99999)}",
        'country': random.choice(countries)
    }

def generate_large_customer_payload(size_kb=None):
    """Generate a large customer payload for stress testing"""
    if size_kb is None:
        size_kb = random.randint(10, CONFIG.get("max_payload_size_kb", 1024))
    
    # Base customer data
    customer = {
        "customer_id": str(uuid.uuid4()),
        "first_name": generate_random_string(8),
        "last_name": generate_random_string(12),
        "email": generate_random_email(),
        "phone": generate_random_phone(),
        "address": generate_random_address(),
        "account_status": random.choice(["active", "inactive", "pending", "suspended"]),
        "created_at": get_current_timestamp(),
        "updated_at": get_current_timestamp(),
        "credit_score": random.randint(300, 850),
        "annual_income": round(random.uniform(20000, 200000), 2),
        "date_of_birth": f"{random.randint(1950, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        "preferences": {
            "language": random.choice(["en", "es", "fr", "de", "zh"]),
            "currency": random.choice(["USD", "EUR", "GBP", "CAD", "JPY"]),
            "notifications": {
                "email": random.choice([True, False]),
                "sms": random.choice([True, False]),
                "push": random.choice([True, False])
            },
            "marketing_emails": random.choice([True, False]),
            "two_factor_auth": random.choice([True, False]),
            "theme": random.choice(["light", "dark", "system"]),
            "timezone": random.choice(["UTC", "America/New_York", "Europe/London", "Asia/Tokyo"]),
            "privacy_settings": {
                "share_data": random.choice([True, False]),
                "analytics": random.choice([True, False]),
                "third_party": random.choice([True, False])
            },
            "display_settings": {
                "items_per_page": random.choice([10, 20, 50, 100]),
                "show_images": random.choice([True, False]),
                "compact_view": random.choice([True, False])
            }
        },
        "tags": [generate_random_string(6) for _ in range(random.randint(0, 5))],
        "loyalty_points": random.randint(0, 10000),
        "purchase_history": []
    }
    
    # Add purchase history to increase payload size
    num_purchases = random.randint(5, 20)
    for _ in range(num_purchases):
        purchase = {
            "purchase_id": str(uuid.uuid4()),
            "date": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat(),
            "items": [],
            "total_amount": 0,
            "payment_method": random.choice(["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer", "Cash"]),
            "shipping_address": f"{random.randint(100, 999)} {generate_random_string(8)} St, {generate_random_string(8)}, {generate_random_string(2).upper()} {random.randint(10000, 99999)}",
            "status": random.choice(["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]),
            "tracking_number": f"{generate_random_string(2).upper()}{random.randint(10, 99)}{generate_random_string(2).upper()}{random.randint(10, 99)}"
        }
        
        # Add items to purchase
        num_items = random.randint(1, 10)
        total_amount = 0
        for _ in range(num_items):
            price = round(random.uniform(10, 500), 2)
            quantity = random.randint(1, 5)
            discount = round(random.uniform(0, 0.3), 2)
            item_total = price * quantity * (1 - discount)
            total_amount += item_total
            
            item = {
                "product_id": str(uuid.uuid4()),
                "product_name": f"{generate_random_string(6)} {generate_random_string(8)}",
                "category": generate_random_string(8),
                "quantity": quantity,
                "price": price,
                "discount": discount,
                "rating": random.randint(1, 5),
                "reviews": [generate_random_string(20) for _ in range(random.randint(0, 3))]
            }
            purchase["items"].append(item)
        
        purchase["total_amount"] = round(total_amount, 2)
        customer["purchase_history"].append(purchase)
    
    # Add a large profile data field to reach desired size
    customer["profile_data"] = generate_random_string(size_kb * 1024 - len(json.dumps(customer)))
    
    # Add metadata
    customer["metadata"] = {
        "browser": f"Mozilla/5.0 ({random.choice(['Windows', 'Macintosh', 'Linux', 'Android', 'iOS'])}) Chrome/{random.randint(70, 100)}.0",
        "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "last_updated": get_current_timestamp(),
        "session_id": str(uuid.uuid4()),
        "device_info": {
            "type": random.choice(["desktop", "mobile", "tablet"]),
            "os": random.choice(["Windows", "MacOS", "Linux", "Android", "iOS"]),
            "screen_resolution": random.choice(["1920x1080", "1366x768", "2560x1440", "3840x2160", "750x1334"]),
            "browser_language": random.choice(["en-US", "es-ES", "fr-FR", "de-DE", "zh-CN"]),
            "user_agent_details": {
                "browser_name": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "browser_version": f"{random.randint(70, 100)}.{random.randint(0, 9)}",
                "operating_system": random.choice(["Windows 10", "MacOS 12", "Ubuntu 22.04", "Android 12", "iOS 15"])
            }
        }
    }
    
    return customer

# -------------------------------------------------------------------------
# Flask Application
# -------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Track application start time for uptime metric
start_time = time.time()

# Initialize MongoDB connection
mongo_manager = MongoDBManager()

# -------------------------------------------------------------------------
# Routes - Health Check
# -------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo_manager.client.admin.command('ping')
        
        # Get connection stats
        connection_stats = mongo_manager.get_connection_stats()
        
        return jsonify({
            "status": "ok",
            "timestamp": get_current_timestamp(),
            "service": "customer-api",
            "version": "1.0.0",
            "instance_id": CONFIG["instance_id"],
            "database_connected": True,
            "database_name": CONFIG["database_name"],
            "collection_name": CONFIG["collection_name"],
            "uptime_seconds": time.time() - start_time,
            "connection_stats": connection_stats
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "error",
            "message": "Database connection failed",
            "timestamp": get_current_timestamp(),
            "instance_id": CONFIG["instance_id"],
            "database_connected": False,
            "uptime_seconds": time.time() - start_time,
            "error": str(e)
        }), 503

@app.route('/metrics', methods=['GET'])
def metrics():
    """Metrics endpoint for monitoring"""
    try:
        collection = mongo_manager.get_collection()
        
        # Get basic metrics
        customer_count = collection.count_documents({})
        active_count = collection.count_documents({"account_status": "active"})
        
        # Get connection stats
        connection_stats = mongo_manager.get_connection_stats()
        
        # Get memory usage (approximate)
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        return jsonify({
            "timestamp": get_current_timestamp(),
            "instance_id": CONFIG["instance_id"],
            "uptime_seconds": time.time() - start_time,
            "database_metrics": {
                "total_customers": customer_count,
                "active_customers": active_count,
                "active_percentage": (active_count / customer_count * 100) if customer_count > 0 else 0
            },
            "connection_stats": connection_stats,
            "system_metrics": {
                "memory_usage_mb": memory_info.rss / (1024 * 1024),
                "cpu_percent": process.cpu_percent(interval=0.1)
            }
        })
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Customer CRUD Operations
# -------------------------------------------------------------------------

@app.route('/customers', methods=['GET'])
def list_customers():
    """List customers with filtering and pagination"""
    try:
        # Get query parameters
        limit = min(int(request.args.get("limit", "100")), 1000)
        skip = int(request.args.get("skip", "0"))
        sort_by = request.args.get("sort_by", "created_at")
        sort_order = request.args.get("sort_order", "desc")
        status = request.args.get("status")
        country = request.args.get("country")
        search = request.args.get("search")
        
        # Build query
        query = {}
        if status:
            query["account_status"] = status
        if country:
            query["address.country"] = country
        if search:
            query["$or"] = [
                {"first_name": {"$regex": search, "$options": "i"}},
                {"last_name": {"$regex": search, "$options": "i"}},
                {"email": {"$regex": search, "$options": "i"}}
            ]
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Execute query with pagination and sorting
        sort_direction = pymongo.DESCENDING if sort_order.lower() == "desc" else pymongo.ASCENDING
        cursor = collection.find(query).sort(sort_by, sort_direction).skip(skip).limit(limit)
        customers = list(cursor)
        
        # Get total count for pagination
        total_count = collection.count_documents(query)
        
        return json_response({
            "customers": customers,
            "count": len(customers),
            "total": total_count,
            "limit": limit,
            "skip": skip,
            "has_more": (skip + len(customers)) < total_count
        })
    
    except Exception as e:
        logger.error(f"Error listing customers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers/<customer_id>', methods=['GET'])
def get_customer(customer_id):
    """Get a specific customer by ID"""
    try:
        collection = mongo_manager.get_collection()
        
        # Try to find the customer
        customer = collection.find_one({"customer_id": customer_id})
        
        if not customer:
            return jsonify({"error": "Customer not found"}), 404
        
        return json_response(customer)
    
    except Exception as e:
        logger.error(f"Error getting customer {customer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers', methods=['POST'])
def create_customer():
    """Create a new customer"""
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Generate customer_id if not provided
        if "customer_id" not in data:
            data["customer_id"] = str(uuid.uuid4())
        
        # Add timestamps
        current_time = get_current_timestamp()
        data["created_at"] = current_time
        data["updated_at"] = current_time
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Check if customer already exists
        existing = collection.find_one({"customer_id": data["customer_id"]})
        if existing:
            return jsonify({"error": "Customer with this ID already exists"}), 409
        
        # Insert the customer
        result = collection.insert_one(data)
        
        if result.acknowledged:
            return json_response({
                "message": "Customer created successfully",
                "customer_id": data["customer_id"],
                "inserted_id": str(result.inserted_id)
            }), 201
        else:
            return jsonify({"error": "Failed to create customer"}), 500
    
    except Exception as e:
        logger.error(f"Error creating customer: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers/<customer_id>', methods=['PUT'])
def update_customer(customer_id):
    """Update an existing customer"""
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Prevent changing customer_id
        if "customer_id" in data and data["customer_id"] != customer_id:
            return jsonify({"error": "Cannot change customer_id"}), 400
        
        # Update timestamp
        data["updated_at"] = get_current_timestamp()
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Check if customer exists
        existing = collection.find_one({"customer_id": customer_id})
        if not existing:
            return jsonify({"error": "Customer not found"}), 404
        
        # Update the customer
        result = collection.update_one(
            {"customer_id": customer_id},
            {"$set": data}
        )
        
        if result.acknowledged:
            return json_response({
                "message": "Customer updated successfully",
                "customer_id": customer_id,
                "modified_count": result.modified_count
            })
        else:
            return jsonify({"error": "Failed to update customer"}), 500
    
    except Exception as e:
        logger.error(f"Error updating customer {customer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    """Delete a customer"""
    try:
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Check if customer exists
        existing = collection.find_one({"customer_id": customer_id})
        if not existing:
            return jsonify({"error": "Customer not found"}), 404
        
        # Delete the customer
        result = collection.delete_one({"customer_id": customer_id})
        
        if result.acknowledged:
            return jsonify({
                "message": "Customer deleted successfully",
                "customer_id": customer_id,
                "deleted_count": result.deleted_count
            })
        else:
            return jsonify({"error": "Failed to delete customer"}), 500
    
    except Exception as e:
        logger.error(f"Error deleting customer {customer_id}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers/batch', methods=['POST'])
def batch_create_customers():
    """Create multiple customers in a single request"""
    try:
        # Get request data
        data = request.get_json()
        if not data or not isinstance(data, list):
            return jsonify({"error": "Expected an array of customers"}), 400
        
        if len(data) > 1000:
            return jsonify({"error": "Batch size exceeds maximum (1000)"}), 400
        
        # Process each customer
        current_time = get_current_timestamp()
        for customer in data:
            # Generate customer_id if not provided
            if "customer_id" not in customer:
                customer["customer_id"] = str(uuid.uuid4())
            
            # Add timestamps
            customer["created_at"] = current_time
            customer["updated_at"] = current_time
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Insert the customers
        result = collection.insert_many(data)
        
        if result.acknowledged:
            return json_response({
                "message": "Customers created successfully",
                "count": len(result.inserted_ids),
                "inserted_ids": [str(id) for id in result.inserted_ids]
            }), 201
        else:
            return jsonify({"error": "Failed to create customers"}), 500
    
    except Exception as e:
        logger.error(f"Error creating customers in batch: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Customer Analytics
# -------------------------------------------------------------------------

@app.route('/customers/analytics/summary', methods=['GET'])
def customer_analytics_summary():
    """Get summary analytics about customers"""
    try:
        # Check cache first
        cache_key = "analytics_summary"
        cached_result = cache.get(cache_key)
        if cached_result:
            return json_response(cached_result)
        
        # Get MongoDB collection
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
        
        # Prepare result
        result = {
            "total_customers": total_count,
            "status_distribution": status_results,
            "top_countries": country_results,
            "registration_by_month": date_results,
            "generated_at": get_current_timestamp()
        }
        
        # Cache result
        cache.set(cache_key, result)
        
        return json_response(result)
    
    except Exception as e:
        logger.error(f"Error getting customer analytics summary: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/customers/analytics/purchases', methods=['GET'])
def customer_purchase_analytics():
    """Get analytics about customer purchases"""
    try:
        # Check cache first
        cache_key = "purchase_analytics"
        cached_result = cache.get(cache_key)
        if cached_result:
            return json_response(cached_result)
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Get purchase statistics
        purchase_pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": {"path": "$purchase_history", "preserveNullAndEmptyArrays": False}},
            {"$group": {
                "_id": None,
                "total_purchases": {"$sum": 1},
                "total_revenue": {"$sum": "$purchase_history.total_amount"},
                "avg_purchase": {"$avg": "$purchase_history.total_amount"},
                "min_purchase": {"$min": "$purchase_history.total_amount"},
                "max_purchase": {"$max": "$purchase_history.total_amount"}
            }}
        ]
        purchase_stats = list(collection.aggregate(purchase_pipeline))
        purchase_stats = purchase_stats[0] if purchase_stats else {
            "total_purchases": 0,
            "total_revenue": 0,
            "avg_purchase": 0,
            "min_purchase": 0,
            "max_purchase": 0
        }
        
        # Get payment method distribution
        payment_pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$purchase_history"},
            {"$group": {
                "_id": "$purchase_history.payment_method",
                "count": {"$sum": 1},
                "total_amount": {"$sum": "$purchase_history.total_amount"}
            }},
            {"$sort": {"count": -1}}
        ]
        payment_methods = list(collection.aggregate(payment_pipeline))
        
        # Get purchase status distribution
        status_pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$purchase_history"},
            {"$group": {
                "_id": "$purchase_history.status",
                "count": {"$sum": 1},
                "total_amount": {"$sum": "$purchase_history.total_amount"}
            }},
            {"$sort": {"count": -1}}
        ]
        status_distribution = list(collection.aggregate(status_pipeline))
        
        # Prepare result
        result = {
            "purchase_stats": purchase_stats,
            "payment_methods": payment_methods,
            "status_distribution": status_distribution,
            "generated_at": get_current_timestamp()
        }
        
        # Cache result
        cache.set(cache_key, result)
        
        return json_response(result)
    
    except Exception as e:
        logger.error(f"Error getting purchase analytics: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Stress Testing
# -------------------------------------------------------------------------

@app.route('/stress/large-payload', methods=['GET'])
def get_large_payload():
    """Generate and return a large payload for stress testing"""
    try:
        # Get size parameter (in KB)
        size_kb = request.args.get("size_kb")
        if size_kb:
            size_kb = int(size_kb)
            # Limit maximum size for safety
            size_kb = min(size_kb, CONFIG.get("max_payload_size_kb", 1024))
        else:
            size_kb = None
        
        # Generate large customer payload
        customer = generate_large_customer_payload(size_kb)
        
        return json_response(customer)
    
    except Exception as e:
        logger.error(f"Error generating large payload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stress/large-payload', methods=['POST'])
def process_large_payload():
    """Process a large payload and return it with modifications"""
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Add processing metadata
        data["processed_by"] = CONFIG["instance_id"]
        data["processed_at"] = get_current_timestamp()
        data["processing_time_ms"] = random.randint(10, 500)
        
        # Simulate processing delay
        delay = request.args.get("delay_ms")
        if delay:
            time.sleep(int(delay) / 1000)
        
        return json_response({
            "message": "Payload processed successfully",
            "processed_data": data,
            "size_bytes": len(json.dumps(data))
        })
    
    except Exception as e:
        logger.error(f"Error processing large payload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stress/batch-customers', methods=['POST'])
def batch_create_random_customers():
    """Create a batch of random customers for stress testing"""
    try:
        # Get parameters
        count = min(int(request.args.get("count", "10")), 100)
        size_kb = min(int(request.args.get("size_kb", "10")), CONFIG.get("max_payload_size_kb", 1024))
        
        # Generate customers
        customers = [generate_large_customer_payload(size_kb) for _ in range(count)]
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Insert the customers
        result = collection.insert_many(customers)
        
        if result.acknowledged:
            return json_response({
                "message": "Random customers created successfully",
                "count": len(result.inserted_ids),
                "inserted_ids": [str(id) for id in result.inserted_ids],
                "total_size_kb": len(json.dumps(customers)) / 1024
            }), 201
        else:
            return jsonify({"error": "Failed to create random customers"}), 500
    
    except Exception as e:
        logger.error(f"Error creating random customers in batch: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stress/heavy-computation', methods=['GET'])
def heavy_computation():
    """Perform a heavy computation to stress CPU and MongoDB"""
    try:
        # Get parameters
        depth = int(request.args.get("depth", "3"))
        complexity = request.args.get("complexity", "medium").lower()
        
        # Limit depth for safety
        depth = min(max(depth, 1), 10)
        
        # Set complexity factors
        if complexity == "low":
            factor = 2
        elif complexity == "medium":
            factor = 5
        elif complexity == "high":
            factor = 10
        else:
            factor = 5
        
        # Start timer
        start = time.time()
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Build a complex nested aggregation pipeline
        pipeline = []
        
        # Start with a match stage to filter documents
        pipeline.append({"$match": {"account_status": {"$exists": True}}})
        
        # Add multiple group and project stages based on depth
        for i in range(depth):
            # Add group stage
            group_id = {}
            for j in range(factor):
                field = f"field_{j}"
                if j % 3 == 0:
                    group_id[field] = f"$account_status"
                elif j % 3 == 1:
                    group_id[field] = {"$ifNull": ["$address.country", "unknown"]}
                else:
                    group_id[field] = {"$substr": [{"$ifNull": ["$created_at", "2023-01-01"]}, 0, 7]}
            
            pipeline.append({
                "$group": {
                    "_id": group_id,
                    "count": {"$sum": 1},
                    "avg_field": {"$avg": {"$multiply": [{"$size": {"$ifNull": ["$tags", []]}}, factor]}}
                }
            })
            
            # Add project stage
            project = {
                "count": 1,
                "avg_field": 1,
                "computed_field": {"$multiply": ["$count", factor]}
            }
            
            # Add more computed fields based on complexity
            for j in range(factor):
                project[f"complex_field_{j}"] = {
                    "$cond": {
                        "if": {"$gt": ["$count", j]},
                        "then": {"$multiply": ["$count", j + 1]},
                        "else": 0
                    }
                }
            
            pipeline.append({"$project": project})
            
            # Add sort stage
            pipeline.append({"$sort": {"count": -1}})
            
            # Add limit to avoid excessive memory usage
            pipeline.append({"$limit": 1000})
        
        # Execute the pipeline
        results = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Calculate execution time
        execution_time = time.time() - start
        
        return jsonify({
            "results_count": len(results),
            "execution_time_seconds": execution_time,
            "depth": depth,
            "complexity": complexity,
            "pipeline_stages": len(pipeline),
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error performing heavy computation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stress/connection-storm', methods=['GET'])
def connection_storm():
    """Simulate a connection storm by creating multiple MongoDB operations"""
    try:
        # Get parameters
        operations = int(request.args.get("operations", "10"))
        concurrency = int(request.args.get("concurrency", "1"))
        
        # Limit parameters for safety
        operations = min(max(operations, 1), 100)
        concurrency = min(max(concurrency, 1), 10)
        
        # Start timer
        start = time.time()
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Define different operations to perform
        operation_types = [
            "count",
            "distinct",
            "find",
            "aggregate"
        ]
        
        # Execute operations
        results = []
        
        def execute_operation(op_type):
            """Execute a specific operation type"""
            if op_type == "count":
                # Perform count with different filters
                filters = [
                    {},
                    {"account_status": "active"},
                    {"address.country": {"$exists": True}},
                    {"tags": {"$exists": True, "$ne": []}}
                ]
                
                for filter_doc in filters:
                    count = collection.count_documents(filter_doc)
                    results.append({
                        "operation": "count",
                        "filter": filter_doc,
                        "result": count
                    })
            
            elif op_type == "distinct":
                # Get distinct values for different fields
                fields = [
                    "account_status",
                    "address.country",
                    "address.state"
                ]
                
                for field in fields:
                    distinct_values = collection.distinct(field)
                    results.append({
                        "operation": "distinct",
                        "field": field,
                        "count": len(distinct_values)
                    })
            
            elif op_type == "find":
                # Perform find operations with different parameters
                queries = [
                    ({}, {"customer_id": 1, "first_name": 1, "last_name": 1}, 100),
                    ({"account_status": "active"}, {"email": 1, "phone": 1}, 50),
                    ({"address.country": "USA"}, {"address": 1}, 50)
                ]
                
                for query, projection, limit in queries:
                    docs = list(collection.find(query, projection).limit(limit))
                    results.append({
                        "operation": "find",
                        "query": query,
                        "projection": projection,
                        "limit": limit,
                        "found": len(docs)
                    })
            
            elif op_type == "aggregate":
                # Perform aggregation operations
                pipelines = [
                    [
                        {"$group": {"_id": "$account_status", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    [
                        {"$match": {"address.country": {"$exists": True}}},
                        {"$group": {"_id": "$address.country", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10}
                    ],
                    [
                        {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
                        {"$unwind": "$purchase_history"},
                        {"$group": {
                            "_id": "$purchase_history.payment_method",
                            "count": {"$sum": 1},
                            "total": {"$sum": "$purchase_history.total_amount"}
                        }},
                        {"$sort": {"count": -1}}
                    ]
                ]
                
                for pipeline in pipelines:
                    agg_results = list(collection.aggregate(pipeline))
                    results.append({
                        "operation": "aggregate",
                        "pipeline": pipeline,
                        "results_count": len(agg_results)
                    })
        
        # Execute operations
        threads = []
        for _ in range(operations):
            # Choose a random operation type
            op_type = random.choice(operation_types)
            
            if concurrency > 1:
                # Execute in a separate thread for concurrency
                thread = threading.Thread(target=execute_operation, args=(op_type,))
                threads.append(thread)
                thread.start()
            else:
                # Execute sequentially
                execute_operation(op_type)
        
        # Wait for all threads to complete if using concurrency
        for thread in threads:
            thread.join()
        
        # Calculate execution time
        execution_time = time.time() - start
        
        return jsonify({
            "operations_requested": operations,
            "operations_completed": len(results),
            "concurrency": concurrency,
            "execution_time_seconds": execution_time,
            "operations_per_second": len(results) / execution_time if execution_time > 0 else 0,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error during connection storm: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Periodic Tasks
# -------------------------------------------------------------------------

def start_periodic_tasks():
    """Start periodic tasks in background threads"""
    
    def run_cache_cleanup():
        """Clean up expired cache entries periodically"""
        while True:
            try:
                cache.cleanup()
                time.sleep(300)  # Run every 5 minutes
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")
                time.sleep(60)  # Retry after 1 minute on error
    
    # Start cache cleanup thread
    cache_thread = threading.Thread(target=run_cache_cleanup)
    cache_thread.daemon = True
    cache_thread.start()

# -------------------------------------------------------------------------
# Application Shutdown
# -------------------------------------------------------------------------

@atexit.register
def shutdown():
    """Close MongoDB connection when the application shuts down"""
    try:
        mongo_manager.close()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# -------------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------------

if __name__ == "__main__":
    port = CONFIG.get("api_port", 5000)
    logger.info(f"Starting Customer API on port {port}")
    logger.info(f"Instance ID: {CONFIG['instance_id']}")
    logger.info(f"Connected to MongoDB database: {CONFIG['database_name']}, collection: {CONFIG['collection_name']}")
    
    # Start periodic tasks
    start_periodic_tasks()
    
    # Start Flask application
    app.run(host="0.0.0.0", port=port, threaded=True, debug=CONFIG.get("debug_mode", False))
