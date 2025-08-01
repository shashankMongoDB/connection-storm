#!/usr/bin/env python3
"""
Analytics API Microservice - 10GB Data Edition

A standalone Flask application that provides analytics operations for customer data
stored in MongoDB. Specifically designed to work with the 10GB customer dataset.

Features:
- Complex aggregation pipelines
- Statistical analysis
- Data segmentation
- Heavy computation endpoints
- Connection pooling for MongoDB
- Caching for expensive operations
- Error handling and logging
- Health check endpoints
- Self-contained configuration management
"""

import os
import sys
import time
import json
import uuid
import logging
import threading
import random
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from functools import wraps

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
logger = logging.getLogger("analytics-api-10gb")

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
        "analytics_api_port": int(os.getenv("ANALYTICS_API_PORT", "5000")),
        "write_concern": os.getenv("MONGO_WRITE_CONCERN", "majority"),
        "max_pool_size": int(os.getenv("MONGO_MAX_POOL_SIZE", "100")),
        "min_pool_size": int(os.getenv("MONGO_MIN_POOL_SIZE", "10")),
        "max_idle_time_ms": int(os.getenv("MONGO_MAX_IDLE_TIME_MS", "30000")),
        "cache_ttl_seconds": int(os.getenv("CACHE_TTL_SECONDS", "300")),
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
# Flask Application
# -------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Track application start time for uptime metric
start_time = time.time()

# Initialize MongoDB connection
mongo_manager = MongoDBManager()

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

def cached(ttl_seconds=None):
    """Decorator to cache function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key based on function name and arguments
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Check cache first
            cached_result = cache.get(key)
            if cached_result is not None:
                return cached_result
            
            # Call function if not cached
            result = func(*args, **kwargs)
            
            # Cache result
            cache.set(key, result)
            
            return result
        return wrapper
    return decorator

def json_response(data):
    """Convert MongoDB BSON to JSON response"""
    return Response(
        dumps(data, default=str),
        mimetype='application/json'
    )

# -------------------------------------------------------------------------
# Routes - Health Check
# -------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo_manager.client.admin.command('ping')
        return jsonify({
            "status": "ok",
            "timestamp": get_current_timestamp(),
            "service": "analytics-api-10gb",
            "version": "1.0.0",
            "database_connected": True,
            "database_name": CONFIG["database_name"],
            "collection_name": CONFIG["collection_name"],
            "uptime_seconds": time.time() - start_time
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "error",
            "message": "Database connection failed",
            "timestamp": get_current_timestamp(),
            "database_connected": False,
            "uptime_seconds": time.time() - start_time
        }), 503

# -------------------------------------------------------------------------
# Routes - Basic Analytics
# -------------------------------------------------------------------------

@app.route('/analytics/statistics', methods=['GET'])
@cached(ttl_seconds=300)
def get_statistics():
    """Get basic statistics about customer data"""
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
        
        return json_response({
            "total_customers": total_count,
            "status_distribution": status_results,
            "top_countries": country_results,
            "registration_by_month": date_results,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/customer-segments', methods=['GET'])
@cached(ttl_seconds=300)
def customer_segments():
    """Segment customers based on various criteria"""
    try:
        collection = mongo_manager.get_collection()
        
        # Define segments based on customer data
        segments = []
        
        # Segment by account status
        status_pipeline = [
            {"$group": {"_id": "$account_status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        status_segments = list(collection.aggregate(status_pipeline))
        segments.append({
            "segment_type": "account_status",
            "segments": status_segments
        })
        
        # Segment by country
        country_pipeline = [
            {"$match": {"address.country": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        country_segments = list(collection.aggregate(country_pipeline))
        segments.append({
            "segment_type": "country",
            "segments": country_segments
        })
        
        # Segment by registration year
        year_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "year": {"$substr": ["$created_at", 0, 4]}  # Get YYYY part
            }},
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        year_segments = list(collection.aggregate(year_pipeline))
        segments.append({
            "segment_type": "registration_year",
            "segments": year_segments
        })
        
        # Segment by tags (if available)
        tags_pipeline = [
            {"$match": {"tags": {"$exists": True, "$ne": []}}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        tags_segments = list(collection.aggregate(tags_pipeline))
        segments.append({
            "segment_type": "tags",
            "segments": tags_segments
        })
        
        return json_response({
            "segments": segments,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error getting customer segments: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/geographic', methods=['GET'])
@cached(ttl_seconds=300)
def geographic_analysis():
    """Analyze customer distribution by geography"""
    try:
        collection = mongo_manager.get_collection()
        
        # Country distribution
        country_pipeline = [
            {"$match": {"address.country": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        countries = list(collection.aggregate(country_pipeline))
        
        # State/Province distribution
        state_pipeline = [
            {"$match": {"address.state": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.state", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 50}
        ]
        states = list(collection.aggregate(state_pipeline))
        
        # City distribution
        city_pipeline = [
            {"$match": {"address.city": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.city", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 50}
        ]
        cities = list(collection.aggregate(city_pipeline))
        
        # Country-state distribution
        country_state_pipeline = [
            {"$match": {
                "address.country": {"$exists": True, "$ne": None},
                "address.state": {"$exists": True, "$ne": None}
            }},
            {"$group": {
                "_id": {"country": "$address.country", "state": "$address.state"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 100}
        ]
        country_state = list(collection.aggregate(country_state_pipeline))
        
        return json_response({
            "countries": countries,
            "states": states,
            "cities": cities,
            "country_state_distribution": country_state,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error getting geographic analysis: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/time-series', methods=['GET'])
@cached(ttl_seconds=300)
def time_series_analysis():
    """Analyze customer registrations over time"""
    try:
        collection = mongo_manager.get_collection()
        
        # Daily registrations
        daily_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "date": {"$substr": ["$created_at", 0, 10]}  # Get YYYY-MM-DD part
            }},
            {"$group": {"_id": "$date", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        daily = list(collection.aggregate(daily_pipeline))
        
        # Monthly registrations
        monthly_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "month": {"$substr": ["$created_at", 0, 7]}  # Get YYYY-MM part
            }},
            {"$group": {"_id": "$month", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        monthly = list(collection.aggregate(monthly_pipeline))
        
        # Yearly registrations
        yearly_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "year": {"$substr": ["$created_at", 0, 4]}  # Get YYYY part
            }},
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        yearly = list(collection.aggregate(yearly_pipeline))
        
        # Day of week distribution
        dow_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "dayOfWeek": {"$dayOfWeek": {"$dateFromString": {"dateString": "$created_at"}}}
            }},
            {"$group": {"_id": "$dayOfWeek", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        day_of_week = list(collection.aggregate(dow_pipeline))
        
        # Map day numbers to names
        day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        for entry in day_of_week:
            # MongoDB dayOfWeek is 1 (Sunday) to 7 (Saturday)
            entry["day_name"] = day_names[entry["_id"] - 1]
        
        return json_response({
            "daily_registrations": daily,
            "monthly_registrations": monthly,
            "yearly_registrations": yearly,
            "day_of_week_distribution": day_of_week,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error getting time series analysis: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Complex Analytics
# -------------------------------------------------------------------------

@app.route('/analytics/aggregate', methods=['POST'])
def custom_aggregation():
    """Run a custom aggregation pipeline"""
    try:
        # Get pipeline from request
        data = request.get_json()
        if not data or "pipeline" not in data:
            return jsonify({"error": "Missing pipeline in request"}), 400
        
        pipeline = data["pipeline"]
        
        # Validate pipeline (basic check)
        if not isinstance(pipeline, list):
            return jsonify({"error": "Pipeline must be an array"}), 400
        
        # Execute pipeline
        collection = mongo_manager.get_collection()
        results = list(collection.aggregate(pipeline))
        
        return json_response({
            "results": results,
            "count": len(results),
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error running custom aggregation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/customer-lifetime-value', methods=['GET'])
@cached(ttl_seconds=300)
def customer_lifetime_value():
    """Calculate customer lifetime value based on purchase history"""
    try:
        collection = mongo_manager.get_collection()
        
        # Pipeline to calculate lifetime value
        pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$project": {
                "customer_id": 1,
                "first_name": 1,
                "last_name": 1,
                "email": 1,
                "purchases": {"$size": "$purchase_history"},
                "total_spent": {"$sum": "$purchase_history.total_amount"}
            }},
            {"$sort": {"total_spent": -1}},
            {"$limit": 100}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Calculate additional metrics
        if results:
            total_spent = sum(r["total_spent"] for r in results)
            avg_spent = total_spent / len(results)
            
            for customer in results:
                customer["avg_order_value"] = customer["total_spent"] / customer["purchases"] if customer["purchases"] > 0 else 0
        
        return json_response({
            "customers": results,
            "count": len(results),
            "metrics": {
                "avg_lifetime_value": avg_spent if results else 0,
                "total_analyzed": len(results)
            },
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error calculating customer lifetime value: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/purchase-patterns', methods=['GET'])
@cached(ttl_seconds=300)
def purchase_patterns():
    """Analyze purchase patterns and frequencies"""
    try:
        collection = mongo_manager.get_collection()
        
        # Payment method distribution
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
        
        # Purchase status distribution
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
        
        # Purchase amount ranges
        amount_pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": "$purchase_history"},
            {"$project": {
                "amount": "$purchase_history.total_amount",
                "range": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lt": ["$purchase_history.total_amount", 50]}, "then": "< $50"},
                            {"case": {"$lt": ["$purchase_history.total_amount", 100]}, "then": "$50-$99"},
                            {"case": {"$lt": ["$purchase_history.total_amount", 200]}, "then": "$100-$199"},
                            {"case": {"$lt": ["$purchase_history.total_amount", 500]}, "then": "$200-$499"},
                            {"case": {"$lt": ["$purchase_history.total_amount", 1000]}, "then": "$500-$999"}
                        ],
                        "default": "$1000+"
                    }
                }
            }},
            {"$group": {
                "_id": "$range",
                "count": {"$sum": 1},
                "total_amount": {"$sum": "$amount"},
                "avg_amount": {"$avg": "$amount"}
            }},
            {"$sort": {"avg_amount": 1}}
        ]
        amount_ranges = list(collection.aggregate(amount_pipeline))
        
        # Purchase frequency
        frequency_pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$project": {
                "customer_id": 1,
                "purchase_count": {"$size": "$purchase_history"}
            }},
            {"$group": {
                "_id": "$purchase_count",
                "customer_count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        frequency = list(collection.aggregate(frequency_pipeline))
        
        return json_response({
            "payment_methods": payment_methods,
            "status_distribution": status_distribution,
            "amount_ranges": amount_ranges,
            "purchase_frequency": frequency,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error analyzing purchase patterns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/cohort-analysis', methods=['GET'])
@cached(ttl_seconds=600)
def cohort_analysis():
    """Perform cohort analysis based on registration date"""
    try:
        collection = mongo_manager.get_collection()
        
        # Get cohorts by month
        cohort_pipeline = [
            {"$match": {"created_at": {"$exists": True, "$ne": None}}},
            {"$project": {
                "cohort_month": {"$substr": ["$created_at", 0, 7]},  # YYYY-MM
                "customer_id": 1,
                "purchase_history": 1
            }},
            {"$group": {
                "_id": "$cohort_month",
                "customer_count": {"$sum": 1},
                "customers_with_purchases": {
                    "$sum": {
                        "$cond": [
                            {"$and": [
                                {"$isArray": "$purchase_history"},
                                {"$gt": [{"$size": "$purchase_history"}, 0]}
                            ]},
                            1,
                            0
                        ]
                    }
                }
            }},
            {"$sort": {"_id": 1}}
        ]
        
        cohorts = list(collection.aggregate(cohort_pipeline))
        
        # Calculate retention and conversion rates
        for cohort in cohorts:
            cohort["conversion_rate"] = (cohort["customers_with_purchases"] / cohort["customer_count"]) * 100 if cohort["customer_count"] > 0 else 0
        
        # Get retention by month (simplified)
        retention_pipeline = [
            {"$match": {
                "created_at": {"$exists": True, "$ne": None},
                "purchase_history": {"$exists": True, "$ne": []}
            }},
            {"$project": {
                "cohort_month": {"$substr": ["$created_at", 0, 7]},  # YYYY-MM
                "purchase_history": 1
            }},
            {"$unwind": "$purchase_history"},
            {"$project": {
                "cohort_month": 1,
                "purchase_month": {"$substr": ["$purchase_history.date", 0, 7]}  # YYYY-MM
            }},
            {"$group": {
                "_id": {
                    "cohort": "$cohort_month",
                    "purchase_month": "$purchase_month"
                },
                "customer_count": {"$sum": 1}
            }},
            {"$sort": {"_id.cohort": 1, "_id.purchase_month": 1}}
        ]
        
        retention_data = list(collection.aggregate(retention_pipeline))
        
        # Format retention data
        retention_matrix = {}
        for entry in retention_data:
            cohort = entry["_id"]["cohort"]
            purchase_month = entry["_id"]["purchase_month"]
            
            if cohort not in retention_matrix:
                retention_matrix[cohort] = {}
            
            retention_matrix[cohort][purchase_month] = entry["customer_count"]
        
        return json_response({
            "cohorts": cohorts,
            "retention_matrix": retention_matrix,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error performing cohort analysis: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Heavy Computation
# -------------------------------------------------------------------------

@app.route('/analytics/heavy-computation', methods=['GET'])
def heavy_computation():
    """Perform a heavy computation to stress MongoDB and CPU"""
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

@app.route('/analytics/process', methods=['POST'])
def process_data():
    """Process data with complex operations"""
    try:
        # Get parameters from request
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        operation = data.get("operation", "complex_analysis")
        parameters = data.get("parameters", {})
        
        # Set default parameters
        depth = parameters.get("depth", 3)
        width = parameters.get("width", 5)
        iterations = parameters.get("iterations", 10)
        memory_intensive = parameters.get("memory_intensive", False)
        
        # Start timer
        start = time.time()
        
        # Get MongoDB collection
        collection = mongo_manager.get_collection()
        
        # Perform different operations based on request
        results = None
        
        if operation == "complex_analysis":
            # Build a complex aggregation pipeline
            pipeline = [
                {"$match": {"account_status": {"$exists": True}}},
                {"$sample": {"size": 1000}},
                {"$project": {
                    "customer_id": 1,
                    "name": {"$concat": ["$first_name", " ", "$last_name"]},
                    "email": 1,
                    "status": "$account_status",
                    "country": "$address.country",
                    "purchase_count": {"$size": {"$ifNull": ["$purchase_history", []]}},
                    "tags_count": {"$size": {"$ifNull": ["$tags", []]}}
                }},
                {"$group": {
                    "_id": {
                        "status": "$status",
                        "country": "$country"
                    },
                    "count": {"$sum": 1},
                    "avg_purchases": {"$avg": "$purchase_count"},
                    "avg_tags": {"$avg": "$tags_count"},
                    "customers": {"$push": {
                        "id": "$customer_id",
                        "name": "$name",
                        "email": "$email"
                    }}
                }},
                {"$sort": {"count": -1}}
            ]
            
            results = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        elif operation == "memory_analysis":
            # Perform memory-intensive operation if requested
            if memory_intensive:
                # Load a large number of documents into memory
                documents = list(collection.find().limit(10000))
                
                # Process documents in memory
                processed = []
                for doc in documents:
                    # Create a processed version with additional computed fields
                    processed_doc = {
                        "id": str(doc.get("_id")),
                        "customer_id": doc.get("customer_id"),
                        "name": f"{doc.get('first_name', '')} {doc.get('last_name', '')}",
                        "email": doc.get("email"),
                        "status": doc.get("account_status"),
                        "country": doc.get("address", {}).get("country"),
                        "computed_fields": {}
                    }
                    
                    # Add computed fields based on width parameter
                    for i in range(width):
                        processed_doc["computed_fields"][f"field_{i}"] = f"computed_value_{i}_{doc.get('customer_id', '')}"
                    
                    processed.append(processed_doc)
                
                results = {
                    "processed_count": len(processed),
                    "sample": processed[:5] if processed else []
                }
            else:
                # Use aggregation for less memory-intensive operation
                pipeline = [
                    {"$sample": {"size": 1000}},
                    {"$project": {
                        "id": {"$toString": "$_id"},
                        "customer_id": 1,
                        "name": {"$concat": ["$first_name", " ", "$last_name"]},
                        "email": 1,
                        "status": "$account_status",
                        "country": "$address.country"
                    }}
                ]
                
                results = list(collection.aggregate(pipeline))
        
        elif operation == "iterative_analysis":
            # Perform multiple iterations of analysis
            iteration_results = []
            
            for i in range(iterations):
                # Create a different pipeline for each iteration
                pipeline = [
                    {"$match": {"account_status": {"$exists": True}}},
                    {"$sample": {"size": 500}},
                    {"$project": {
                        "customer_id": 1,
                        "name": {"$concat": ["$first_name", " ", "$last_name"]},
                        "email": 1,
                        "status": "$account_status",
                        "country": "$address.country",
                        "iteration": {"$literal": i}
                    }},
                    {"$group": {
                        "_id": {
                            "status": "$status",
                            "iteration": "$iteration"
                        },
                        "count": {"$sum": 1}
                    }}
                ]
                
                iter_result = list(collection.aggregate(pipeline))
                iteration_results.append({
                    "iteration": i,
                    "results": iter_result
                })
            
            results = {
                "iterations": iterations,
                "results": iteration_results
            }
        
        else:
            return jsonify({"error": f"Unknown operation: {operation}"}), 400
        
        # Calculate execution time
        execution_time = time.time() - start
        
        return json_response({
            "operation": operation,
            "parameters": parameters,
            "execution_time_seconds": execution_time,
            "results": results,
            "generated_at": get_current_timestamp()
        })
    
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/analytics/stress-test', methods=['GET'])
def stress_test():
    """Stress test MongoDB with multiple operations"""
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
            "aggregate",
            "mixed"
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
            
            elif op_type == "mixed":
                # Perform a mix of operations
                # 1. Count
                count = collection.count_documents({})
                
                # 2. Find one
                sample = collection.find_one({})
                
                # 3. Aggregate
                pipeline = [
                    {"$sample": {"size": 100}},
                    {"$project": {
                        "customer_id": 1,
                        "name": {"$concat": ["$first_name", " ", "$last_name"]},
                        "email": 1
                    }}
                ]
                agg_results = list(collection.aggregate(pipeline))
                
                results.append({
                    "operation": "mixed",
                    "count": count,
                    "sample_id": str(sample["_id"]) if sample else None,
                    "agg_results_count": len(agg_results)
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
        logger.error(f"Error during stress test: {e}")
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

# NOTE:
# Flask’s `teardown_appcontext` fires after *every* request which was closing
# the shared MongoClient from our singleton and breaking subsequent calls.
# We therefore remove the per-request teardown and close the connection once
# when the Python process actually terminates.

import atexit

@atexit.register
def _close_mongo_on_exit() -> None:
    """Close MongoDB connection exactly once when the app terminates."""
    try:
        mongo_manager.close()
    except Exception:
        # Ignore any errors during shutdown
        pass

# -------------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------------

if __name__ == "__main__":
    port = CONFIG.get("analytics_api_port", 5000)
    logger.info(f"Starting Analytics API on port {port}")
    
    # Start periodic tasks
    start_periodic_tasks()
    
    # Start Flask application
    app.run(host="0.0.0.0", port=port, threaded=True)
