#!/usr/bin/env python3
"""
Reports API Microservice - 10GB Data Edition

A standalone Flask application that provides report generation for customer data
stored in MongoDB. Specifically designed to work with the 10GB customer dataset.

Features:
- CSV and text report generation
- PDF report simulation
- Background task processing
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
import csv
import io
import logging
import uuid
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union

# Flask imports
from flask import Flask, request, jsonify, Response, send_file, make_response
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
logger = logging.getLogger("reports-api-10gb")

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
        "reports_api_port": int(os.getenv("REPORTS_API_PORT", "3000")),
        "write_concern": os.getenv("MONGO_WRITE_CONCERN", "majority"),
        "max_pool_size": int(os.getenv("MONGO_MAX_POOL_SIZE", "100")),
        "min_pool_size": int(os.getenv("MONGO_MIN_POOL_SIZE", "10")),
        "max_idle_time_ms": int(os.getenv("MONGO_MAX_IDLE_TIME_MS", "30000")),
        "cache_ttl_seconds": int(os.getenv("CACHE_TTL_SECONDS", "300")),
        "reports_workers": int(os.getenv("REPORTS_BACKGROUND_WORKERS", "4")),
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

# -------------------------------------------------------------------------
# Background Task Manager
# -------------------------------------------------------------------------

class BackgroundTaskManager:
    def __init__(self, max_workers=4):
        self.tasks = {}
        self.max_workers = max_workers
        self.lock = threading.Lock()
    
    def start_task(self, task_type, params=None):
        """Start a new background task"""
        task_id = str(uuid.uuid4())
        
        with self.lock:
            # Check if we have too many running tasks
            running_tasks = [t for t in self.tasks.values() if t["status"] == "running"]
            if len(running_tasks) >= self.max_workers:
                return None, "Too many tasks running, try again later"
            
            # Create task record
            self.tasks[task_id] = {
                "id": task_id,
                "type": task_type,
                "params": params or {},
                "status": "pending",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "result": None,
                "error": None
            }
        
        # Start task in background thread
        thread = threading.Thread(
            target=self._run_task,
            args=(task_id, task_type, params)
        )
        thread.daemon = True
        thread.start()
        
        return task_id, None
    
    def _run_task(self, task_id, task_type, params):
        """Run the task in a background thread"""
        try:
            # Update status to running
            with self.lock:
                if task_id not in self.tasks:
                    return
                self.tasks[task_id]["status"] = "running"
                self.tasks[task_id]["updated_at"] = datetime.now().isoformat()
            
            # Run the appropriate task based on type
            result = None
            if task_type == "customer_report":
                result = self._generate_customer_report(params)
            elif task_type == "purchase_report":
                result = self._generate_purchase_report(params)
            elif task_type == "analytics_report":
                result = self._generate_analytics_report(params)
            elif task_type == "geographic_report":
                result = self._generate_geographic_report(params)
            elif task_type == "segment_report":
                result = self._generate_segment_report(params)
            else:
                raise ValueError(f"Unknown task type: {task_type}")
            
            # Update task with result
            with self.lock:
                if task_id not in self.tasks:
                    return
                self.tasks[task_id]["status"] = "completed"
                self.tasks[task_id]["result"] = result
                self.tasks[task_id]["updated_at"] = datetime.now().isoformat()
        
        except Exception as e:
            logger.error(f"Error running task {task_id}: {e}")
            # Update task with error
            with self.lock:
                if task_id not in self.tasks:
                    return
                self.tasks[task_id]["status"] = "failed"
                self.tasks[task_id]["error"] = str(e)
                self.tasks[task_id]["updated_at"] = datetime.now().isoformat()
    
    def _generate_customer_report(self, params):
        """Generate a customer report from the 10GB dataset"""
        mongo_manager = MongoDBManager()
        collection = mongo_manager.get_collection()
        
        # Build query based on parameters
        query = {}
        if params.get("status"):
            query["account_status"] = params["status"]
        if params.get("country"):
            query["address.country"] = params["country"]
        
        # Limit results
        limit = min(int(params.get("limit", 1000)), 5000)
        
        # Execute query
        customers = list(collection.find(query).limit(limit))
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header
        writer.writerow([
            "Customer ID", "Name", "Email", "Phone", "Country", "Status", 
            "Registration Date", "Credit Score", "Annual Income", "Loyalty Points"
        ])
        
        # Write data
        for customer in customers:
            writer.writerow([
                customer.get("customer_id", ""),
                f"{customer.get('first_name', '')} {customer.get('last_name', '')}",
                customer.get("email", ""),
                customer.get("phone", ""),
                customer.get("address", {}).get("country", ""),
                customer.get("account_status", ""),
                customer.get("created_at", ""),
                customer.get("credit_score", ""),
                customer.get("annual_income", ""),
                customer.get("loyalty_points", "")
            ])
        
        # Return CSV data
        return {
            "format": "csv",
            "filename": f"customer_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "data": csv_data.getvalue(),
            "count": len(customers)
        }
    
    def _generate_purchase_report(self, params):
        """Generate a purchase report from the 10GB dataset"""
        mongo_manager = MongoDBManager()
        collection = mongo_manager.get_collection()
        
        # Build pipeline
        pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": {"path": "$purchase_history", "preserveNullAndEmptyArrays": False}},
            {"$project": {
                "customer_id": 1,
                "name": {"$concat": ["$first_name", " ", "$last_name"]},
                "email": 1,
                "purchase_id": "$purchase_history.purchase_id",
                "purchase_date": "$purchase_history.date",
                "total_amount": "$purchase_history.total_amount",
                "payment_method": "$purchase_history.payment_method",
                "status": "$purchase_history.status"
            }}
        ]
        
        # Add filters
        if params.get("min_amount"):
            pipeline.append({"$match": {"total_amount": {"$gte": float(params["min_amount"])}}})
        if params.get("max_amount"):
            pipeline.append({"$match": {"total_amount": {"$lte": float(params["max_amount"])}}})
        
        # Sort and limit
        pipeline.append({"$sort": {"purchase_date": -1}})
        limit = min(int(params.get("limit", 1000)), 5000)
        pipeline.append({"$limit": limit})
        
        # Execute pipeline
        purchases = list(collection.aggregate(pipeline))
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header
        writer.writerow([
            "Purchase ID", "Customer ID", "Customer Name", "Email", 
            "Purchase Date", "Total Amount", "Payment Method", "Status"
        ])
        
        # Write data
        for purchase in purchases:
            writer.writerow([
                purchase.get("purchase_id", ""),
                purchase.get("customer_id", ""),
                purchase.get("name", ""),
                purchase.get("email", ""),
                purchase.get("purchase_date", ""),
                purchase.get("total_amount", ""),
                purchase.get("payment_method", ""),
                purchase.get("status", "")
            ])
        
        # Return CSV data
        return {
            "format": "csv",
            "filename": f"purchase_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "data": csv_data.getvalue(),
            "count": len(purchases)
        }
    
    def _generate_analytics_report(self, params):
        """Generate an analytics report from the 10GB dataset"""
        mongo_manager = MongoDBManager()
        collection = mongo_manager.get_collection()
        
        # Calculate various metrics
        total_customers = collection.count_documents({})
        
        # Status distribution
        status_pipeline = [
            {"$group": {"_id": "$account_status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        status_distribution = list(collection.aggregate(status_pipeline))
        
        # Country distribution
        country_pipeline = [
            {"$match": {"address.country": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$address.country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        country_distribution = list(collection.aggregate(country_pipeline))
        
        # Purchase statistics
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
        
        # Generate text report
        report_lines = [
            "CUSTOMER ANALYTICS REPORT",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Database: {CONFIG['database_name']}, Collection: {CONFIG['collection_name']}",
            "",
            f"Total Customers: {total_customers}",
            "",
            "ACCOUNT STATUS DISTRIBUTION",
            "--------------------------"
        ]
        
        for status in status_distribution:
            percentage = (status["count"] / total_customers) * 100 if total_customers > 0 else 0
            report_lines.append(f"{status['_id']}: {status['count']} ({percentage:.2f}%)")
        
        report_lines.extend([
            "",
            "TOP 10 COUNTRIES",
            "--------------"
        ])
        
        for country in country_distribution:
            percentage = (country["count"] / total_customers) * 100 if total_customers > 0 else 0
            report_lines.append(f"{country['_id']}: {country['count']} ({percentage:.2f}%)")
        
        report_lines.extend([
            "",
            "PURCHASE STATISTICS",
            "------------------",
            f"Total Purchases: {purchase_stats['total_purchases']}",
            f"Total Revenue: ${purchase_stats['total_revenue']:.2f}",
            f"Average Purchase: ${purchase_stats['avg_purchase']:.2f}",
            f"Minimum Purchase: ${purchase_stats['min_purchase']:.2f}",
            f"Maximum Purchase: ${purchase_stats['max_purchase']:.2f}",
            "",
            "END OF REPORT"
        ])
        
        # Return text report
        return {
            "format": "text",
            "filename": f"analytics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "data": "\n".join(report_lines),
            "metrics": {
                "total_customers": total_customers,
                "status_distribution": status_distribution,
                "country_distribution": country_distribution,
                "purchase_stats": purchase_stats
            }
        }
    
    def _generate_geographic_report(self, params):
        """Generate a geographic distribution report from the 10GB dataset"""
        mongo_manager = MongoDBManager()
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
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header and country data
        writer.writerow(["Location Type", "Location Name", "Customer Count"])
        
        # Write country data
        for country in countries:
            writer.writerow(["Country", country["_id"], country["count"]])
        
        # Write state data
        for state in states:
            writer.writerow(["State/Province", state["_id"], state["count"]])
        
        # Write city data (top 50)
        for city in cities:
            writer.writerow(["City", city["_id"], city["count"]])
        
        # Return CSV data
        return {
            "format": "csv",
            "filename": f"geographic_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "data": csv_data.getvalue(),
            "metrics": {
                "country_count": len(countries),
                "state_count": len(states),
                "city_count": len(cities)
            }
        }
    
    def _generate_segment_report(self, params):
        """Generate a customer segment report from the 10GB dataset"""
        mongo_manager = MongoDBManager()
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
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header
        writer.writerow(["Segment Type", "Segment Value", "Customer Count"])
        
        # Write segment data
        for segment in segments:
            segment_type = segment["segment_type"]
            for item in segment["segments"]:
                writer.writerow([segment_type, item["_id"], item["count"]])
        
        # Return CSV data
        return {
            "format": "csv",
            "filename": f"segment_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "data": csv_data.getvalue(),
            "segments": segments
        }
    
    def get_task(self, task_id):
        """Get task status and result"""
        with self.lock:
            return self.tasks.get(task_id)
    
    def list_tasks(self, limit=10):
        """List recent tasks"""
        with self.lock:
            # Sort tasks by creation time (newest first)
            sorted_tasks = sorted(
                self.tasks.values(),
                key=lambda t: t["created_at"],
                reverse=True
            )
            return sorted_tasks[:limit]
    
    def cleanup_tasks(self, max_age_hours=24):
        """Remove old completed tasks"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        with self.lock:
            task_ids_to_remove = []
            for task_id, task in self.tasks.items():
                if task["status"] in ["completed", "failed"]:
                    created_at = datetime.fromisoformat(task["created_at"])
                    if created_at < cutoff_time:
                        task_ids_to_remove.append(task_id)
            
            for task_id in task_ids_to_remove:
                del self.tasks[task_id]
            
            return len(task_ids_to_remove)

# Initialize cache and task manager
cache = SimpleCache(ttl_seconds=CONFIG.get("cache_ttl_seconds", 300))
task_manager = BackgroundTaskManager(max_workers=CONFIG.get("reports_workers", 4))

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
            "service": "reports-api-10gb",
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
# Routes - Reports
# -------------------------------------------------------------------------

@app.route('/reports', methods=['GET'])
def list_reports():
    """List available reports"""
    try:
        # Get parameters
        limit = min(int(request.args.get("limit", "10")), 100)
        report_type = request.args.get("type")
        
        # Get recent tasks that generated reports
        tasks = task_manager.list_tasks(limit=100)
        
        # Filter completed report tasks
        report_tasks = [
            task for task in tasks 
            if task["status"] == "completed" 
            and task["type"] in ["customer_report", "purchase_report", "analytics_report", 
                               "geographic_report", "segment_report"]
        ]
        
        # Further filter by type if specified
        if report_type:
            report_tasks = [task for task in report_tasks if task["type"] == report_type]
        
        # Limit results
        report_tasks = report_tasks[:limit]
        
        # Format response
        reports = []
        for task in report_tasks:
            result = task.get("result", {})
            reports.append({
                "id": task["id"],
                "type": task["type"],
                "created_at": task["created_at"],
                "format": result.get("format", "unknown"),
                "filename": result.get("filename", ""),
                "size": len(result.get("data", "")) if result.get("data") else 0,
                "record_count": result.get("count", 0),
                "download_url": f"/reports/tasks/{task['id']}?download=true"
            })
        
        # Also include available report types
        available_report_types = [
            {
                "id": "customer_report",
                "name": "Customer Summary Report",
                "description": "Summary of customer information",
                "format": "CSV",
                "endpoint": "/reports/customer-summary"
            },
            {
                "id": "purchase_report",
                "name": "Purchase History Report",
                "description": "Detailed purchase history",
                "format": "CSV",
                "endpoint": "/reports/purchase-history"
            },
            {
                "id": "analytics_report",
                "name": "Analytics Report",
                "description": "Customer analytics and statistics",
                "format": "Text",
                "endpoint": "/reports/analytics"
            },
            {
                "id": "geographic_report",
                "name": "Geographic Distribution Report",
                "description": "Customer distribution by location",
                "format": "CSV",
                "endpoint": "/reports/geographic"
            },
            {
                "id": "segment_report",
                "name": "Customer Segment Report",
                "description": "Customer segmentation analysis",
                "format": "CSV",
                "endpoint": "/reports/segments"
            }
        ]
        
        return jsonify({
            "reports": reports,
            "count": len(reports),
            "available_report_types": available_report_types
        })
    except Exception as e:
        logger.error(f"Error listing reports: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/generate', methods=['POST'])
def generate_report():
    """Generate a new report"""
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Get report type
        report_type = data.get("report_type")
        if not report_type:
            return jsonify({"error": "Report type is required"}), 400
        
        # Map report type to task type
        task_type_mapping = {
            "customer": "customer_report",
            "purchase": "purchase_report",
            "analytics": "analytics_report",
            "geographic": "geographic_report",
            "segment": "segment_report"
        }
        
        task_type = task_type_mapping.get(report_type)
        if not task_type:
            return jsonify({"error": f"Unknown report type: {report_type}"}), 400
        
        # Extract parameters from request
        params = data.get("parameters", {})
        
        # Start background task
        task_id, error = task_manager.start_task(task_type, params)
        if error:
            return jsonify({"error": error}), 503
        
        # Return task ID
        return jsonify({
            "task_id": task_id,
            "status": "pending",
            "message": "Report generation started in background",
            "check_status_url": f"/reports/tasks/{task_id}"
        })
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/generate-pdf', methods=['POST'])
def generate_pdf_report():
    """Generate a PDF report (simulated for JMeter testing)"""
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Get report parameters
        report_id = data.get("report_id", f"REP{int(time.time())}")
        title = data.get("title", "Performance Report")
        description = data.get("description", "System performance metrics")
        report_type = data.get("type", "performance")
        
        # For simplicity, we'll generate a text report instead of a PDF
        # In a real implementation, you would use a PDF library like ReportLab
        
        # Generate a simple report
        report_lines = [
            f"REPORT ID: {report_id}",
            f"TYPE: {report_type}",
            f"TITLE: {title}",
            f"DESCRIPTION: {description}",
            f"GENERATED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This is a simulated PDF report for JMeter testing.",
            "In a real implementation, this would be a PDF document.",
            "",
            "REPORT CONTENT",
            "--------------",
            "This report contains performance metrics and analysis.",
            f"The report was requested at {datetime.now().isoformat()}",
            f"Report parameters: {json.dumps(data.get('parameters', {}), indent=2)}",
            "",
            "END OF REPORT"
        ]
        
        # Return text report (simulating PDF)
        report_content = "\n".join(report_lines)
        response = make_response(report_content)
        response.headers["Content-Disposition"] = f"attachment; filename={report_id}.txt"
        response.headers["Content-type"] = "text/plain"
        
        return response
    except Exception as e:
        logger.error(f"Error generating PDF report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/customer-summary', methods=['GET'])
def customer_summary_report():
    """Generate a customer summary report"""
    try:
        # Get parameters
        params = {
            "status": request.args.get("status"),
            "country": request.args.get("country"),
            "limit": request.args.get("limit", "1000")
        }
        
        # Check if async mode requested
        async_mode = request.args.get("async", "false").lower() == "true"
        
        if async_mode:
            # Start background task
            task_id, error = task_manager.start_task("customer_report", params)
            if error:
                return jsonify({"error": error}), 503
            
            return jsonify({
                "task_id": task_id,
                "status": "pending",
                "message": "Report generation started in background"
            })
        else:
            # Generate report synchronously
            report = task_manager._generate_customer_report(params)
            
            # Return CSV file
            response = make_response(report["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={report['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
    
    except Exception as e:
        logger.error(f"Error generating customer summary report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/purchase-history', methods=['GET'])
def purchase_history_report():
    """Generate a purchase history report"""
    try:
        # Get parameters
        params = {
            "min_amount": request.args.get("min_amount"),
            "max_amount": request.args.get("max_amount"),
            "limit": request.args.get("limit", "1000")
        }
        
        # Check if async mode requested
        async_mode = request.args.get("async", "false").lower() == "true"
        
        if async_mode:
            # Start background task
            task_id, error = task_manager.start_task("purchase_report", params)
            if error:
                return jsonify({"error": error}), 503
            
            return jsonify({
                "task_id": task_id,
                "status": "pending",
                "message": "Report generation started in background"
            })
        else:
            # Generate report synchronously
            report = task_manager._generate_purchase_report(params)
            
            # Return CSV file
            response = make_response(report["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={report['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
    
    except Exception as e:
        logger.error(f"Error generating purchase history report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/analytics', methods=['GET'])
def analytics_report():
    """Generate an analytics report"""
    try:
        # Get parameters
        params = {}
        
        # Check if async mode requested
        async_mode = request.args.get("async", "false").lower() == "true"
        
        if async_mode:
            # Start background task
            task_id, error = task_manager.start_task("analytics_report", params)
            if error:
                return jsonify({"error": error}), 503
            
            return jsonify({
                "task_id": task_id,
                "status": "pending",
                "message": "Report generation started in background"
            })
        else:
            # Generate report synchronously
            report = task_manager._generate_analytics_report(params)
            
            # Return text file
            response = make_response(report["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={report['filename']}"
            response.headers["Content-type"] = "text/plain"
            return response
    
    except Exception as e:
        logger.error(f"Error generating analytics report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/geographic', methods=['GET'])
def geographic_report():
    """Generate a geographic distribution report"""
    try:
        # Get parameters
        params = {}
        
        # Check if async mode requested
        async_mode = request.args.get("async", "false").lower() == "true"
        
        if async_mode:
            # Start background task
            task_id, error = task_manager.start_task("geographic_report", params)
            if error:
                return jsonify({"error": error}), 503
            
            return jsonify({
                "task_id": task_id,
                "status": "pending",
                "message": "Report generation started in background"
            })
        else:
            # Generate report synchronously
            report = task_manager._generate_geographic_report(params)
            
            # Return CSV file
            response = make_response(report["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={report['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
    
    except Exception as e:
        logger.error(f"Error generating geographic report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/segments', methods=['GET'])
def segment_report():
    """Generate a customer segment report"""
    try:
        # Get parameters
        params = {}
        
        # Check if async mode requested
        async_mode = request.args.get("async", "false").lower() == "true"
        
        if async_mode:
            # Start background task
            task_id, error = task_manager.start_task("segment_report", params)
            if error:
                return jsonify({"error": error}), 503
            
            return jsonify({
                "task_id": task_id,
                "status": "pending",
                "message": "Report generation started in background"
            })
        else:
            # Generate report synchronously
            report = task_manager._generate_segment_report(params)
            
            # Return CSV file
            response = make_response(report["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={report['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
    
    except Exception as e:
        logger.error(f"Error generating segment report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Get status of a background task"""
    try:
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404
        
        # If task is completed and has result, check if download requested
        if task["status"] == "completed" and task["result"] and request.args.get("download", "false").lower() == "true":
            result = task["result"]
            
            # Return file
            response = make_response(result["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={result['filename']}"
            
            if result["format"] == "csv":
                response.headers["Content-type"] = "text/csv"
            else:
                response.headers["Content-type"] = "text/plain"
            
            return response
        
        # Otherwise return task status
        return jsonify(task)
    
    except Exception as e:
        logger.error(f"Error getting task status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/tasks', methods=['GET'])
def list_tasks():
    """List recent tasks"""
    try:
        limit = min(int(request.args.get("limit", "10")), 100)
        tasks = task_manager.list_tasks(limit=limit)
        
        # Don't include the actual result data in the list to keep response size small
        for task in tasks:
            if "result" in task and task["result"] and "data" in task["result"]:
                data_size = len(task["result"]["data"])
                task["result"]["data"] = f"[{data_size} bytes]"
        
        return jsonify({
            "tasks": tasks,
            "count": len(tasks)
        })
    
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------------------------------
# Routes - Simple Data Exports
# -------------------------------------------------------------------------

@app.route('/reports/export/customers', methods=['GET'])
def export_customers():
    """Export customers data as CSV"""
    try:
        # Get parameters
        limit = min(int(request.args.get("limit", "1000")), 5000)
        
        # Check cache first
        cache_key = generate_cache_key("export_customers", {"limit": limit})
        cached_result = cache.get(cache_key)
        if cached_result:
            # Return cached CSV file
            response = make_response(cached_result["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={cached_result['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
        
        # Query MongoDB
        collection = mongo_manager.get_collection()
        customers = list(collection.find({}).limit(limit))
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header
        writer.writerow([
            "Customer ID", "Name", "Email", "Phone", "Street", "City", "State", "Zip", "Country", 
            "Status", "Registration Date", "Credit Score", "Annual Income", "Loyalty Points"
        ])
        
        # Write data
        for customer in customers:
            address = customer.get("address", {})
            writer.writerow([
                customer.get("customer_id", ""),
                f"{customer.get('first_name', '')} {customer.get('last_name', '')}",
                customer.get("email", ""),
                customer.get("phone", ""),
                address.get("street", ""),
                address.get("city", ""),
                address.get("state", ""),
                address.get("postal_code", ""),
                address.get("country", ""),
                customer.get("account_status", ""),
                customer.get("created_at", ""),
                customer.get("credit_score", ""),
                customer.get("annual_income", ""),
                customer.get("loyalty_points", "")
            ])
        
        # Get CSV data
        csv_content = csv_data.getvalue()
        filename = f"customers_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Cache result
        cache.set(cache_key, {
            "data": csv_content,
            "filename": filename,
            "count": len(customers)
        })
        
        # Return CSV file
        response = make_response(csv_content)
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        response.headers["Content-type"] = "text/csv"
        return response
    
    except Exception as e:
        logger.error(f"Error exporting customers: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/reports/export/purchases', methods=['GET'])
def export_purchases():
    """Export purchases data as CSV"""
    try:
        # Get parameters
        limit = min(int(request.args.get("limit", "1000")), 5000)
        
        # Check cache first
        cache_key = generate_cache_key("export_purchases", {"limit": limit})
        cached_result = cache.get(cache_key)
        if cached_result:
            # Return cached CSV file
            response = make_response(cached_result["data"])
            response.headers["Content-Disposition"] = f"attachment; filename={cached_result['filename']}"
            response.headers["Content-type"] = "text/csv"
            return response
        
        # Query MongoDB
        collection = mongo_manager.get_collection()
        pipeline = [
            {"$match": {"purchase_history": {"$exists": True, "$ne": []}}},
            {"$unwind": {"path": "$purchase_history", "preserveNullAndEmptyArrays": False}},
            {"$project": {
                "customer_id": 1,
                "name": {"$concat": ["$first_name", " ", "$last_name"]},
                "email": 1,
                "purchase_id": "$purchase_history.purchase_id",
                "purchase_date": "$purchase_history.date",
                "total_amount": "$purchase_history.total_amount",
                "payment_method": "$purchase_history.payment_method",
                "status": "$purchase_history.status",
                "tracking_number": "$purchase_history.tracking_number"
            }},
            {"$sort": {"purchase_date": -1}},
            {"$limit": limit}
        ]
        
        purchases = list(collection.aggregate(pipeline))
        
        # Generate CSV data
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        
        # Write header
        writer.writerow([
            "Purchase ID", "Customer ID", "Customer Name", "Email", 
            "Purchase Date", "Total Amount", "Payment Method", "Status", "Tracking Number"
        ])
        
        # Write data
        for purchase in purchases:
            writer.writerow([
                purchase.get("purchase_id", ""),
                purchase.get("customer_id", ""),
                purchase.get("name", ""),
                purchase.get("email", ""),
                purchase.get("purchase_date", ""),
                purchase.get("total_amount", ""),
                purchase.get("payment_method", ""),
                purchase.get("status", ""),
                purchase.get("tracking_number", "")
            ])
        
        # Get CSV data
        csv_content = csv_data.getvalue()
        filename = f"purchases_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Cache result
        cache.set(cache_key, {
            "data": csv_content,
            "filename": filename,
            "count": len(purchases)
        })
        
        # Return CSV file
        response = make_response(csv_content)
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        response.headers["Content-type"] = "text/csv"
        return response
    
    except Exception as e:
        logger.error(f"Error exporting purchases: {e}")
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
    
    def run_task_cleanup():
        """Clean up old task records periodically"""
        while True:
            try:
                removed = task_manager.cleanup_tasks(max_age_hours=24)
                if removed > 0:
                    logger.info(f"Removed {removed} old task records")
                time.sleep(3600)  # Run every hour
            except Exception as e:
                logger.error(f"Error in task cleanup: {e}")
                time.sleep(300)  # Retry after 5 minutes on error
    
    # Start cleanup threads
    cache_thread = threading.Thread(target=run_cache_cleanup)
    cache_thread.daemon = True
    cache_thread.start()
    
    task_thread = threading.Thread(target=run_task_cleanup)
    task_thread.daemon = True
    task_thread.start()

# -------------------------------------------------------------------------
# Application Shutdown
# -------------------------------------------------------------------------

@app.teardown_appcontext
def shutdown_session(exception=None):
    """Close MongoDB connection when the application shuts down"""
    try:
        mongo_manager.close()
    except:
        pass

# -------------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------------

if __name__ == "__main__":
    port = CONFIG.get("reports_api_port", 3000)
    logger.info(f"Starting Reports API on port {port}")
    
    # Start periodic tasks
    start_periodic_tasks()
    
    # Start Flask application
    app.run(host="0.0.0.0", port=port, threaded=True)
