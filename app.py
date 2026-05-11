# ==================== FLASK APP lCONFIGURATION ====================

from flask import Flask, request, jsonify
from flask_cors import CORS
from databricks import sql
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Any

print("⚙️ Configuring Flask App...\n")

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration

# Configuration
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Database configuration
DB_CONFIG = {
    'server_hostname': spark.conf.get("spark.databricks.workspaceUrl"),
    'http_path': '/sql/1.0/warehouses/7af74a4c359f588a',  # ⬅️ EDIT THIS
    'catalog': 'workspace',
    'schema': 'bronze',
    'table': 'closing_transaction'
}

# API Configuration
API_PREFIX = '/api'
API_VERSION = 'v1'
BASE_URL = f"{API_PREFIX}/{API_VERSION}"

print("✅ Flask app configured!")
print(f"\n📋 Configuration:")
print(f"   Database: {DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}")
print(f"   API Base: {BASE_URL}")
print(f"   CORS: Enabled")

# ==================== DATABASE CONNECTION HELPER ====================

from contextlib import contextmanager
from databricks.sdk import WorkspaceClient

print("🔌 Setting up database connection helper...\n")

@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    Automatically handles connection opening and closing.
    """
    connection = None
    try:
        # Get OAuth token from workspace
        w = WorkspaceClient()
        
        # Create connection using workspace authentication
        connection = sql.connect(
            server_hostname=DB_CONFIG['server_hostname'],
            http_path=DB_CONFIG['http_path'],
            access_token=w.config.token or dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        )
        
        yield connection
    
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        raise
    
    finally:
        if connection:
            connection.close()

def execute_query(query: str, params: Optional[Dict] = None) -> List[Dict]:
    """
    Execute SQL query and return results as list of dictionaries.
    
    Args:
        query: SQL query string
        params: Optional parameters for parameterized queries
    
    Returns:
        List of dictionaries representing rows
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows and convert to dictionaries
        rows = cursor.fetchall()
        results = [
            {columns[i]: value for i, value in enumerate(row)}
            for row in rows
        ]
        
        cursor.close()
        return results

def execute_statement(query: str, params: Optional[Dict] = None) -> int:
    """
    Execute SQL statement (INSERT, UPDATE, DELETE) and return affected rows.
    
    Args:
        query: SQL statement string
        params: Optional parameters
    
    Returns:
        Number of affected rows
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        affected_rows = cursor.rowcount
        cursor.close()
        
        return affected_rows

print("✅ Database helper functions created!")
print("\n📋 Available functions:")
print("   - get_db_connection(): Context manager for connections")
print("   - execute_query(): Execute SELECT queries")
print("   - execute_statement(): Execute INSERT/UPDATE/DELETE")

# ==================== GET ENDPOINT - LIST ALL ====================

print("🔵 Creating GET endpoint for listing transactions...\n")

@app.route(f'{BASE_URL}/transactions', methods=['GET'])
def get_transactions():
    """
    GET /api/v1/transactions
    
    Query Parameters:
        - limit: Number of records to return (default: 100, max: 1000)
        - offset: Number of records to skip (default: 0)
        - sort_by: Column to sort by (default: id)
        - order: Sort order (asc/desc, default: desc)
        - truck: Filter by truck
        - driver: Filter by driver
    
    Returns:
        JSON array of transactions
    """
    try:
        # Get query parameters
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        sort_by = request.args.get('sort_by', 'id')
        order = request.args.get('order', 'desc').upper()
        
        # Filters
        truck_filter = request.args.get('truck')
        driver_filter = request.args.get('driver')
        
        # Build query
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        
        # WHERE clause
        where_clauses = []
        if truck_filter:
            where_clauses.append(f"truck = '{truck_filter}'")
        if driver_filter:
            where_clauses.append(f"driver = '{driver_filter}'")
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Count total records
        count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_sql}"
        count_result = execute_query(count_query)
        total_records = count_result[0]['total'] if count_result else 0
        
        # Main query
        query = f"""
            SELECT *
            FROM {table_name}
            {where_sql}
            ORDER BY {sort_by} {order}
            LIMIT {limit}
            OFFSET {offset}
        """
        
        results = execute_query(query)
        
        # Response
        response = {
            'success': True,
            'data': results,
            'metadata': {
                'total': total_records,
                'limit': limit,
                'offset': offset,
                'count': len(results),
                'has_more': (offset + len(results)) < total_records
            }
        }
        
        return jsonify(response), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to fetch transactions'
        }), 500

print("✅ GET /api/v1/transactions endpoint created!")
print("\n📋 Supported query parameters:")
print("   - limit, offset (pagination)")
print("   - sort_by, order (sorting)")
print("   - truck, driver (filtering)")

# ==================== GET ENDPOINT - SINGLE RECORD ====================

print("🔵 Creating GET endpoint for single transaction...\n")

@app.route(f'{BASE_URL}/transactions/<int:transaction_id>', methods=['GET'])
def get_transaction(transaction_id: int):
    """
    GET /api/v1/transactions/<id>
    
    Path Parameters:
        - transaction_id: ID of the transaction
    
    Returns:
        JSON object of single transaction
    """
    try:
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE id = {transaction_id}
        """
        
        results = execute_query(query)
        
        if not results:
            return jsonify({
                'success': False,
                'error': 'NOT_FOUND',
                'message': f'Transaction with ID {transaction_id} not found'
            }), 404
        
        response = {
            'success': True,
            'data': results[0]
        }
        
        return jsonify(response), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': f'Failed to fetch transaction {transaction_id}'
        }), 500

print("✅ GET /api/v1/transactions/<id> endpoint created!")
print("\n📋 Returns:")
print("   - 200: Transaction found")
print("   - 404: Transaction not found")
print("   - 500: Server error")

# ==================== POST ENDPOINT - CREATE ====================

print("🟢 Creating POST endpoint for creating transactions...\n")

@app.route(f'{BASE_URL}/transactions', methods=['POST'])
def create_transaction():
    """
    POST /api/v1/transactions
    
    Request Body (JSON):
        {
            "truck": "T-123",
            "driver": "John Doe",
            "gross": 2500.50,
            "net": 2300.00
        }
    
    Returns:
        JSON object of created transaction with ID
    """
    try:
        # Get request data
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'INVALID_REQUEST',
                'message': 'Request body is required'
            }), 400
        
        # Validate required fields
        required_fields = ['truck', 'driver', 'gross', 'net']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            return jsonify({
                'success': False,
                'error': 'MISSING_FIELDS',
                'message': f'Missing required fields: {missing_fields}'
            }), 400
        
        # Build INSERT query
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        
        # Get current timestamp
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        query = f"""
            INSERT INTO {table_name} (truck, driver, gross, net, created_at)
            VALUES ('{data['truck']}', '{data['driver']}', {data['gross']}, {data['net']}, '{created_at}')
        """
        
        # Execute insert
        affected_rows = execute_statement(query)
        
        if affected_rows == 0:
            return jsonify({
                'success': False,
                'error': 'INSERT_FAILED',
                'message': 'Failed to create transaction'
            }), 500
        
        # Get the created record (last inserted)
        get_query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY id DESC
            LIMIT 1
        """
        
        created_record = execute_query(get_query)
        
        response = {
            'success': True,
            'message': 'Transaction created successfully',
            'data': created_record[0] if created_record else None
        }
        
        return jsonify(response), 201
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to create transaction'
        }), 500

print("✅ POST /api/v1/transactions endpoint created!")
print("\n📋 Required fields:")
print("   - truck (string)")
print("   - driver (string)")
print("   - gross (number)")
print("   - net (number)")

# ==================== PUT ENDPOINT - UPDATE ====================

print("🟡 Creating PUT endpoint for updating transactions...\n")

@app.route(f'{BASE_URL}/transactions/<int:transaction_id>', methods=['PUT'])
def update_transaction(transaction_id: int):
    """
    PUT /api/v1/transactions/<id>
    
    Path Parameters:
        - transaction_id: ID of the transaction to update
    
    Request Body (JSON):
        {
            "truck": "T-456",
            "driver": "Jane Smith",
            "gross": 2800.00,
            "net": 2600.00
        }
    
    Returns:
        JSON object of updated transaction
    """
    try:
        # Get request data
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'INVALID_REQUEST',
                'message': 'Request body is required'
            }), 400
        
        # Check if transaction exists
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        
        check_query = f"SELECT id FROM {table_name} WHERE id = {transaction_id}"
        exists = execute_query(check_query)
        
        if not exists:
            return jsonify({
                'success': False,
                'error': 'NOT_FOUND',
                'message': f'Transaction with ID {transaction_id} not found'
            }), 404
        
        # Build UPDATE query
        set_clauses = []
        
        if 'truck' in data:
            set_clauses.append(f"truck = '{data['truck']}'")
        if 'driver' in data:
            set_clauses.append(f"driver = '{data['driver']}'")
        if 'gross' in data:
            set_clauses.append(f"gross = {data['gross']}")
        if 'net' in data:
            set_clauses.append(f"net = {data['net']}")
        
        if not set_clauses:
            return jsonify({
                'success': False,
                'error': 'NO_FIELDS',
                'message': 'No fields to update'
            }), 400
        
        # Add updated_at timestamp
        updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        set_clauses.append(f"updated_at = '{updated_at}'")
        
        query = f"""
            UPDATE {table_name}
            SET {', '.join(set_clauses)}
            WHERE id = {transaction_id}
        """
        
        # Execute update
        affected_rows = execute_statement(query)
        
        if affected_rows == 0:
            return jsonify({
                'success': False,
                'error': 'UPDATE_FAILED',
                'message': 'Failed to update transaction'
            }), 500
        
        # Get updated record
        get_query = f"SELECT * FROM {table_name} WHERE id = {transaction_id}"
        updated_record = execute_query(get_query)
        
        response = {
            'success': True,
            'message': 'Transaction updated successfully',
            'data': updated_record[0] if updated_record else None
        }
        
        return jsonify(response), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': f'Failed to update transaction {transaction_id}'
        }), 500

print("✅ PUT /api/v1/transactions/<id> endpoint created!")
print("\n📋 Updateable fields:")
print("   - truck (optional)")
print("   - driver (optional)")
print("   - gross (optional)")
print("   - net (optional)")

# ==================== DELETE ENDPOINT - REMOVE ====================

print("🔴 Creating DELETE endpoint for removing transactions...\n")

@app.route(f'{BASE_URL}/transactions/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id: int):
    """
    DELETE /api/v1/transactions/<id>
    
    Path Parameters:
        - transaction_id: ID of the transaction to delete
    
    Returns:
        JSON confirmation message
    """
    try:
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        
        # Check if transaction exists
        check_query = f"SELECT * FROM {table_name} WHERE id = {transaction_id}"
        exists = execute_query(check_query)
        
        if not exists:
            return jsonify({
                'success': False,
                'error': 'NOT_FOUND',
                'message': f'Transaction with ID {transaction_id} not found'
            }), 404
        
        # Store the record before deletion for response
        deleted_record = exists[0]
        
        # Delete query
        query = f"DELETE FROM {table_name} WHERE id = {transaction_id}"
        
        # Execute delete
        affected_rows = execute_statement(query)
        
        if affected_rows == 0:
            return jsonify({
                'success': False,
                'error': 'DELETE_FAILED',
                'message': 'Failed to delete transaction'
            }), 500
        
        response = {
            'success': True,
            'message': f'Transaction {transaction_id} deleted successfully',
            'deleted_data': deleted_record
        }
        
        return jsonify(response), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': f'Failed to delete transaction {transaction_id}'
        }), 500

print("✅ DELETE /api/v1/transactions/<id> endpoint created!")
print("\n⚠️ Warning: Deletion is permanent!")
print("\n📋 Returns:")
print("   - 200: Transaction deleted")
print("   - 404: Transaction not found")
print("   - 500: Server error")

# ==================== ERROR HANDLERS ====================

print("⚠️ Setting up error handlers...\n")

# 404 Not Found Handler
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'NOT_FOUND',
        'message': 'Endpoint not found',
        'path': request.path
    }), 404

# 405 Method Not Allowed Handler
@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'error': 'METHOD_NOT_ALLOWED',
        'message': f'Method {request.method} not allowed for this endpoint',
        'allowed_methods': error.valid_methods if hasattr(error, 'valid_methods') else None
    }), 405

# 500 Internal Server Error Handler
@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'INTERNAL_SERVER_ERROR',
        'message': 'An internal error occurred',
        'details': str(error) if app.debug else None
    }), 500

# Generic Exception Handler
@app.errorhandler(Exception)
def handle_exception(error):
    return jsonify({
        'success': False,
        'error': 'UNHANDLED_EXCEPTION',
        'message': str(error) if app.debug else 'An unexpected error occurred'
    }), 500

print("✅ Error handlers configured!")

# ==================== HEALTH CHECK ENDPOINT ====================

print("\n💚 Setting up health check endpoint...\n")

@app.route(f'{BASE_URL}/health', methods=['GET'])
def health_check():
    """
    GET /api/v1/health
    
    Health check endpoint to verify API is running and database is accessible.
    """
    try:
        # Test database connection
        table_name = f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}"
        test_query = f"SELECT COUNT(*) as count FROM {table_name} LIMIT 1"
        
        result = execute_query(test_query)
        db_accessible = True
        record_count = result[0]['count'] if result else 0
    
    except Exception as e:
        db_accessible = False
        record_count = None
    
    status = 'healthy' if db_accessible else 'unhealthy'
    status_code = 200 if db_accessible else 503
    
    response = {
        'success': db_accessible,
        'status': status,
        'service': 'Databricks REST API',
        'version': API_VERSION,
        'database': {
            'accessible': db_accessible,
            'table': f"{DB_CONFIG['catalog']}.{DB_CONFIG['schema']}.{DB_CONFIG['table']}",
            'record_count': record_count
        },
        'timestamp': datetime.now().isoformat()
    }
    
    return jsonify(response), status_code

print("✅ Health check endpoint created!")
print("\n📋 Available endpoints:")
print("   GET  /api/v1/health")
print("   GET  /api/v1/transactions")
print("   GET  /api/v1/transactions/<id>")
print("   POST /api/v1/transactions")
print("   PUT  /api/v1/transactions/<id>")
print("   DELETE /api/v1/transactions/<id>")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)