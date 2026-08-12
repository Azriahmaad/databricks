from fastapi import FastAPI, Query, HTTPException
from databricks.sdk import WorkspaceClient
import os

app = FastAPI(title="Customers API - wim.public.customers")
w = WorkspaceClient()

WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]
TABLE = "wim.public.customers"


def execute_query(sql: str):
    """Helper untuk execute SQL dan return list of dicts."""
    stmt = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        wait_timeout="30s"
    )
    if not stmt.result or not stmt.result.data_array:
        return []
    columns = [col.name for col in stmt.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in stmt.result.data_array]


@app.get("/")
def root():
    return {"message": "Customers API is running", "table": TABLE}


@app.get("/customers")
def list_customers(limit: int = Query(10, le=100), offset: int = 0):
    """GET /customers - List all customers with pagination."""
    data = execute_query(f"SELECT * FROM {TABLE} LIMIT {limit} OFFSET {offset}")
    count = execute_query(f"SELECT COUNT(*) as total FROM {TABLE}")
    return {
        "data": data,
        "total": count[0]["total"] if count else 0,
        "limit": limit,
        "offset": offset
    }


@app.get("/customers/{customer_id}")
def get_customer(customer_id: int):
    """GET /customers/{id} - Get single customer."""
    data = execute_query(f"SELECT * FROM {TABLE} WHERE id = {customer_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"data": data[0]}


@app.post("/customers")
def create_customer(payload: dict):
    """POST /customers - Create new customer."""
    cols = ", ".join(payload.keys())
    vals = ", ".join(
        [f"\'{v}\'" if isinstance(v, str) else str(v) for v in payload.values()]
    )
    execute_query(f"INSERT INTO {TABLE} ({cols}) VALUES ({vals})")
    return {"message": "Customer created", "data": payload}


@app.put("/customers/{customer_id}")
def update_customer(customer_id: int, payload: dict):
    """PUT /customers/{id} - Update customer."""
    set_clause = ", ".join(
        [f"{k} = \'{v}\'" if isinstance(v, str) else f"{k} = {v}"
         for k, v in payload.items()]
    )
    execute_query(f"UPDATE {TABLE} SET {set_clause} WHERE id = {customer_id}")
    return {"message": f"Customer {customer_id} updated"}


@app.delete("/customers/{customer_id}")
def delete_customer(customer_id: int):
    """DELETE /customers/{id} - Delete customer."""
    execute_query(f"DELETE FROM {TABLE} WHERE id = {customer_id}")
    return {"message": f"Customer {customer_id} deleted"}
