from fastapi import FastAPI
from data_generator import *

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, Alpine Docker FastAPI!"}

@app.get("/products")
def products():
    return generate_fake_products()


@app.get("/users")
def users():
    return generate_fake_users()


@app.get("/transactions")
def transactions():
    return generate_fake_transaction_data()