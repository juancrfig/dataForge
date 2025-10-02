from fastapi import FastAPI


# Create an instance of the FastAPI class
app = FastAPI(
    title="DataForge API",
    description="API for ingesting and processing e-commerce  data."
)

@app.get("/")
def read_root():
    """
    Root endpoint.
    """
    return {"status": "ok", "message": "Hello, World!"}