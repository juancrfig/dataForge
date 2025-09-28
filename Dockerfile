# Stage 1: Build the dependencies
FROM python:3.13.3 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
     build-essential \
     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /app/wheels -r requirements.txt


# Stage 2: Create the final production image
FROM python:3.13.3

# Create a non-root user for security
RUN useradd --create-home appuser
WORKDIR /home/appuser/app

# Copy installed dependencies from the builder stage
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install the Python dependencies from local wheels
RUN pip install --no-cache /wheels/*

# Copy the application source code
COPY src/ .

# Change ownership of the app directory to the non-root user
RUN chown -R appuser:appuser /home/appuser

USER appuser

EXPOSE 8000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "4", "-b", "0.0.0.0:8000", "api.app:app"]