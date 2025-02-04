# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install
RUN python -m playwright install-deps

# Copy application code
COPY . .

# Environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "cannabis_scraper.py"]
