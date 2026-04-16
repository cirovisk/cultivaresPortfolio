FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for psycopg2 and other tools
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code (will be overwritten by volume in dev, but good for build)
COPY . .

# Environment variable defaults
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=cultivares_user
ENV POSTGRES_PASSWORD=cultivares_password
ENV POSTGRES_DB=cultivares_db

# Default command can be empty or keep the notebook one
CMD ["python", "src/main.py"]
