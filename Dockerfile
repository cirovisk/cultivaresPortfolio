FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Default command: execute notebook and export to HTML
CMD ["jupyter", "nbconvert", "--to", "html", "--execute", "relatorio_cultivares.ipynb", "--output", "report.html"]
