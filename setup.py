"""
Setup script for Ship Lineup Data Pipeline
"""
import os
import sys
from pathlib import Path
from config import Config


def create_directories():
    """Create necessary directories"""
    print("Creating directories...")
    Config.create_directories()
    print("✓ Directories created successfully")


def create_sample_config():
    """Create sample configuration file"""
    print("Creating sample configuration...")
    
    sample_config = """# Ship Lineup Data Pipeline Configuration
# Copy this file to .env and modify as needed

# Database Configuration
DATABASE_URL=sqlite:///ship_lineup.db
# For PostgreSQL: DATABASE_URL=postgresql://username:password@localhost:5432/ship_lineup_db

# Data Storage Paths
BRONZE_DATA_PATH=./data/bronze
SILVER_DATA_PATH=./data/silver
GOLD_DATA_PATH=./data/gold

# Logging
LOG_LEVEL=INFO
LOG_FILE=./logs/ship_lineup.log

# API Configuration
REQUEST_TIMEOUT=30
MAX_RETRIES=3
"""
    
    config_file = Path('.env.example')
    with open(config_file, 'w') as f:
        f.write(sample_config)
    
    print("✓ Sample configuration created (.env.example)")


def create_gitignore():
    """Create .gitignore file"""
    print("Creating .gitignore...")
    
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual Environment
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Data files
data/
logs/
*.db
*.sqlite
*.sqlite3

# Environment variables
.env
.env.local

# OS
.DS_Store
Thumbs.db

# Jupyter Notebooks
.ipynb_checkpoints/

# pytest
.pytest_cache/

# Coverage
htmlcov/
.coverage
.coverage.*
coverage.xml
"""
    
    gitignore_file = Path('.gitignore')
    with open(gitignore_file, 'w') as f:
        f.write(gitignore_content)
    
    print("✓ .gitignore created")


def create_docker_files():
    """Create Docker configuration files"""
    print("Creating Docker files...")
    
    # Dockerfile
    dockerfile_content = """FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/bronze data/silver data/gold logs

# Set environment variables
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO

# Default command
CMD ["python", "main.py", "scheduler"]
"""
    
    with open('Dockerfile', 'w') as f:
        f.write(dockerfile_content)
    
    # docker-compose.yml
    docker_compose_content = """version: '3.8'

services:
  ship-lineup-pipeline:
    build: .
    container_name: ship-lineup-pipeline
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - DATABASE_URL=postgresql://postgres:password@db:5432/ship_lineup_db
      - LOG_LEVEL=INFO
    depends_on:
      - db
    restart: unless-stopped

  db:
    image: postgres:13
    container_name: ship-lineup-db
    environment:
      - POSTGRES_DB=ship_lineup_db
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
"""
    
    with open('docker-compose.yml', 'w') as f:
        f.write(docker_compose_content)
    
    print("✓ Docker files created")


def create_makefile():
    """Create Makefile for common operations"""
    print("Creating Makefile...")
    
    makefile_content = """# Ship Lineup Data Pipeline Makefile

.PHONY: help install test clean run-daily run-incremental run-scheduler run-test

help: ## Show this help message
	@echo "Ship Lineup Data Pipeline - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \\033[36m%-20s\\033[0m %s\\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

setup: ## Initial setup (create directories, config files)
	python setup.py

test: ## Run system test
	python main.py test

run-daily: ## Run daily data collection
	python main.py daily

run-incremental: ## Run incremental data update
	python main.py incremental

run-scheduler: ## Run automated scheduler
	python main.py scheduler

run-manual: ## Run manual collection (requires START_DATE and END_DATE)
	python main.py manual --start-date $(START_DATE) --end-date $(END_DATE)

clean: ## Clean up data and logs
	rm -rf data/bronze/* data/silver/* data/gold/*
	rm -rf logs/*

docker-build: ## Build Docker image
	docker build -t ship-lineup-pipeline .

docker-run: ## Run with Docker Compose
	docker-compose up -d

docker-stop: ## Stop Docker containers
	docker-compose down

docker-logs: ## View Docker logs
	docker-compose logs -f

example: ## Run example usage
	python examples/example_usage.py
"""
    
    with open('Makefile', 'w') as f:
        f.write(makefile_content)
    
    print("✓ Makefile created")


def create_github_workflows():
    """Create GitHub Actions workflow"""
    print("Creating GitHub Actions workflow...")
    
    workflow_dir = Path('.github/workflows')
    workflow_dir.mkdir(parents=True, exist_ok=True)
    
    workflow_content = """name: CI/CD Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, "3.10"]

    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run tests
      run: |
        python main.py test
    
    - name: Run example
      run: |
        python examples/example_usage.py

  lint:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: "3.9"
    
    - name: Install linting tools
      run: |
        pip install flake8 black isort
    
    - name: Run linting
      run: |
        flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics
        black --check src/
        isort --check-only src/
"""
    
    workflow_file = workflow_dir / 'ci.yml'
    with open(workflow_file, 'w') as f:
        f.write(workflow_content)
    
    print("✓ GitHub Actions workflow created")


def main():
    """Main setup function"""
    print("Ship Lineup Data Pipeline - Setup")
    print("=" * 40)
    
    try:
        create_directories()
        create_sample_config()
        create_gitignore()
        create_docker_files()
        create_makefile()
        create_github_workflows()
        
        print("\n" + "=" * 40)
        print("✓ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Copy .env.example to .env and configure your settings")
        print("2. Install dependencies: pip install -r requirements.txt")
        print("3. Run test: python main.py test")
        print("4. Run example: python examples/example_usage.py")
        
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

