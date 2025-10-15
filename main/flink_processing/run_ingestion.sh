#!/bin/bash

# MongoDB Ingestion Pipeline Runner Script
# This script sets up the environment and runs the PyFlink MongoDB ingestion pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo -e "${BLUE}🚀 PyFlink MongoDB Ingestion Pipeline${NC}"
echo "=================================================="

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
print_status "Checking prerequisites..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed or not in PATH"
    exit 1
fi

# Check if MongoDB is running (optional check)
if command -v mongosh &> /dev/null; then
    if ! mongosh --eval "db.adminCommand('ping')" --quiet &> /dev/null; then
        print_warning "MongoDB connection test failed. Make sure MongoDB is running."
    else
        print_status "MongoDB connection OK"
    fi
else
    print_warning "MongoDB shell (mongosh) not found. Cannot test connection."
fi

# Check if input file exists
INPUT_FILE="${PROJECT_ROOT}/data_scraping/output/reddit_posts_20251007_225355.json"
if [ ! -f "$INPUT_FILE" ]; then
    print_error "Input file not found: $INPUT_FILE"
    print_error "Please run the data scraping pipeline first."
    exit 1
fi

print_status "Input file found: $INPUT_FILE"

# Load environment variables if .env file exists
ENV_FILE="${SCRIPT_DIR}/.env"
if [ -f "$ENV_FILE" ]; then
    print_status "Loading environment variables from .env file"
    export $(cat "$ENV_FILE" | grep -v '^#' | xargs)
else
    print_warning ".env file not found. Using default configuration."
    print_warning "Copy .env.example to .env and customize your settings."
fi

# Set default environment variables
export MONGODB_HOST="${MONGODB_HOST:-localhost}"
export MONGODB_PORT="${MONGODB_PORT:-27017}"
export MONGODB_DATABASE="${MONGODB_DATABASE:-crypto_sentiment}"
export MONGODB_COLLECTION="${MONGODB_COLLECTION:-reddit_posts}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

print_status "Configuration:"
print_status "  MongoDB: ${MONGODB_HOST}:${MONGODB_PORT}"
print_status "  Database: ${MONGODB_DATABASE}"
print_status "  Collection: ${MONGODB_COLLECTION}"
print_status "  Input file: $(basename "$INPUT_FILE")"

# Check if we should install dependencies
if [ "$1" = "--install-deps" ]; then
    print_status "Installing Python dependencies..."
    pip3 install -r "${SCRIPT_DIR}/requirements.txt"
fi

# Ask for confirmation unless --yes flag is provided
if [ "$1" != "--yes" ] && [ "$2" != "--yes" ]; then
    echo
    read -p "Do you want to proceed with the ingestion? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Operation cancelled by user."
        exit 0
    fi
fi

# Change to script directory
cd "$SCRIPT_DIR"

# Run the ingestion pipeline
print_status "Starting PyFlink MongoDB ingestion pipeline..."
echo "=================================================="

# Check if running in Docker environment
if [ -f "/.dockerenv" ]; then
    print_status "Running in Docker environment"
    # In Docker, use the full path
    export PYTHONPATH="/home/pyflink:$PYTHONPATH"
fi

# Execute the Python script
python3 mongodb_ingestion.py

exit_code=$?

echo "=================================================="
if [ $exit_code -eq 0 ]; then
    print_status "✅ Pipeline completed successfully!"
    print_status "Check your MongoDB collection for the ingested data:"
    print_status "  Database: ${MONGODB_DATABASE}"
    print_status "  Collection: ${MONGODB_COLLECTION}"
    
    # Provide MongoDB query examples
    echo
    print_status "Example MongoDB queries:"
    echo "  mongosh ${MONGODB_DATABASE}"
    echo "  > db.${MONGODB_COLLECTION}.countDocuments()"
    echo "  > db.${MONGODB_COLLECTION}.find({platform: 'reddit'}).limit(5)"
    echo "  > db.${MONGODB_COLLECTION}.find({'meta.subreddit': 'CryptoCurrency'}).count()"
    
else
    print_error "❌ Pipeline failed with exit code $exit_code"
    print_error "Check the logs above for error details."
    exit $exit_code
fi