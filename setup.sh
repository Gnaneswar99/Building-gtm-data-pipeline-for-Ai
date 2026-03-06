#!/bin/bash

# GTM Data Pipeline Setup Script

echo "Setting up GTM Data Pipeline environment..."

# Check Python version
python3 --version >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "Setup complete!"
echo ""
echo "To get started:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Run the mock server: python mock_server.py"
echo "3. Implement and run your pipeline"
echo ""
echo "The mock server will be available at http://localhost:8000"
echo "API documentation will be at http://localhost:8000/docs"
