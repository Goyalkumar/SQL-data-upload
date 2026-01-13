#!/bin/bash
################################################################################
# Bulk UPSERT Script - Linux/Mac Launcher
################################################################################

set -e  # Exit on error

# Change to script directory
cd "$(dirname "$0")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${BLUE}============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    echo "Please install Python 3.8 or higher"
    exit 1
fi

print_success "Python 3 found: $(python3 --version)"

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_warning ".env file not found!"
    echo "Please copy .env.template to .env and configure your settings"
    echo ""
    read -p "Do you want to create .env from template now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cp .env.template .env
        print_success ".env file created. Please edit it with your database credentials"
        exit 0
    else
        exit 1
    fi
fi

print_success ".env file found"

# Check if dependencies are installed
if ! python3 -c "import pandas, pyodbc, dotenv" 2>/dev/null; then
    print_warning "Required dependencies not found"
    echo "Installing dependencies..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        print_error "Failed to install dependencies"
        exit 1
    fi
    print_success "Dependencies installed"
fi

print_success "All dependencies are installed"
echo ""

# Main menu
print_header "Bulk UPSERT Script Launcher"
echo ""
echo "Options:"
echo "  1. Run normal execution (commit changes)"
echo "  2. Run dry-run mode (preview only)"
echo "  3. Run with custom CSV path"
echo "  4. View latest log file"
echo "  5. Run tests"
echo "  6. Exit"
echo ""

read -p "Select option (1-6): " choice

case $choice in
    1)
        echo ""
        print_header "Starting Normal Execution"
        python3 bulk_upsert_enhanced.py
        EXIT_CODE=$?
        ;;
    2)
        echo ""
        print_header "Starting Dry-Run Mode"
        python3 bulk_upsert_enhanced.py --dry-run
        EXIT_CODE=$?
        ;;
    3)
        echo ""
        read -p "Enter CSV file path: " csv_path
        echo ""
        print_header "Starting Execution with Custom Path"
        python3 bulk_upsert_enhanced.py --csv-path "$csv_path"
        EXIT_CODE=$?
        ;;
    4)
        echo ""
        LATEST_LOG=$(ls -t logs/*.log 2>/dev/null | head -n1)
        if [ -n "$LATEST_LOG" ]; then
            print_header "Latest Log File: $LATEST_LOG"
            less "$LATEST_LOG"
        else
            print_warning "No log files found"
        fi
        exit 0
        ;;
    5)
        echo ""
        print_header "Running Tests"
        if command -v pytest &> /dev/null; then
            pytest test_bulk_upsert.py -v
        else
            print_warning "pytest not installed. Installing..."
            pip3 install pytest
            pytest test_bulk_upsert.py -v
        fi
        exit $?
        ;;
    6)
        echo "Exiting..."
        exit 0
        ;;
    *)
        print_error "Invalid option"
        exit 1
        ;;
esac

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    print_header "SUCCESS: Script completed successfully"
    print_success "Check logs directory for detailed execution logs"
else
    print_header "ERROR: Script failed with exit code $EXIT_CODE"
    print_error "Check the log files in the logs/ directory for details"
    echo ""
    
    # Show last 20 lines of latest log
    LATEST_LOG=$(ls -t logs/*.log 2>/dev/null | head -n1)
    if [ -n "$LATEST_LOG" ]; then
        echo "Last 20 lines of log:"
        echo "----------------------------------------"
        tail -n 20 "$LATEST_LOG"
    fi
fi

echo ""
read -p "Press Enter to exit..."
exit $EXIT_CODE
