#!/bin/bash

set -e  # Stop script execution on error

PYTHON_VERSION="3.10.10"
VENV_DIR="venv"

# Detect the operating system
OS="$(uname -s)"

install_python() {
    echo "Python $PYTHON_VERSION not found. Installing..."
    case "$OS" in
        Linux*)
            sudo apt update && sudo apt install -y python$PYTHON_VERSION python$PYTHON_VERSION-venv
            ;;
        Darwin*)
            brew install python@$PYTHON_VERSION
            ;;
        CYGWIN*|MINGW*|MSYS*)
            echo "Please install Python $PYTHON_VERSION manually on Windows."
            exit 1
            ;;
        *)
            echo "Unsupported operating system."
            exit 1
            ;;
    esac
}

# Check if Python 3.10.10 is installed
if command -v python$PYTHON_VERSION &>/dev/null; then
    PYTHON_EXEC="python$PYTHON_VERSION"
elif command -v python3 &>/dev/null && [[ "$(python3 --version)" == *"$PYTHON_VERSION"* ]]; then
    PYTHON_EXEC="python3"
elif command -v python &>/dev/null && [[ "$(python --version)" == *"$PYTHON_VERSION"* ]]; then
    PYTHON_EXEC="python"
else
    install_python
    PYTHON_EXEC="python$PYTHON_VERSION"
fi

echo "Using Python at: $(which $PYTHON_EXEC)"

# Create virtual environment if it does not exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    $PYTHON_EXEC -m venv $VENV_DIR
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source $VENV_DIR/bin/activate 2>/dev/null || source $VENV_DIR/Scripts/activate 2>/dev/null

# Install required packages
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup completed. To activate the environment, use:"
echo "source $VENV_DIR/bin/activate"