#!/bin/bash
echo "=== NSE Camarilla Scanner Deployment Script ==="

# 1. Fetch latest changes from GitHub
echo "Pulling latest code from GitHub..."
git fetch origin
git reset --hard origin/main

# 2. Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
elif [ -d "env" ]; then
    echo "Activating environment..."
    source env/bin/activate
fi

# 3. Install dependencies
echo "Updating packages..."
pip install -r requirements.txt

# 4. Restarting application
echo "Deployment complete! Please restart your service now."
echo "If using systemd:  sudo systemctl restart nsescanner"
echo "If using PM2:      pm2 restart dashboard"
echo "If using Gunicorn: kill and restart your screen/tmux session"
