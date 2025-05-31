#!/bin/bash
# scripts/run_all.sh

echo "🌍 Starting GlobeMart Complete Pipeline"
echo "======================================="

# Function to cleanup on exit
cleanup() {
    echo "🛑 Shutting down all processes..."
    kill $(jobs -p) 2>/dev/null
    docker-compose down
    exit
}

# Register cleanup function
trap cleanup SIGINT SIGTERM

# Kill any existing Python processes
taskkill //F //IM python.exe 2>/dev/null || true

# Start Kafka and Zookeeper
echo "🚀 Starting Kafka and Zookeeper..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

# Start FX producer
echo "💱 Starting FX rate producer..."
python scripts/fx_producer.py &

# Start the pipeline
echo "📊 Starting data pipeline..."
python src/main.py &

# Start the dashboard
echo "📊 Starting Streamlit dashboard..."
streamlit run src/dashboard/streamlit_app.py

# This will keep the script running until Ctrl+C
wait