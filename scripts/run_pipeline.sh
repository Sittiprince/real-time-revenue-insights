#!/bin/bash
# scripts/run_pipeline.sh

echo "🚀 GlobeMart Real-Time Revenue Pipeline Setup"
echo "=============================================="

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data logs

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check if PostgreSQL is ready
echo "🔍 Checking PostgreSQL connection..."
until docker-compose exec -T postgres pg_isready -U globemart; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Check if Kafka is ready
echo "🔍 Checking Kafka connection..."
sleep 5

# Setup database
echo "🗄️ Setting up database..."
python scripts/setup_database.py

# Generate initial data
echo "📊 Generating sample data..."
python scripts/data_generator.py

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start FX rate producer: python scripts/fx_producer.py"
echo "2. Start main pipeline: python main.py"
echo "3. Start dashboard: streamlit run src/dashboard/streamlit_app.py"
echo ""
echo "Or run all together:"
echo "bash scripts/run_all.sh"