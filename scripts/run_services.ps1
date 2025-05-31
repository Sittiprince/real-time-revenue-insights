# Set environment variables
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_DB = "globemart"
$env:POSTGRES_USER = "dataeng"
$env:POSTGRES_PASSWORD = "pipeline123"
$env:BATCH_SIZE = "100"
$env:UPDATE_INTERVAL = "10"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_TOPIC_TRANSACTIONS = "globemart.transactions"
$env:KAFKA_TOPIC_FX_RATES = "globemart.fx_rates"

# Kill any existing Python processes
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
Remove-Item -Force data/analytics.duckdb* -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Start FX rate generator
Write-Host "Starting FX rate generator..."
Start-Process python -ArgumentList "src/data_generation/fx_rate_generator.py"

# Start transaction generator
Write-Host "Starting transaction generator..."
Start-Process python -ArgumentList "src/data_generation/transaction_generator.py"

# Wait a bit longer for generators to initialize
Start-Sleep -Seconds 10

# Start the dashboard
Write-Host "Starting Streamlit dashboard..."
streamlit run src/dashboard/streamlit_app.py 