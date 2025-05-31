Write-Host "Starting GlobeMart Complete Pipeline"
Write-Host "======================================="

# Function to cleanup on exit
function Cleanup {
    Write-Host "Shutting down all processes..."
    Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
    docker-compose down
    exit
}

# Register cleanup function
$null = Register-EngineEvent PowerShell.Exiting -Action { Cleanup }

# Kill any existing Python processes and remove DuckDB files
Write-Host "Cleaning up existing processes..."
docker-compose down
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force
Remove-Item -Force data/analytics.duckdb* -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Start Docker containers
Write-Host "Starting Docker containers..."
docker-compose down
docker-compose up -d
Write-Host "Waiting for services to be ready..."
Start-Sleep -Seconds 20

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

# Keep running until Ctrl+C
try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    Cleanup
} 