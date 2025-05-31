import subprocess
import sys
import time
import os
import signal
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_component(command, name):
    """Run a component and return the process"""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=True
        )
        logger.info(f"Started {name}")
        return process
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}")
        return None

def main():
    # Ensure the data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Start components
    processes = []
    
    # Start FX Rate Generator
    fx_gen = run_component(
        "python src/data_generation/fx_rate_generator.py",
        "FX Rate Generator"
    )
    if fx_gen:
        processes.append(('FX Rate Generator', fx_gen))
    
    # Wait for Kafka to receive some FX rates
    time.sleep(5)
    
    # Start Transaction Generator
    tx_gen = run_component(
        "python src/data_generation/transaction_generator.py",
        "Transaction Generator"
    )
    if tx_gen:
        processes.append(('Transaction Generator', tx_gen))
    
    # Wait for some transactions to be generated
    time.sleep(5)
    
    # Start Streamlit Dashboard
    dashboard = run_component(
        "streamlit run src/dashboard/streamlit_app.py --server.port 8502",
        "Streamlit Dashboard"
    )
    if dashboard:
        processes.append(('Streamlit Dashboard', dashboard))
    
    try:
        logger.info("All components started. Press Ctrl+C to stop...")
        
        # Monitor processes
        while True:
            time.sleep(1)
            
            # Check if any process has terminated
            for name, process in processes:
                if process.poll() is not None:
                    logger.error(f"{name} has terminated unexpectedly")
                    # Terminate all processes
                    for _, p in processes:
                        try:
                            p.terminate()
                        except:
                            pass
                    sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
        # Terminate all processes
        for name, process in processes:
            logger.info(f"Stopping {name}...")
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
        
        logger.info("All components stopped")

if __name__ == "__main__":
    main() 