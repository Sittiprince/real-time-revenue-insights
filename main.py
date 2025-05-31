import logging
import asyncio
import signal
import sys
from datetime import datetime

from src.pipeline.realtime_processor import RealTimeProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class PipelineManager:
    def __init__(self):
        self.processor = None
        self.running = False
    
    def start_pipeline(self):
        """Start the complete pipeline"""
        try:
            logging.info("Starting GlobeMart Real-Time Revenue Pipeline")
            logging.info(f"Start time: {datetime.now()}")
            
            # Initialize processor
            self.processor = RealTimeProcessor()
            
            # Start all processing threads
            threads = self.processor.start()
            
            self.running = True
            logging.info("Pipeline started successfully")
            
            # Keep main thread alive
            try:
                while self.running:
                    # Print status every 60 seconds
                    metrics = self.processor.get_current_metrics()
                    total_revenue = metrics.get('total_revenue', 0)
                    logging.info(f"Current total revenue: ${total_revenue:,.2f}")
                    
                    # Sleep for 60 seconds
                    import time
                    time.sleep(60)
                    
            except KeyboardInterrupt:
                logging.info("Shutdown signal received")
                self.stop_pipeline()
                
        except Exception as e:
            logging.error(f"Failed to start pipeline: {e}")
            sys.exit(1)
    
    def stop_pipeline(self):
        """Stop the pipeline gracefully"""
        logging.info("Stopping pipeline...")
        self.running = False
        
        if self.processor:
            self.processor.stop()
        
        logging.info("Pipeline stopped successfully")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logging.info(f"Received signal {signum}")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start pipeline
    manager = PipelineManager()
    manager.start_pipeline()