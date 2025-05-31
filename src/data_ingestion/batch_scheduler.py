import logging
import time
from datetime import datetime, timedelta
import threading
from typing import Optional
from .batch_processor import BatchProcessor

logger = logging.getLogger(__name__)

class BatchScheduler:
    def __init__(self, batch_processor: Optional[BatchProcessor] = None):
        """Initialize the batch scheduler
        
        Args:
            batch_processor: Optional BatchProcessor instance. If None, creates a new one.
        """
        self.processor = batch_processor or BatchProcessor()
        self._stop_event = threading.Event()
        self._scheduler_thread = None
        self._lock = threading.Lock()
        self.running = False
        
    def start(self):
        """Start the scheduler in a background thread"""
        with self._lock:
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                logger.warning("Scheduler is already running")
                return
                
            self._stop_event.clear()
            self.running = True
            self._scheduler_thread = threading.Thread(target=self._run_scheduler)
            self._scheduler_thread.daemon = True
            self._scheduler_thread.start()
            logger.info("Batch scheduler started")
        
    def stop(self):
        """Stop the scheduler and cleanup"""
        logger.info("Stopping batch scheduler...")
        
        with self._lock:
            self.running = False
            self._stop_event.set()
            
            if self._scheduler_thread:
                try:
                    self._scheduler_thread.join(timeout=5)  # Wait up to 5 seconds
                except:
                    pass
                self._scheduler_thread = None
                
            try:
                self.processor.close()
            except:
                pass
                
        logger.info("Batch scheduler stopped")
        
    def _run_scheduler(self):
        """Main scheduler loop"""
        try:
            # Process any backlog first
            if self.running:
                self.processor.process_backlog(hours=24)
            
            while not self._stop_event.is_set() and self.running:
                now = datetime.now()
                
                # Calculate time until next hour
                next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                wait_seconds = (next_hour - now).total_seconds()
                
                # Wait until next hour, checking stop event every second
                while wait_seconds > 0 and not self._stop_event.is_set() and self.running:
                    time.sleep(min(1, wait_seconds))  # Check more frequently
                    wait_seconds -= 1
                
                if self._stop_event.is_set() or not self.running:
                    break
                    
                # Process the previous hour's data
                previous_hour = next_hour - timedelta(hours=1)
                try:
                    if self.running:  # Double check we're still running
                        self.processor.process_hourly_batch(previous_hour)
                except Exception as e:
                    if self.running:  # Only log if we're still supposed to be running
                        logger.error(f"Failed to process batch: {e}")
                        logger.exception("Full traceback:")
                    
        except Exception as e:
            if self.running:  # Only log if we're still supposed to be running
                logger.error(f"Scheduler error: {e}")
                logger.exception("Full traceback:")
            
        finally:
            logger.info("Scheduler loop ended")
            
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()

# Test the scheduler
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    scheduler = BatchScheduler()
    try:
        scheduler.start()
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        scheduler.stop() 