import pandas as pd
import psycopg2
import sqlite3
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict, Optional
import socket
import threading
import queue

load_dotenv()

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.pool = queue.Queue(maxsize=max_connections)
        self.local = threading.local()
        
        # Initialize the pool with connections
        for _ in range(max_connections):
            conn = sqlite3.connect(db_path, check_same_thread=False)
            self.pool.put(conn)
    
    def get_connection(self):
        """Get a connection from the pool"""
        if not hasattr(self.local, 'connection'):
            self.local.connection = self.pool.get()
        return self.local.connection
    
    def release_connection(self, conn):
        """Release a connection back to the pool"""
        if hasattr(self.local, 'connection'):
            self.pool.put(conn)
            delattr(self.local, 'connection')
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()

class PostgreSQLConnector:
    def __init__(self, host=None, port=5432, database='globemart', user='dataeng', password='pipeline123', test_mode=False):
        """Initialize database connector
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            test_mode: If True, use SQLite for testing
        """
        self.test_mode = test_mode
        if test_mode:
            self.db_path = 'tests/data/test.db'
            self.pool = SQLiteConnectionPool(self.db_path)
            self.conn = None
        else:
            self.conn_params = {
                'host': host or self._get_db_host(),
                'port': port,
                'database': database,
                'user': user,
                'password': password
            }
            self.conn = None
            self._connect()
    
    def _get_db_host(self):
        """Get the correct database host based on environment"""
        try:
            # Try to resolve the Docker container name
            socket.gethostbyname('globemart_postgres')
            return 'globemart_postgres'
        except socket.gaierror:
            return 'localhost'
    
    def _connect(self):
        """Establish database connection"""
        try:
            if self.test_mode:
                self.conn = self.pool.get_connection()
            else:
                if self.conn is None or self.conn.closed:
                    logger.info(f"Connecting to PostgreSQL at {self.conn_params['host']}")
                    self.conn = psycopg2.connect(**self.conn_params)
                    logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            logger.exception("Full traceback:")
            raise
    
    def _ensure_connection(self):
        """Ensure we have a valid connection"""
        try:
            if self.test_mode:
                self.conn = self.pool.get_connection()
                try:
                    cur = self.conn.cursor()
                    cur.execute('SELECT 1')
                    cur.close()
                    return True
                except:
                    return False
            else:
                if self.conn and not self.conn.closed:
                    # Test the connection
                    cur = self.conn.cursor()
                    cur.execute('SELECT 1')
                    cur.close()
                    return True
        except:
            pass
        
        # Reconnect if needed
        try:
            self._connect()
            return True
        except:
            return False
    
    def test_connection(self):
        """Test if the connection is alive and working"""
        if not self._ensure_connection():
            raise Exception("Failed to establish database connection")
        return True
    
    def reconnect(self):
        """Attempt to reconnect to the database"""
        try:
            if self.conn:
                try:
                    if not self.test_mode:
                        self.conn.close()
                except:
                    pass
            self._connect()
            return True
        except Exception as e:
            logger.error(f"Failed to reconnect: {e}")
            raise
    
    def get_transactions_by_timerange(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict]:
        """Get transactions within a time range"""
        self._ensure_connection()  # Ensure connection is active
        
        try:
            if self.test_mode:
                query = """
                    SELECT *
                    FROM transactions
                    WHERE transaction_time >= ?
                    AND transaction_time < ?
                    ORDER BY transaction_time ASC
                """
                cursor = self.conn.cursor()
                cursor.execute(query, (start_time.isoformat(), end_time.isoformat()))
                columns = [desc[0] for desc in cursor.description]
                transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
            else:
                with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT *
                        FROM transactions
                        WHERE transaction_time >= %s
                        AND transaction_time < %s
                        ORDER BY transaction_time ASC
                    """, (start_time, end_time))
                    transactions = cur.fetchall()
                
                logger.info(f"Retrieved {len(transactions)} transactions")
                return transactions
                
        except Exception as e:
            logger.error(f"Error retrieving transactions: {e}")
            logger.exception("Full traceback:")
            return []
    
    def close(self):
        """Close the database connection"""
        if self.test_mode:
            if hasattr(self, 'pool'):
                self.pool.close_all()
        else:
            if self.conn:
                self.conn.close()
                self.conn = None
    
    def get_connection(self):
        """Get the current database connection"""
        return self.conn
    
    def fetch_hourly_batch(self, batch_hour=None):
        """Fetch transactions for a specific hour
        
        Args:
            batch_hour: The hour to fetch transactions for. If None, uses previous hour.
            
        Returns:
            pandas.DataFrame: DataFrame containing the transactions
        """
        if batch_hour is None:
            batch_hour = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            
        start_time = batch_hour
        end_time = start_time + timedelta(hours=1)
        
        try:
            if self.test_mode:
                query = """
                    SELECT *
                    FROM transactions
                    WHERE transaction_time >= ?
                    AND transaction_time < ?
                    ORDER BY transaction_time ASC
                """
                cursor = self.conn.cursor()
                cursor.execute(query, (start_time.isoformat(), end_time.isoformat()))
                columns = [desc[0] for desc in cursor.description]
                transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return pd.DataFrame(transactions)
            else:
                with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT *
                        FROM transactions
                        WHERE transaction_time >= %s
                        AND transaction_time < %s
                        ORDER BY transaction_time ASC
                    """, (start_time, end_time))
                    transactions = cur.fetchall()
                    
                if transactions:
                    return pd.DataFrame(transactions)
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching hourly batch: {e}")
            logger.exception("Full traceback:")
            return pd.DataFrame()
    
    def get_recent_transactions(self, hours=1, batch_size=1000):
        """Get recent transactions from the last N hours
        
        Args:
            hours: Number of hours to look back
            batch_size: Maximum number of transactions to return
            
        Returns:
            List[Dict]: List of transactions
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        try:
            if self.test_mode:
                query = """
                    SELECT *
                    FROM transactions
                    WHERE transaction_time >= ?
                    AND transaction_time < ?
                    ORDER BY transaction_time DESC
                    LIMIT ?
                """
                cursor = self.conn.cursor()
                cursor.execute(query, (start_time.isoformat(), end_time.isoformat(), batch_size))
                columns = [desc[0] for desc in cursor.description]
                transactions = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return transactions
            else:
                with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT *
                        FROM transactions
                        WHERE transaction_time >= %s
                        AND transaction_time < %s
                        ORDER BY transaction_time DESC
                        LIMIT %s
                    """, (start_time, end_time, batch_size))
                    return cur.fetchall()
                
        except Exception as e:
            logger.error(f"Error getting recent transactions: {e}")
            logger.exception("Full traceback:")
            return []
    
    def get_transaction_summary(self):
        """Get summary statistics of transactions
        
        Returns:
            Dict: Summary statistics
        """
        try:
            if self.test_mode:
                query = """
                    SELECT 
                        COUNT(*) as total_transactions,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount,
                        MIN(transaction_time) as earliest_transaction,
                        MAX(transaction_time) as latest_transaction
                    FROM transactions
                """
                cursor = self.conn.cursor()
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                summary = dict(zip(columns, cursor.fetchone()))
                cursor.close()
                return summary
            else:
                with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_transactions,
                            SUM(amount) as total_amount,
                            AVG(amount) as avg_amount,
                            MIN(transaction_time) as earliest_transaction,
                            MAX(transaction_time) as latest_transaction
                        FROM transactions
                    """)
                    return cur.fetchone()
                
        except Exception as e:
            logger.error(f"Error getting transaction summary: {e}")
            logger.exception("Full traceback:")
            return {
                'total_transactions': 0,
                'total_amount': 0,
                'avg_amount': 0,
                'earliest_transaction': None,
                'latest_transaction': None
            }

# Test the connector
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test connection
    db = PostgreSQLConnector()
    
    # Test getting transactions
    end = datetime.now()
    start = end.replace(hour=0, minute=0, second=0, microsecond=0)  # Start of day
    
    transactions = db.get_transactions_by_timerange(start, end)
    print(f"Found {len(transactions)} transactions today")
    
    if transactions:
        print("\nSample transaction:")
        print(transactions[0])
    
    db.close()