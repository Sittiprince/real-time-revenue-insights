import pandas as pd
import streamlit as st
import psycopg2
from kafka import KafkaProducer
import duckdb

print("✅ All packages imported successfully!")
print(f"Python version: {__import__('sys').version}")
print(f"Pandas version: {pd.__version__}")
print("🎉 Virtual environment is ready!")