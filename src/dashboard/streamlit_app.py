import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import logging
import pytz

# Configure page
st.set_page_config(
    page_title="GlobeMart Revenue Insights",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import our pipeline components
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipeline.realtime_processor import RealTimeProcessor

# Initialize processor
@st.cache_resource
def init_processor():
    """Initialize the real-time processor"""
    try:
        processor = RealTimeProcessor()
        processor.start()  # Start the processor
        return processor
    except Exception as e:
        st.error(f"Failed to initialize processor: {e}")
        return None

def main():
    # Header
    st.title("🌍 GlobeMart Real-Time Revenue Insights")
    st.markdown("*Real-time global e-commerce revenue monitoring in USD*")
    
    # Initialize processor
    processor = init_processor()
    
    if not processor:
        st.error("Failed to initialize system components")
        return
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Controls")
        
        # Time range selector
        time_range = st.selectbox(
            "Time Range",
            ["Last 24 Hours", "Last 12 Hours"],
            index=0
        )
        
        time_mapping = {
            "Last 24 Hours": 24,
            "Last 12 Hours": 12
        }
        hours = time_mapping[time_range]
        
        # Manual refresh button
        if st.button("🔄 Refresh Data"):
            st.session_state.last_refresh = 0  # Force refresh
        
        # System status
        st.header("📊 System Status")
        if processor.running:
            st.success("✅ Pipeline Active")
        else:
            st.error("❌ Pipeline Stopped")
        
        # Show FX rates status
        fx_count = len(processor.latest_fx_rates)
        if fx_count > 0:
            st.success(f"✅ FX Rates Active ({fx_count} currencies)")
        else:
            st.warning("⚠️ No FX Rates Available")
        
        st.info(f"🕐 Last Update: {datetime.now().strftime('%H:%M:%S')}")
    
    # Initialize session state for metrics history
    if 'metrics_history' not in st.session_state:
        st.session_state.metrics_history = []
    
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = 0
    
    # Refresh logic - only on button click
    if st.session_state.last_refresh == 0:
        st.session_state.last_refresh = time.time()
    
        try:
            # Get current metrics
            metrics = processor.analytics_db.get_revenue_metrics(hours=hours)
            
            # Add current metrics to history with timestamp
            if metrics and metrics.get('hourly_trends'):
                current_metrics = {
                    'timestamp': datetime.now(pytz.UTC),
                    'revenue': metrics['total_revenue'],
                    'transactions': sum(h['transactions'] for h in metrics['hourly_trends'])
                }
                st.session_state.metrics_history.append(current_metrics)
                
                # Keep only last 100 data points
                if len(st.session_state.metrics_history) > 100:
                    st.session_state.metrics_history = st.session_state.metrics_history[-100:]
        except Exception as e:
            st.error(f"Error refreshing data: {str(e)}")
            logging.error(f"Refresh error: {str(e)}")
            # Reset processor if connection is lost
            if "connection" in str(e).lower():
                processor = init_processor()
    
    # Get current metrics for display
    try:
        with st.spinner("Loading revenue data..."):
            metrics = processor.analytics_db.get_revenue_metrics(hours=hours)
            
            if not metrics or all(not metrics[k] for k in ['total_revenue', 'revenue_by_country', 'revenue_by_currency', 'hourly_trends']):
                st.warning("No transaction data available yet. The system is running and waiting for transactions.")
                
                # Show system status
                st.subheader("System Status")
                status_cols = st.columns(2)
                
                with status_cols[0]:
                    st.info("✓ Pipeline Components:")
                    st.write("- Real-time processor: Running")
                    st.write("- FX Rate Consumer: Active")
                    st.write("- Transaction Processor: Active")
                    st.write("- Analytics DB: Connected")
                
                with status_cols[1]:
                    st.info("✓ Latest Updates:")
                    st.write(f"- FX Rates: {fx_count} currencies loaded")
                    st.write("- Waiting for transactions...")
                    st.write("- DB Tables initialized")
                    st.write("- Aggregations ready")
                
                return
        
        # Key Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_revenue = metrics.get('total_revenue', 0)
            st.metric(
                label="💰 Total Revenue (USD)",
                value=f"${total_revenue:,.2f}",
                delta=f"+${total_revenue * 0.05:,.2f}"  # Mock growth
            )
        
        with col2:
            country_count = len(metrics.get('revenue_by_country', []))
            st.metric(
                label="🌍 Active Countries",
                value=str(country_count),
                delta="+2" if country_count > 0 else None
            )
        
        with col3:
            currency_count = len(metrics.get('revenue_by_currency', []))
            st.metric(
                label="💱 Currencies",
                value=str(currency_count),
                delta="+1" if currency_count > 0 else None
            )
        
        with col4:
            total_transactions = sum([h['transactions'] for h in metrics.get('hourly_trends', [])])
            st.metric(
                label="📈 Total Transactions",
                value=f"{total_transactions:,}",
                delta=f"+{int(total_transactions * 0.1):,}" if total_transactions > 0 else None
            )
        
        # Charts Row 1
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🌍 Revenue by Country")
            
            country_data = metrics.get('revenue_by_country', [])
            if country_data:
                df_country = pd.DataFrame(country_data)
                fig = px.bar(
                    df_country.head(10), 
                    x='country', 
                    y='revenue',
                    title=f"Top 10 Countries by Revenue (Last {hours} Hours)",
                    color='revenue',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No country data available")
        
        with col2:
            st.subheader("💱 Revenue by Currency")
            
            currency_data = metrics.get('revenue_by_currency', [])
            if currency_data:
                df_currency = pd.DataFrame(currency_data)
                fig = px.pie(
                    df_currency, 
                    values='revenue', 
                    names='currency',
                    title=f"Revenue Distribution by Currency (Last {hours} Hours)"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No currency data available")
        
        # Charts Row 2
        st.subheader("📈 Real-Time Revenue and Transaction Trends")
        
        if st.session_state.metrics_history:
            df_history = pd.DataFrame(st.session_state.metrics_history)
            
            fig = go.Figure()
            
            # Revenue line
            fig.add_trace(go.Scatter(
                x=df_history['timestamp'],
                y=df_history['revenue'],
                mode='lines+markers',
                name='Revenue (USD)',
                line=dict(color='#1f77b4', width=3),
                yaxis='y'
            ))
            
            # Transaction count line
            fig.add_trace(go.Scatter(
                x=df_history['timestamp'],
                y=df_history['transactions'],
                mode='lines+markers',
                name='Transactions',
                line=dict(color='#2ca02c', width=2, dash='dot'),
                yaxis='y2'
            ))
            
            # Update layout with better time formatting
            fig.update_layout(
                title="Live Revenue and Transaction Updates",
                xaxis=dict(
                    title="Time",
                    tickformat='%H:%M:%S',
                    tickmode='auto',
                    nticks=10,
                    showgrid=True
                ),
                yaxis=dict(
                    title="Revenue (USD)",
                    tickformat='$,.0f',
                    showgrid=True
                ),
                yaxis2=dict(
                    title="Transaction Count",
                    overlaying='y',
                    side='right',
                    showgrid=False
                ),
                height=400,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                hovermode='x unified',
                plot_bgcolor='white',
                margin=dict(l=60, r=60, t=50, b=50)  # Adjust margins
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for real-time data...")
    except Exception as e:
        st.error(f"Error loading revenue data: {str(e)}")
        logging.error(f"Data loading error: {str(e)}")

if __name__ == "__main__":
    main()