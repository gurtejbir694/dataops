import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dataops.logging import setup_logger
import plotly.express as px

def main(config: dict):
    logger = setup_logger(False, Path(config["log_dir"]))
    st.title("Data Quality Dashboard")
    
    db_type = config["database"]["type"]
    if db_type == "sqlite":
        engine = create_engine(f"sqlite:///{config['database']['path']}")
    else:
        engine = create_engine(
            f"postgresql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['name']}"
        )
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT date, metric, value FROM results ORDER BY date DESC", conn)
        
        if df.empty:
            st.write("No data available")
            return
        
        fig = px.line(df, x="date", y="value", color="metric", title="Data Quality Metrics Over Time")
        st.plotly_chart(fig)
        st.subheader("Recent Results")
        st.dataframe(df.head(10))
    except Exception as e:
        logger.error(f"Dashboard failed: {e}")
        st.error(f"Error: {e}")

if __name__ == "__main__":
    from dataops.config import load_config
    main(load_config())
