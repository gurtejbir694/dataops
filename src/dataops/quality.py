import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from pathlib import Path
import yaml
import re
from dataops.logging import setup_logger

def run_quality_checks(
    source: str,
    csv_path: str = None,
    config: dict = None,
    verbose: bool = False,
    table_name: str = "data_table",
    checks_config_path: str = None
):
    logger = setup_logger(verbose, Path(config["log_dir"]) / "data_quality.log")
    
    # Load checks configuration
    if checks_config_path:
        with open(checks_config_path, "r") as f:
            checks_config = yaml.safe_load(f)
    else:
        checks_config = {
            "fields": [
                {"name": "id", "type": "string", "checks": {"not_null": True}},
                {"name": "name", "type": "string", "checks": {"not_null": True}},
                {"name": "email", "type": "string", "checks": {"regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}},
                {"name": "age", "type": "integer", "checks": {"range": [18, 80]}},
                {"name": "salary", "type": "integer", "checks": {"range": [30000, 120000]}}
            ]
        }
    
    # Initialize engine
    db_type = config["database"]["type"]
    if db_type == "sqlite":
        engine = create_engine(f"sqlite:///{config['database']['path']}")
    else:
        engine = create_engine(
            f"postgresql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['name']}"
        )
    
    # Load data
    if source == "csv":
        if not csv_path or not Path(csv_path).exists():
            raise ValueError("Valid CSV path required")
        df = pd.read_csv(csv_path)
    else:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    
    # Perform quality checks
    results = {}
    for field in checks_config.get("fields", []):
        field_name = field["name"]
        if field_name not in df.columns:
            logger.warning(f"Field {field_name} not found in data")
            continue
        
        field_results = {}
        checks = field.get("checks", {})
        
        if "not_null" in checks and checks["not_null"]:
            field_results["nulls"] = int(df[field_name].isnull().sum())
        
        if "regex" in checks:
            regex = checks["regex"]
            field_results["invalid_format"] = len(df[~df[field_name].str.match(regex, na=False)])
        
        if "range" in checks and field["type"] in ["integer", "float"]:
            min_val, max_val = checks["range"]
            field_results["out_of_range"] = len(df[(df[field_name] < min_val) | (df[field_name] > max_val)])
        
        if "positive" in checks and checks["positive"]:
            field_results["non_positive"] = len(df[df[field_name] <= 0])
        
        if "unique" in checks and checks["unique"]:
            field_results["duplicates"] = len(df[df[field_name].duplicated()])
        
        results[field_name] = field_results
        for check, value in field_results.items():
            logger.info(f"Field {field_name} - {check}: {value}")
    
    # Save results to database
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS results (
                date TIMESTAMP,
                field TEXT,
                metric TEXT,
                value INTEGER
            )
        """))
        current_time = datetime.now()
        for field_name, field_results in results.items():
            for metric, value in field_results.items():
                conn.execute(
                    text("INSERT INTO results (date, field, metric, value) VALUES (:date, :field, :metric, :value)"),
                    {"date": current_time, "field": field_name, "metric": metric, "value": value}
                )
        conn.commit()
    
    return results
