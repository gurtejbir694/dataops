import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
import uuid6
from pathlib import Path
import yaml
import random
from datetime import datetime, timedelta
from dataops.logging import setup_logger

def generate_synthetic_data(n: int, config: dict, verbose: bool, checks_config_path: str = None):
    logger = setup_logger(verbose, Path(config["log_dir"]) / "data_quality.log")
    fake = Faker()
    
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
    
    # Generate data based on fields
    data = []
    for _ in range(n):
        row = {}
        for field in checks_config.get("fields", []):
            field_name = field["name"]
            field_type = field["type"]
            checks = field.get("checks", {})
            
            if field_type == "string":
                if "regex" in checks and "email" in field_name.lower():
                    value = fake.email()
                else:
                    value = fake.word() if random.random() > 0.1 else None
            elif field_type == "integer":
                if "range" in checks:
                    min_val, max_val = checks["range"]
                    value = random.randint(min_val, max_val) if random.random() > 0.1 else None
                else:
                    value = random.randint(1, 1000)
            elif field_type == "float":
                value = round(random.uniform(0, 1000), 2) if random.random() > 0.1 else None
            elif field_type == "date":
                value = fake.date_between(start_date="-10y", end_date="today") if random.random() > 0.1 else None
            else:
                value = str(uuid6.uuid7())
            
            row[field_name] = value
        data.append(row)
    
    df = pd.DataFrame(data)
    
    data_dir = Path(config["data_dir"])
    data_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(data_dir / "sample.csv", index=False)
    logger.info(f"Saved synthetic data to {data_dir / 'sample.csv'}")
    
    db_type = config["database"]["type"]
    if db_type == "sqlite":
        engine = create_engine(f"sqlite:///{config['database']['path']}")
    else:
        engine = create_engine(
            f"postgresql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['name']}"
        )
    
    with engine.connect() as conn:
        # Create table dynamically
        columns = ", ".join([f"{field['name']} {field['type'].upper()}" for field in checks_config["fields"]])
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS data_table (
                {columns}
            )
        """))
        df.to_sql("data_table", conn, if_exists="append", index=False)
        conn.commit()
        logger.info(f"Inserted {n} rows into data_table")
