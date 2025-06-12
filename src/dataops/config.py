import yaml
from pathlib import Path
import os
from typing import Optional
from dotenv import load_dotenv

def get_project_root() -> Path:
    return Path.cwd().absolute()

def load_config() -> dict:
    project_root = get_project_root()
    config_path = project_root / ".dataops" / "config.yaml"
    default_config = {
        "data_dir": str(project_root / ".dataops" / "data"),
        "log_dir": str(project_root / ".dataops" / "logs"),
        "database": {
            "type": "sqlite",
            "path": str(project_root / ".dataops" / "data.db"),
            "host": "localhost",
            "port": 5432,
            "name": "customer_data",
            "user": "admin",
            "password": "password",
        },
        "airflow": {
            "home": str(project_root / ".dataops" / "airflow"),
        },
    }
    
    if config_path.exists():
        with open(config_path, "r") as f:
            user_config = yaml.safe_load(f) or {}
        default_config.update(user_config)
    
    # Override with environment variables
    load_dotenv()
    default_config["database"]["user"] = os.getenv("DB_USER", default_config["database"]["user"])
    default_config["database"]["password"] = os.getenv("DB_PASSWORD", default_config["database"]["password"])
    
    return default_config

def save_default_config():
    project_root = get_project_root()
    config_path = project_root / ".dataops" / "config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config = load_config()
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)
