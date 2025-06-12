import subprocess
import psutil
from pathlib import Path
from dataops.logging import setup_logger
import shutil

def start_services(config: dict):
    logger = setup_logger(False, Path(config["log_dir"]))
    project_root = Path.cwd()
    airflow_home = Path(config["airflow"]["home"])
    log_dir = Path(config["log_dir"])
    
    if not shutil.which("airflow"):
        logger.warning("Airflow not installed, skipping Airflow services")
    else:
        env = {"AIRFLOW_HOME": str(airflow_home)}
        try:
            subprocess.Popen([str(project_root / ".venv/bin/airflow"), "scheduler"], env=env, stdout=open(log_dir / "scheduler.log", "a"), stderr=subprocess.STDOUT)
            subprocess.Popen([str(project_root / ".venv/bin/airflow"), "webserver", "--port", "8080"], env=env, stdout=open(log_dir / "webserver.log", "a"), stderr=subprocess.STDOUT)
            logger.info("Airflow services started")
        except Exception as e:
            logger.error(f"Failed to start Airflow services: {e}")
            raise
    
    if not shutil.which("streamlit"):
        logger.warning("Streamlit not installed, skipping Streamlit service")
    else:
        try:
            subprocess.Popen([str(project_root / ".venv/bin/streamlit"), "run", str(project_root / "src/dataops/dashboard.py")], stdout=open(log_dir / "streamlit.log", "a"), stderr=subprocess.STDOUT)
            logger.info("Streamlit service started")
        except Exception as e:
            logger.error(f"Failed to start Streamlit service: {e}")
            raise

def stop_services():
    logger = setup_logger(False)
    stopped = {"airflow": False, "streamlit": False}
    for proc in psutil.process_iter(["name"]):
        if proc.info["name"] in ["airflow", "streamlit"]:
            proc.kill()
            stopped[proc.info["name"]] = True
    for service, was_stopped in stopped.items():
        if was_stopped:
            logger.info(f"Stopped {service}")
        else:
            logger.info(f"No {service} processes found")

def check_status(config: dict):
    logger = setup_logger(False, Path(config["log_dir"]) / "status.log")
    services = {"airflow": False, "streamlit": False}
    for proc in psutil.process_iter(["name"]):
        if proc.info["name"] == "airflow":
            services["airflow"] = True
        elif proc.info["name"] == "streamlit":
            services["streamlit"] = True
    for service, running in services.items():
        print(f"{service}: {'Running' if running else 'Stopped'}")
        logger.info(f"{service}: {'Running' if running else 'Stopped'}")

def tail_log(log_file: Path):
    if not log_file.exists():
        raise FileNotFoundError(f"Log file {log_file} does not exist")
    with open(log_file, "r") as f:
        lines = f.readlines()[-10:]
        for line in lines:
            print(line.strip())
