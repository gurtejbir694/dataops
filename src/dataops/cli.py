import typer
import subprocess
import shutil
from pathlib import Path
from dataops.config import load_config, save_default_config, get_project_root
from dataops.utils import start_services, stop_services, check_status, tail_log
from dataops.logging import setup_logger
from dataops.data import generate_synthetic_data
from dataops.quality import run_quality_checks

app = typer.Typer(help="Generic DataOps CLI for data quality pipelines")

@app.command()
def init(verbose: bool = False):
    """Initialize the project."""
    logger = setup_logger(verbose)
    logger.info("Initializing DataOps project...")
    config = load_config()
    project_root = get_project_root()
    airflow_dags = Path(config["airflow"]["home"]) / "dags"
    airflow_dags.mkdir(parents=True, exist_ok=True)
    Path(config["data_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["log_dir"]).mkdir(parents=True, exist_ok=True)
    save_default_config()
    
    if shutil.which("airflow"):
        env = {"AIRFLOW_HOME": str(config["airflow"]["home"])}
        try:
            subprocess.run([str(project_root / ".venv/bin/airflow"), "db", "migrate"], check=True, env=env)
            subprocess.run([str(project_root / ".venv/bin/airflow"), "users", "create", "--username", "admin", "--firstname", "Admin", "--lastname", "User", "--role", "Admin", "--email", "admin@example.com", "--password", "admin"], check=True, env=env)
            logger.info("Airflow initialized")
        except subprocess.CalledProcessError as e:
            logger.error(f"Airflow initialization failed: {e}")
            raise typer.Exit(code=1)
    
    logger.info("Initialization complete")
    typer.echo(f"Initialization complete. Copy scripts (if Airflow): cp {project_root}/src/dataops/*.py {airflow_dags}/")

@app.command()
def start(verbose: bool = False):
    """Start Airflow and Streamlit services."""
    logger = setup_logger(verbose)
    config = load_config()
    logger.info("Starting services...")
    try:
        start_services(config)
        typer.echo("Services started: http://localhost:8080 (Airflow), http://localhost:8501 (Streamlit)")
    except Exception as e:
        logger.error(f"Start services failed: {e}")
        raise typer.Exit(code=1)

@app.command()
def stop(verbose: bool = False):
    """Stop Airflow and Streamlit services."""
    logger = setup_logger(verbose)
    logger.info("Stopping services...")
    try:
        stop_services()
        typer.echo("Services stopped")
    except Exception as e:
        logger.error(f"Stop services failed: {e}")
        raise typer.Exit(code=1)

@app.command()
def trigger(verbose: bool = False):
    """Trigger the data_quality_dag."""
    logger = setup_logger(verbose)
    config = load_config()
    project_root = get_project_root()
    airflow_home = Path(config["airflow"]["home"])
    logger.info("Triggering data_quality_dag...")
    if not shutil.which("airflow"):
        logger.error("Airflow not installed")
        typer.echo("Error: Airflow not installed")
        raise typer.Exit(code=1)
    env = {"AIRFLOW_HOME": str(airflow_home)}
    try:
        subprocess.run([str(project_root / ".venv/bin/airflow"), "dags", "trigger", "data_quality_dag"], check=True, env=env)
        typer.echo("DAG triggered: http://localhost:8080")
    except subprocess.CalledProcessError as e:
        logger.error(f"DAG trigger failed: {e}")
        raise typer.Exit(code=1)

@app.command()
def status(verbose: bool = False):
    """Check service status."""
    logger = setup_logger(verbose)
    config = load_config()
    logger.info("Checking service status...")
    check_status(config)

@app.command()
def logs(file: str = "data_quality", verbose: bool = False):
    """Display logs: data_quality, webserver, scheduler, streamlit."""
    logger = setup_logger(verbose)
    config = load_config()
    log_dir = Path(config["log_dir"])
    log_paths = {
        "data_quality": log_dir / "data_quality.log",
        "webserver": log_dir / "webserver.log",
        "scheduler": log_dir / "scheduler.log",
        "streamlit": log_dir / "streamlit.log",
    }
    if file not in log_paths:
        typer.echo(f"Invalid log file. Options: {', '.join(log_paths.keys())}")
        raise typer.Exit(code=1)
    log_file = log_paths[file]
    if not log_file.exists():
        typer.echo(f"Log file {log_file} does not exist")
        raise typer.Exit(code=1)
    tail_log(log_file)

@app.command()
def generate(n: int = 100, verbose: bool = False):
    """Generate synthetic data."""
    logger = setup_logger(verbose)
    config = load_config()
    try:
        generate_synthetic_data(n, config, verbose)
        typer.echo("Synthetic data generated")
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        raise typer.Exit(code=1)

@app.command()
def check_quality(
    source: str = "db",
    csv_path: str = None,
    table_name: str = "data_table",
    checks_config: str = None,
    verbose: bool = False
):
    """Run data quality checks."""
    logger = setup_logger(verbose)
    config = load_config()
    if source == "csv" and not csv_path:
        typer.echo("CSV path required for csv source")
        raise typer.Exit(code=1)
    try:
        results = run_quality_checks(
            source, csv_path, config, verbose, table_name, checks_config
        )
        typer.echo(f"Quality Check Results: {results}")
    except Exception as e:
        logger.error(f"Quality check failed: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
