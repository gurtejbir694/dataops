import logging
from pathlib import Path

def setup_logger(verbose: bool, log_dir: Path = None):
    logger = logging.getLogger("dataops")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    
    if log_dir:
        log_dir.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
