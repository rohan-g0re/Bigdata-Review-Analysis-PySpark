import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# Ensure logs directory exists
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")
os.makedirs(logs_dir, exist_ok=True)

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Set up a logger with console and file handlers
    
    Args:
        name (str): Logger name
        log_file (str, optional): Path to log file. If None, only console handler is used.
        level (int, optional): Logging level. Defaults to logging.INFO.
    
    Returns:
        logging.Logger: Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log_file is provided
    if log_file:
        file_path = os.path.join(logs_dir, log_file)
        file_handler = RotatingFileHandler(
            file_path, 
            maxBytes=10*1024*1024,  # 10 MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Example usage:
# logger = setup_logger("producer", "producer.log")
# logger.info("Producer started") 