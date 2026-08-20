"""
Shared utilities for medallion pipeline.
"""

import logging
import sys
from datetime import datetime
from typing import Dict, Any, Optional

import json


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Configure a logger with console and optional file output.
    
    Args:
        name: Logger name
        level: Logging level (INFO, DEBUG, ERROR, etc.)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Console handler with timestamp
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def log_step(step_num: int, step_name: str, logger: logging.Logger):
    """Log pipeline step start with formatted output."""
    logger.info(f"\n{'='*80}")
    logger.info(f"STEP {step_num}: {step_name}")
    logger.info(f"{'='*80}")


def log_metrics(metrics: Dict[str, Any], logger: logging.Logger):
    """Log metrics in JSON format for monitoring."""
    logger.info(f"METRICS: {json.dumps(metrics, indent=2, default=str)}")


def validate_dataframe(df, schema_requirements: Optional[Dict[str, str]] = None) -> bool:
    """
    Validate DataFrame schema and data quality.
    
    Args:
        df: PySpark DataFrame
        schema_requirements: Dict of column_name: expected_type
    
    Returns:
        True if validation passes, False otherwise
    """
    if df.count() == 0:
        return False
    
    if schema_requirements:
        df_schema = {field.name: field.dataType.typeName() for field in df.schema}
        for col, expected_type in schema_requirements.items():
            if col not in df_schema or str(df_schema[col]).lower() != expected_type.lower():
                return False
    
    return True


def get_row_count(df) -> int:
    """Safely get row count from DataFrame."""
    return df.count()


def benchmark_operation(operation_name: str, logger: logging.Logger):
    """
    Decorator to time and log operation execution.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = datetime.now()
            logger.info(f"Starting: {operation_name}")
            result = func(*args, **kwargs)
            duration = (datetime.now() - start).total_seconds()
            logger.info(f"Completed: {operation_name} (Duration: {duration:.2f}s)")
            return result
        return wrapper
    return decorator
