"""Processor module for orchestrating Amplitude to S3 export."""

import yaml
from pathlib import Path
from app.config.logger import get_logger

logger = get_logger(__name__)

MAPPINGS_PATH = Path(__file__).parent / "field_mappings.yaml"
MAPPINGS = yaml.safe_load(MAPPINGS_PATH.read_text())

logger.info("Loaded field mappings from %s", MAPPINGS_PATH)
