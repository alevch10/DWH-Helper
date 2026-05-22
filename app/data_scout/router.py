"""API endpoints for data analysis."""

from fastapi import APIRouter, Query
from fastapi.responses import Response
import yaml

from app.config.logger import get_logger
from app.data_scout.service import analyze_data

logger = get_logger(__name__)
router = APIRouter()


@router.get("/data_analysis")
async def get_data_analysis(path: str = Query(..., description="S3 path prefix")) -> Response:
    """
    Analyze data structure in S3.

    Args:
        path: S3 prefix path (e.g., "amplitude_exports/2024/")

    Returns:
        YAML-formatted schema profile
    """
    try:
        result = analyze_data(path)
        yaml_output = yaml.dump(result, default_flow_style=False, allow_unicode=True, sort_keys=False)
        return Response(content=yaml_output, media_type="application/x-yaml")

    except ValueError as e:
        error_response = {"error": "No zip files found", "detail": str(e)}
        yaml_output = yaml.dump(error_response, default_flow_style=False)
        return Response(
            content=yaml_output,
            status_code=404,
            media_type="application/x-yaml",
        )
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        error_response = {"error": "Internal server error", "detail": str(e)}
        yaml_output = yaml.dump(error_response, default_flow_style=False)
        return Response(
            content=yaml_output,
            status_code=500,
            media_type="application/x-yaml",
        )
