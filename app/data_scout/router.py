from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
import yaml

from app.auth.deps import require_read
from app.config.logger import get_logger
from app.config.settings import settings
from app.data_scout.service import analyze_sample
from app.data_scout.schemas import AnalyzeSampleRequest

logger = get_logger(__name__)
router = APIRouter()


@router.post("/analyze-sample", summary="Analyze sample of data")
async def analyze_sample_endpoint(
    request: AnalyzeSampleRequest, user=Depends(require_read)
):
    """Analyze first N rows from first NDJSON file in S3 prefix."""
    try:
        bucket = settings.s3.bucket_name
        report = analyze_sample(
            bucket=bucket,
            prefix=request.path,
            sample_size=request.sample_size,
            max_samples_per_field=request.max_samples_per_field,
        )
        yaml_output = yaml.dump(
            report, default_flow_style=False, allow_unicode=True, sort_keys=False
        )
        return Response(content=yaml_output, media_type="application/x-yaml")
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        raise HTTPException(500, detail=str(e))
