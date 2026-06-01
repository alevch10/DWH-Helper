from pydantic import BaseModel, Field


class AnalyzeSampleRequest(BaseModel):
    path: str = Field(..., description="S3 prefix path")
    sample_size: int = Field(1000, description="Number of rows to sample")
    max_samples_per_field: int = Field(100, description="Max unique samples per field")
