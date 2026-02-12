import os
import tempfile
import zipfile
from typing import AsyncGenerator


async def create_ndjson_zip(
    lines_iterator: AsyncGenerator[str, None],
    archive_name: str,
    ndjson_filename: str,
) -> str:
    """
    Consume lines from the async iterator, write them as newline-delimited JSON
    into a temporary .ndjson file, pack it into a ZIP, and return the path to
    the temporary ZIP file.
    """
    # Temporary directory for the .ndjson file
    with tempfile.TemporaryDirectory() as tmpdir:
        ndjson_path = os.path.join(tmpdir, ndjson_filename)
        # Write all lines, one per line
        with open(ndjson_path, "w", encoding="utf-8") as f:
            async for line in lines_iterator:
                f.write(line + "\n")

        # Create ZIP containing only that file
        zip_path = os.path.join(tmpdir, archive_name)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(ndjson_path, arcname=ndjson_filename)

        # Copy the ZIP to a persistent temporary file (outlives the context)
        persistent_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        with open(zip_path, "rb") as src, open(persistent_zip.name, "wb") as dst:
            dst.write(src.read())

    return persistent_zip.name
