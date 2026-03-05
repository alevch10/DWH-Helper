import io

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.auth.deps import require_read
from app.config.settings import settings
from .client import MetrikaClient
from . import schemas
from .services import generate_report
from .ad_efficiency import get_ad_efficiency

router = APIRouter()


def get_token_from_header(request: Request) -> str:
    """Извлекает токен из заголовка Authorization (поддерживает Bearer и OAuth)."""
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = auth_header.split()
    if len(parts) != 2:
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization header format. Expected 'Bearer <token>' or 'OAuth <token>'",
        )
    scheme, token = parts[0], parts[1]
    if scheme.lower() not in ["bearer", "oauth"]:
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization scheme. Expected Bearer or OAuth",
        )
    return token


@router.get("/counters", response_model=schemas.CountersResponse)
async def get_counters(
    request: Request,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Получить список доступных счетчиков"""
    client = MetrikaClient(token)
    try:
        data = await client.get_counters()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.post("/logrequests", response_model=schemas.LogRequest)
async def create_logrequest(
    request: Request,
    counter_id: int = Query(...),
    date1: str = Query(...),
    date2: str = Query(...),
    source: str = Query("hits"),
    fields: str = Query(
        settings.yandexmetrica.default_fields, description="Список полей через запятую"
    ),
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Создать запрос логов"""
    client = MetrikaClient(token)
    try:
        data = await client.create_logrequest(counter_id, date1, date2, fields, source)
        return data["log_request"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.get("/logrequests", response_model=schemas.LogRequestsResponse)
async def get_logrequests(
    request: Request,
    counter_id: int = Query(...),
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Получить список запросов логов"""
    client = MetrikaClient(token)
    try:
        data = await client.get_logrequests(counter_id)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.get("/logrequests/evaluate", response_model=schemas.LogRequestEvaluation)
async def evaluate_logrequest(
    request: Request,
    counter_id: int = Query(...),
    date1: str = Query(...),
    date2: str = Query(...),
    source: str = Query("hits"),
    fields: str = Query(
        settings.yandexmetrica.default_fields, description="Список полей через запятую"
    ),
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Оценить возможность создания запроса"""
    client = MetrikaClient(token)
    try:
        data = await client.evaluate_logrequest(
            counter_id, date1, date2, fields, source
        )
        return data["log_request_evaluation"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.get("/logrequest/{request_id}", response_model=schemas.LogRequest)
async def get_logrequest_info(
    request: Request,
    counter_id: int = Query(...),
    request_id: int = ...,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Получить информацию о запросе логов"""
    client = MetrikaClient(token)
    try:
        data = await client.get_logrequest_info(counter_id, request_id)
        return data["log_request"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.post("/logrequest/{request_id}/clean", response_model=schemas.LogRequest)
async def clean_logrequest(
    request: Request,
    counter_id: int = Query(...),
    request_id: int = ...,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Очистить подготовленные логи обработанного запроса"""
    client = MetrikaClient(token)
    try:
        data = await client.clean_logrequest(counter_id, request_id)
        return data["log_request"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.post("/logrequest/{request_id}/cancel", response_model=schemas.LogRequest)
async def cancel_logrequest(
    request: Request,
    counter_id: int = Query(...),
    request_id: int = ...,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Отменить не обработанный запрос логов"""
    client = MetrikaClient(token)
    try:
        data = await client.cancel_logrequest(counter_id, request_id)
        return data["log_request"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.get("/logrequest/{request_id}/part/{part_number}/download")
async def download_part(
    request: Request,
    counter_id: int = Query(...),
    request_id: int = ...,
    part_number: int = ...,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """Загрузить часть подготовленных логов"""
    client = MetrikaClient(token)
    try:
        content = await client.download_part(counter_id, request_id, part_number)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename=part_{part_number}.tsv"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.close()


@router.post("/report")
async def prepare_report(
    request: Request,
    counter_id: int = Query(...),
    date1: str = Query(...),
    date2: str = Query(...),
    source: str = Query("hits"),
    fields: str = Query(
        settings.yandexmetrica.default_fields, description="Список полей через запятую"
    ),
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """
    Подготавливает единый отчет: создает запрос, ожидает обработки, скачивает все части,
    объединяет и возвращает ZIP-архив с CSV-файлом.
    """
    try:
        zip_data = await generate_report(
            token=token,
            counter_id=counter_id,
            date1=date1,
            date2=date2,
            source=source,
            fields=fields,
        )
        return StreamingResponse(
            io.BytesIO(zip_data),
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename=metrika_report_{date1}_{date2}.zip"
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ad_efficiency", response_model=schemas.ProcessDayResponse)
async def ad_efficiency(
    request: schemas.ProcessDayRequest,
    token: str = Depends(get_token_from_header),
    user=Depends(require_read),
):
    """
    Запускает ETL-процесс для указанного дня.
    Возвращает количество записей в каждой витрине после обработки.
    """
    try:
        # Если fields не передан, используем значение по умолчанию из настроек
        fields_to_use = request.fields or settings.yandexmetrica.default_fields.split(
            ","
        )
        statistics = await get_ad_efficiency(
            token=token,
            counter_id=request.counter_id,
            date=request.date,
            source=request.source,
            fields=fields_to_use,
        )
        return schemas.ProcessDayResponse(
            status="success", statistics=statistics, message="Данные успешно обработаны"
        )
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Ошибка обработки: {str(e)}")
