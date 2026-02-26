import io

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.auth.deps import require_read, require_write
from .client import MetrikaClient
from . import schemas
from .service import generate_report

router = APIRouter()

# Дефолтный набор полей из задания
DEFAULT_FIELDS = (
    "ym:pv:watchID,ym:pv:pageViewID,ym:pv:visitID,ym:pv:counterID,ym:pv:clientID,"
    "ym:pv:counterUserIDHash,ym:pv:dateTime,ym:pv:title,ym:pv:pageCharset,"
    "ym:pv:goalsID,ym:pv:URL,ym:pv:referer,ym:pv:UTMContent,"
    "ym:pv:UTMMedium,ym:pv:UTMSource,ym:pv:UTMTerm,ym:pv:operatingSystem,"
    "ym:pv:hasGCLID,ym:pv:GCLID,ym:pv:lastTrafficSource,ym:pv:lastSearchEngineRoot,"
    "ym:pv:lastSearchEngine,ym:pv:lastAdvEngine,ym:pv:lastSocialNetwork,"
    "ym:pv:lastSocialNetworkProfile,ym:pv:recommendationSystem,ym:pv:messenger,ym:pv:browser,"
    "ym:pv:browserMajorVersion,ym:pv:browserMinorVersion,ym:pv:browserCountry,ym:pv:browserEngine,"
    "ym:pv:browserEngineVersion1,ym:pv:browserEngineVersion2,ym:pv:browserEngineVersion3,"
    "ym:pv:browserEngineVersion4,ym:pv:browserLanguage,ym:pv:clientTimeZone,ym:pv:cookieEnabled,"
    "ym:pv:deviceCategory,ym:pv:javascriptEnabled,ym:pv:mobilePhone,ym:pv:mobilePhoneModel,"
    "ym:pv:operatingSystemRoot,ym:pv:physicalScreenHeight,ym:pv:physicalScreenWidth,"
    "ym:pv:screenColors,ym:pv:screenFormat,ym:pv:screenHeight,ym:pv:screenOrientation,"
    "ym:pv:screenOrientationName,ym:pv:screenWidth,ym:pv:windowClientHeight,ym:pv:windowClientWidth,"
    "ym:pv:ipAddress,ym:pv:regionCity,ym:pv:regionCountry,"
    "ym:pv:isPageView,ym:pv:iFrame,ym:pv:link,ym:pv:download,"
    "ym:pv:notBounce,ym:pv:artificial,ym:pv:promotionName,ym:pv:promotionCreative,ym:pv:promotionPosition,"
    "ym:pv:promotionCreativeSlot,ym:pv:promotionEventType,"
    "ym:pv:offlineCallTalkDuration,ym:pv:offlineCallHoldDuration,ym:pv:offlineCallMissed,"
    "ym:pv:offlineCallTag,ym:pv:offlineCallFirstTimeCaller,ym:pv:offlineCallURL,"
    "ym:pv:offlineUploadingID,ym:pv:params,"
    "ym:pv:httpError,ym:pv:networkType,ym:pv:shareService,ym:pv:shareURL,ym:pv:shareTitle,"
    "ym:pv:hasSBCLID,ym:pv:SBCLID"
)


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
    fields: str = Query(DEFAULT_FIELDS, description="Список полей через запятую"),
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
    fields: str = Query(DEFAULT_FIELDS, description="Список полей через запятую"),
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
    fields: str = Query(DEFAULT_FIELDS, description="Список полей через запятую"),
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
            return_as_zip=True,
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
