import ast
import json

from pydantic import BaseModel, field_validator, BeforeValidator
from typing import List, Optional, Annotated
from datetime import date, datetime


def normalize_json_params(v: Optional[str]) -> Optional[str]:
    """Нормализует JSON-строку: убирает внешние кавычки и делает компактной."""
    if not v or not isinstance(v, str):
        return None
    v = v.strip()
    # Убираем внешние кавычки, если строка ими обрамлена
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    try:
        obj = json.loads(v)
        # Возвращаем компактный JSON без пробелов
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except json.JSONDecodeError:
        return v


# Определяем аннотированный тип
NormalizedJsonStr = Annotated[Optional[str], BeforeValidator(normalize_json_params)]


class Counter(BaseModel):
    id: int
    name: str
    site: str
    # при необходимости добавьте другие поля из ответа


class CountersResponse(BaseModel):
    rows: int
    counters: List[Counter]


class LogRequestPart(BaseModel):
    part_number: int
    size: int


class LogRequest(BaseModel):
    request_id: int
    counter_id: int
    source: str
    date1: date
    date2: date
    fields: List[str]
    status: str
    size: int
    parts: Optional[List[LogRequestPart]] = None
    attribution: str


class LogRequestsResponse(BaseModel):
    requests: List[LogRequest]


class LogRequestEvaluation(BaseModel):
    possible: bool
    expected_size: Optional[int] = None
    max_possible_day_quantity: Optional[int] = None
    log_request_sum_max_size: Optional[int] = None
    log_request_sum_size: Optional[int] = None


class MetrikaHitRow(BaseModel):
    watchID: Optional[int] = None
    pageViewID: Optional[int] = None
    visitID: Optional[int] = None
    counterID: Optional[int] = None
    clientID: Optional[int] = None
    counterUserIDHash: Optional[int] = None
    dateTime: Optional[datetime] = None
    title: Optional[str] = None
    goalsID: Optional[List[int]] = None
    URL: Optional[str] = None
    referer: Optional[str] = None
    UTMCampaign: Optional[str] = None
    UTMContent: Optional[str] = None
    UTMMedium: Optional[str] = None
    UTMSource: Optional[str] = None
    UTMTerm: Optional[str] = None
    operatingSystem: Optional[str] = None
    hasGCLID: Optional[int] = None
    GCLID: Optional[str] = None
    lastTrafficSource: Optional[str] = None
    lastSearchEngineRoot: Optional[str] = None
    lastSearchEngine: Optional[str] = None
    lastAdvEngine: Optional[str] = None
    lastSocialNetwork: Optional[str] = None
    lastSocialNetworkProfile: Optional[str] = None
    recommendationSystem: Optional[str] = None
    messenger: Optional[str] = None
    browser: Optional[str] = None
    browserMajorVersion: Optional[int] = None
    browserMinorVersion: Optional[int] = None
    browserCountry: Optional[str] = None
    browserEngine: Optional[str] = None
    browserEngineVersion1: Optional[int] = None
    browserEngineVersion2: Optional[int] = None
    browserEngineVersion3: Optional[int] = None
    browserEngineVersion4: Optional[int] = None
    browserLanguage: Optional[str] = None
    clientTimeZone: Optional[int] = None
    cookieEnabled: Optional[int] = None
    deviceCategory: Optional[str] = None
    javascriptEnabled: Optional[int] = None
    mobilePhone: Optional[str] = None
    mobilePhoneModel: Optional[str] = None
    operatingSystemRoot: Optional[str] = None
    physicalScreenHeight: Optional[int] = None
    physicalScreenWidth: Optional[int] = None
    screenColors: Optional[int] = None
    screenFormat: Optional[str] = None
    screenHeight: Optional[int] = None
    screenOrientation: Optional[int] = None
    screenOrientationName: Optional[str] = None
    screenWidth: Optional[int] = None
    windowClientHeight: Optional[int] = None
    windowClientWidth: Optional[int] = None
    ipAddress: Optional[str] = None
    regionCity: Optional[str] = None
    regionCountry: Optional[str] = None
    isPageView: Optional[int] = None
    isTurboApp: Optional[int] = None
    iFrame: Optional[int] = None
    link: Optional[int] = None
    download: Optional[int] = None
    notBounce: Optional[int] = None
    artificial: Optional[int] = None
    promotionID: Optional[List[str]] = None
    promotionName: Optional[List[str]] = None
    promotionCreative: Optional[List[str]] = None
    promotionPosition: Optional[List[str]] = None
    promotionCreativeSlot: Optional[List[str]] = None
    promotionEventType: Optional[List[int]] = None
    offlineCallTalkDuration: Optional[int] = None
    offlineCallHoldDuration: Optional[int] = None
    offlineCallMissed: Optional[int] = None
    offlineCallTag: Optional[str] = None
    offlineCallFirstTimeCaller: Optional[int] = None
    offlineCallURL: Optional[str] = None
    offlineUploadingID: Optional[str] = None
    params: NormalizedJsonStr = None
    httpError: Optional[str] = None
    networkType: Optional[str] = None
    shareService: Optional[str] = None
    shareURL: Optional[str] = None
    shareTitle: Optional[str] = None
    hasSBCLID: Optional[int] = None
    SBCLID: Optional[str] = None

    @field_validator(
        "goalsID",
        "promotionName",
        "promotionCreative",
        "promotionPosition",
        "promotionCreativeSlot",
        "promotionEventType",
        mode="before",
    )
    @classmethod
    def parse_string_to_list(cls, v):
        """Преобразует строку, представляющую список, в реальный список Python."""
        if v is None or v == "":
            return None
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            try:
                return ast.literal_eval(v)
            except (SyntaxError, ValueError):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass
        return v

    class Config:
        populate_by_name = True
