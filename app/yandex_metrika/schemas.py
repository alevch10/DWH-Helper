import ast
import json

from pydantic import (
    BaseModel,
    model_validator,
    ConfigDict,
    BeforeValidator,
    Field,
    field_validator,
)
from typing import List, Optional, Annotated, Any, Dict
from datetime import date, datetime

# ------------------------------------------------------------------------
# Нормализаторы
# ------------------------------------------------------------------------


def normalize_json_params(v: Optional[str]) -> Optional[str]:
    """Нормализует JSON-строку: убирает внешние кавычки и делает компактной."""
    if not v or not isinstance(v, str):
        return None
    v = v.strip()
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    try:
        obj = json.loads(v)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except json.JSONDecodeError:
        return v


NormalizedJsonStr = Annotated[Optional[str], BeforeValidator(normalize_json_params)]


def coerce_to_optional_int(v: Any) -> int | None:
    """Преобразует значение в int или None, если пусто/некорректно."""
    if v in (None, "", " ", "-", "-1", "undefined", "null", "None"):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        cleaned = v.strip()
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


OptionalInt = Annotated[Optional[int], BeforeValidator(coerce_to_optional_int)]


def parse_list_like(v: Any) -> list | None:
    """Парсит строки вида '[1,2,3]', '[]', json-списки и т.п."""
    if v is None or v == "":
        return None
    if isinstance(v, list):
        return v
    if not isinstance(v, str):
        return None

    v = v.strip()
    if v in ("[]", "[ ]", ""):
        return []

    try:
        parsed = ast.literal_eval(v)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    try:
        parsed = json.loads(v)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass

    return [v]


ListLike = Annotated[list | None, BeforeValidator(parse_list_like)]


# ------------------------------------------------------------------------
# Основные схемы
# ------------------------------------------------------------------------


class Counter(BaseModel):
    id: int
    name: str
    site: str


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
    watch_id: OptionalInt = Field(None, alias="watchID")
    page_view_id: OptionalInt = Field(
        None, ge=-2147483648, le=2147483647, alias="pageViewID"
    )
    visit_id: OptionalInt = Field(None, alias="visitID")
    counter_id: OptionalInt = Field(
        None, ge=-2147483648, le=2147483647, alias="counterID"
    )
    client_id: int = Field(None, alias="clientID")
    counter_user_id_hash: OptionalInt = Field(None, alias="counterUserIDHash")

    date_time: Optional[datetime] = Field(None, alias="dateTime")
    title: Optional[str] = Field(None, alias="title", max_length=800)

    goals_id: ListLike = Field(None, alias="goalsID")
    url: Optional[str] = Field(None, alias="URL", max_length=2048)
    referer: Optional[str] = Field(None, alias="referer", max_length=2278)

    utm_campaign: Optional[str] = Field(None, alias="UTMCampaign")
    utm_content: Optional[str] = Field(None, alias="UTMContent", max_length=512)
    utm_medium: Optional[str] = Field(None, alias="UTMMedium", max_length=14)
    utm_source: Optional[str] = Field(None, alias="UTMSource", max_length=50)
    utm_term: Optional[str] = Field(None, alias="UTMTerm", max_length=512)

    operating_system: Optional[str] = Field(
        None, alias="operatingSystem", max_length=22
    )
    has_gclid: Optional[bool] = Field(None, alias="hasGCLID")
    gclid: Optional[str] = Field(None, alias="GCLID")

    last_traffic_source: Optional[str] = Field(
        None, alias="lastTrafficSource", max_length=9
    )
    last_search_engine_root: Optional[str] = Field(
        None, alias="lastSearchEngineRoot", max_length=10
    )
    last_search_engine: Optional[str] = Field(
        None, alias="lastSearchEngine", max_length=17
    )
    last_adv_engine: Optional[str] = Field(None, alias="lastAdvEngine", max_length=14)
    last_social_network: Optional[str] = Field(
        None, alias="lastSocialNetwork", max_length=13
    )
    last_social_network_profile: Optional[str] = Field(
        None, alias="lastSocialNetworkProfile", max_length=50
    )
    recommendation_system: Optional[str] = Field(
        None, alias="recommendationSystem", max_length=10
    )
    messenger: Optional[str] = Field(None, alias="messenger", max_length=8)

    browser: Optional[str] = Field(None, alias="browser", max_length=17)
    browser_major_version: OptionalInt = Field(
        None, ge=-2147483648, le=2147483647, alias="browserMajorVersion"
    )
    browser_minor_version: OptionalInt = Field(
        None, ge=-2147483648, le=2147483647, alias="browserMinorVersion"
    )
    browser_country: Optional[str] = Field(None, alias="browserCountry", max_length=2)
    browser_engine: Optional[str] = Field(None, alias="browserEngine", max_length=50)
    browser_engine_version1: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="browserEngineVersion1"
    )
    browser_engine_version2: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="browserEngineVersion2"
    )
    browser_engine_version3: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="browserEngineVersion3"
    )
    browser_engine_version4: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="browserEngineVersion4"
    )
    browser_language: Optional[str] = Field(None, alias="browserLanguage", max_length=4)

    cookie_enabled: Optional[bool] = Field(None, alias="cookieEnabled")
    device_category: Optional[str] = Field(None, alias="deviceCategory", max_length=1)
    javascript_enabled: Optional[bool] = Field(None, alias="javascriptEnabled")

    mobile_phone: Optional[str] = Field(None, alias="mobilePhone", max_length=14)
    mobile_phone_model: Optional[str] = Field(
        None, alias="mobilePhoneModel", max_length=50
    )
    operating_system_root: Optional[str] = Field(
        None, alias="operatingSystemRoot", max_length=20
    )

    physical_screen_height: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="physicalScreenHeight"
    )
    physical_screen_width: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="physicalScreenWidth"
    )
    screen_colors: OptionalInt = Field(None, ge=-32768, le=32767, alias="screenColors")
    screen_format: Optional[str] = Field(None, alias="screenFormat", max_length=9)
    screen_height: OptionalInt = Field(None, ge=-32768, le=32767, alias="screenHeight")
    screen_orientation: OptionalInt = Field(None, alias="screenOrientation")
    screen_orientation_name: Optional[str] = Field(
        None, alias="screenOrientationName", max_length=9
    )
    screen_width: OptionalInt = Field(None, ge=-32768, le=32767, alias="screenWidth")

    window_client_height: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="windowClientHeight"
    )
    window_client_width: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="windowClientWidth"
    )

    ip_address: Optional[str] = Field(None, alias="ipAddress", max_length=24)
    region_city: Optional[str] = Field(None, alias="regionCity", max_length=26)
    region_country: Optional[str] = Field(None, alias="regionCountry", max_length=63)

    is_page_view: Optional[bool] = Field(None, alias="isPageView")
    is_turbo_app: Optional[bool] = Field(None, alias="isTurboApp")
    iframe: Optional[bool] = Field(None, alias="iFrame")
    link: Optional[bool] = Field(None, alias="link")
    download: Optional[bool] = Field(None, alias="download")
    not_bounce: Optional[bool] = Field(None, alias="notBounce")
    artificial: Optional[bool] = Field(None, alias="artificial")

    offline_call_talk_duration: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="offlineCallTalkDuration"
    )
    offline_call_hold_duration: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="offlineCallHoldDuration"
    )
    offline_call_missed: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="offlineCallMissed"
    )
    offline_call_tag: Optional[str] = Field(
        None, alias="offlineCallTag", max_length=1000
    )
    offline_call_first_time_caller: OptionalInt = Field(
        None, ge=-32768, le=32767, alias="offlineCallFirstTimeCaller"
    )
    offline_call_url: Optional[str] = Field(
        None, alias="offlineCallURL", max_length=2048
    )
    offline_uploading_id: Optional[str] = Field(
        None, alias="offlineUploadingID", max_length=10
    )

    params: NormalizedJsonStr = Field(None, alias="params", max_length=2048)

    http_error: OptionalInt = Field(None, ge=-32768, le=32767, alias="httpError")
    network_type: Optional[str] = Field(None, alias="networkType", max_length=8)
    share_service: Optional[str] = Field(None, alias="shareService")
    share_url: Optional[str] = Field(None, alias="shareURL", max_length=2048)
    share_title: Optional[str] = Field(None, alias="shareTitle")

    has_sbclid: Optional[bool] = Field(None, alias="hasSBCLID")
    sbclid: Optional[str] = Field(None, alias="SBCLID")

    @field_validator(
        "watch_id", "client_id", "counter_user_id_hash", "visit_id", mode="after"
    )
    @classmethod
    def check_numeric_20(cls, v):
        if v is None:
            return v
        if not isinstance(v, int):
            raise ValueError(f"Value must be integer, got {type(v).__name__}")
        # Проверяем, что число помещается в 20 знаков (без учёта знака)
        if abs(v) >= 10**20:
            raise ValueError(f"Value exceeds 20 digits: {v}")
        return v

    @model_validator(mode="before")
    @classmethod
    def empty_string_to_none(cls, values: dict) -> dict:
        """
        Заменяет все пустые строки "" на None для всех полей
        """
        for key, value in values.items():
            if isinstance(value, str) and value.strip() == "":
                values[key] = None
        return values

    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=None,
        arbitrary_types_allowed=True,
    )


class MetricaAdData(BaseModel):
    """Данные о первом визите клиента (first-click атрибуция)."""

    watch_id: Optional[int]
    page_view_id: Optional[int]
    visit_id: Optional[int]
    client_id: Optional[int]
    date_time: Optional[datetime]
    url: Optional[str]
    referer: Optional[str]

    utm_campaign: Optional[str]
    utm_content: Optional[str]
    utm_medium: Optional[str]
    utm_source: Optional[str]
    utm_term: Optional[str]

    last_traffic_source: Optional[str]
    last_search_engine_root: Optional[str]
    last_search_engine: Optional[str]
    last_adv_engine: Optional[str]
    last_social_network: Optional[str]
    last_social_network_profile: Optional[str]
    recommendation_system: Optional[str]
    messenger: Optional[str]


class CallData(BaseModel):
    """Данные о звонках."""

    visit_id: Optional[int]
    client_id: Optional[int]
    date_time: Optional[datetime]
    offline_call_talk_duration: Optional[int]
    offline_call_hold_duration: Optional[int]
    offline_call_missed: Optional[int]
    offline_call_tag: Optional[str]
    offline_call_first_time_caller: Optional[int]
    offline_call_url: Optional[str]
    offline_uploading_id: Optional[str]


class MetrikaSuccessfulEntries(BaseModel):
    """События успешной записи на приём."""

    client_id: int = Field(..., description="ID клиента")
    visit_id: Optional[int] = Field(None, description="ID визита")
    date_time: datetime = Field(..., description="Время записи")


class BookingVisit(BaseModel):
    """Визит, в котором был хотя бы один хит на домене booking."""

    visit_id: int = Field(..., description="ID визита")
    client_id: int = Field(..., description="ID клиента")
    visit_start_time: datetime = Field(..., description="Время начала визита")
    utm_source: Optional[str] = Field(None)
    utm_medium: Optional[str] = Field(None)
    utm_campaign: Optional[str] = Field(None)
    utm_term: Optional[str] = Field(None)
    utm_content: Optional[str] = Field(None)
    referer: Optional[str] = Field(None)
    last_traffic_source: Optional[str] = Field(None)
    device_category: Optional[str] = Field(None)
    region_city: Optional[str] = Field(None)
    had_successful_entry: bool = Field(False)


class BookingTransition(BaseModel):
    """Переход на booking с сайта или прямой вход."""

    client_id: int = Field(..., description="ID клиента")
    hit_time: datetime = Field(..., description="Время перехода")
    booking_url: str = Field(...)
    prev_url: Optional[str] = Field(None)
    transition_type: str = Field(..., description="from_site или direct_booking")
    last_traffic_source: Optional[str] = Field(None)


class UserPath(BaseModel):
    """Путь пользователя: последовательность визитов."""

    client_id: int = Field(..., description="ID клиента")
    visit_id: int = Field(..., description="ID визита")
    visit_start_time: datetime = Field(..., description="Время начала визита")
    utm_source: Optional[str] = Field(None)
    utm_medium: Optional[str] = Field(None)
    utm_campaign: Optional[str] = Field(None)
    visit_number: int = Field(..., description="Порядковый номер визита")
    had_successful_entry: bool = Field(False)
    entry_time: Optional[datetime] = Field(None, description="Время первой записи")


class PageTransition(BaseModel):
    """переходы между страницами внутри одного визита"""

    visit_id: int = Field(..., description="ID визита")
    client_id: int = Field(..., description="ID клиента")
    transition_date: date = Field(..., description="Дата перехода (день)")
    source: str = Field(..., description="Исходная страница (или '(start)')")
    target: str = Field(..., description="Целевая страница")
    sequence_num: int = Field(
        ...,
        description="Порядковый номер шага в визите (начиная с 1 для первого перехода)",
    )


class ProcessDayRequest(BaseModel):
    counter_id: int = Field(..., description="Номер счётчика")
    date: str = Field(
        ..., description="Дата в формате YYYY-MM-DD", pattern=r"^\d{4}-\d{2}-\d{2}$"
    )
    source: str = Field("hits", description="Тип данных: 'hits' или 'visits'")
    fields: Optional[List[str]] = Field(
        None,
        description="Список полей для запроса (если не указан, берётся из настроек)",
    )


class ProcessDayResponse(BaseModel):
    status: str = Field(..., description="Статус обработки: 'success' или 'error'")
    statistics: Dict[str, int] = Field(
        ..., description="Словарь с количеством записей в каждой витрине"
    )
    message: Optional[str] = Field(
        None, description="Дополнительное сообщение (например, об ошибке)"
    )
