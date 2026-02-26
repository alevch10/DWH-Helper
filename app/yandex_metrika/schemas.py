from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Union, Any
from datetime import date, datetime
import json


class Counter(BaseModel):
    id: int
    name: str
    site: str
    # другие поля можно добавить при необходимости


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
    date: Optional[datetime] = None
    dateTime: Optional[datetime] = None
    title: Optional[str] = None
    pageCharset: Optional[str] = None
    goalsID: Optional[List[int]] = None
    URL: Optional[str] = None
    referer: Optional[str] = None
    UTMCampaign: Optional[str] = None
    UTMContent: Optional[str] = None
    UTMMedium: Optional[str] = None
    UTMSource: Optional[str] = None
    UTMTerm: Optional[str] = None
    openstatAd: Optional[str] = None
    openstatCampaign: Optional[str] = None
    openstatService: Optional[str] = None
    openstatSource: Optional[str] = None
    operatingSystem: Optional[str] = None
    from_: Optional[str] = Field(None, alias="from")
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
    regionCityID: Optional[int] = None
    regionCountryID: Optional[int] = None
    isPageView: Optional[int] = None
    isTurboPage: Optional[int] = None
    isTurboApp: Optional[int] = None
    iFrame: Optional[int] = None
    link: Optional[int] = None
    download: Optional[int] = None
    notBounce: Optional[int] = None
    artificial: Optional[int] = None
    purchaseID: Optional[List[str]] = None
    purchaseRevenue: Optional[List[float]] = None
    purchaseTax: Optional[List[str]] = None
    purchaseShipping: Optional[List[str]] = None
    purchaseCoupon: Optional[List[str]] = None
    purchaseCurrency: Optional[List[str]] = None
    purchaseProductQuantity: Optional[List[int]] = None
    productID: Optional[List[str]] = None
    productList: Optional[List[str]] = None
    productBrand: Optional[List[str]] = None
    productCategory: Optional[List[str]] = None
    productCategoryLevel1: Optional[List[str]] = None
    productCategoryLevel2: Optional[List[str]] = None
    productCategoryLevel3: Optional[List[str]] = None
    productCategoryLevel4: Optional[List[str]] = None
    productCategoryLevel5: Optional[List[str]] = None
    productVariant: Optional[List[str]] = None
    productPosition: Optional[List[int]] = None
    productPrice: Optional[List[int]] = None
    productCurrency: Optional[List[str]] = None
    productCoupon: Optional[List[str]] = None
    productQuantity: Optional[List[int]] = None
    productEventType: Optional[List[int]] = None
    productDiscount: Optional[List[str]] = None
    productName: Optional[List[str]] = None
    promotionID: Optional[List[str]] = None
    promotionName: Optional[List[str]] = None
    promotionCreative: Optional[List[str]] = None
    promotionPosition: Optional[List[str]] = None
    promotionCreativeSlot: Optional[List[str]] = None
    promotionEventType: Optional[List[int]] = None
    ecommerce: Optional[str] = None
    offlineCallTalkDuration: Optional[int] = None
    offlineCallHoldDuration: Optional[int] = None
    offlineCallMissed: Optional[int] = None
    offlineCallTag: Optional[str] = None
    offlineCallFirstTimeCaller: Optional[int] = None
    offlineCallURL: Optional[str] = None
    offlineUploadingID: Optional[str] = None
    params: Optional[str] = None
    parsedParamsKey1: Optional[List[str]] = None
    parsedParamsKey2: Optional[List[str]] = None
    parsedParamsKey3: Optional[List[str]] = None
    parsedParamsKey4: Optional[List[str]] = None
    parsedParamsKey5: Optional[List[str]] = None
    parsedParamsKey6: Optional[List[str]] = None
    parsedParamsKey7: Optional[List[str]] = None
    parsedParamsKey8: Optional[List[str]] = None
    parsedParamsKey9: Optional[List[str]] = None
    parsedParamsKey10: Optional[List[str]] = None
    httpError: Optional[str] = None
    networkType: Optional[str] = None
    shareService: Optional[str] = None
    shareURL: Optional[str] = None
    shareTitle: Optional[str] = None
    hasSBCLID: Optional[int] = None
    SBCLID: Optional[str] = None

    @field_validator(
        "goalsID",
        "purchaseID",
        "purchaseRevenue",
        "purchaseTax",
        "purchaseShipping",
        "purchaseCoupon",
        "purchaseCurrency",
        "purchaseProductQuantity",
        "productID",
        "productList",
        "productBrand",
        "productCategory",
        "productCategoryLevel1",
        "productCategoryLevel2",
        "productCategoryLevel3",
        "productCategoryLevel4",
        "productCategoryLevel5",
        "productVariant",
        "productPosition",
        "productPrice",
        "productCurrency",
        "productCoupon",
        "productQuantity",
        "productEventType",
        "productDiscount",
        "productName",
        "promotionID",
        "promotionName",
        "promotionCreative",
        "promotionPosition",
        "promotionCreativeSlot",
        "promotionEventType",
        "parsedParamsKey1",
        "parsedParamsKey2",
        "parsedParamsKey3",
        "parsedParamsKey4",
        "parsedParamsKey5",
        "parsedParamsKey6",
        "parsedParamsKey7",
        "parsedParamsKey8",
        "parsedParamsKey9",
        "parsedParamsKey10",
        mode="before",
    )
    def parse_json_array(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, str):
            # Пытаемся распарсить JSON-массив
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # Если не получилось, возможно это список через запятую? оставим как есть
                return v
        return v

    class Config:
        populate_by_name = True
