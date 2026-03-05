from typing import Any, Dict, List


from app.yandex_metrika.services import get_metrika_hits
from app.yandex_metrika.schemas import (
    MetrikaHitRow,
    MetricaAdData,
    MetrikaSuccessfulEntries,
    BookingVisit,
    BookingTransition,
    UserPath,
    CallData,
    PageTransition,
    ProcessDayResponse,
)
from app.db.repository import DBRepository
from app.etl.services import remove_query_params, is_url_target
from app.config.settings import settings

repository = DBRepository()


def get_earliest_visit(ym_raw_data: list[MetrikaHitRow]) -> list[MetricaAdData]:
    """Группирует по client_id и сохраняет самый первый вход на сайт и маркетинговые данные"""
    best_by_client: Dict[str, MetrikaHitRow] = {}
    for row in ym_raw_data:
        if row.client_id is None or not row.is_page_view:
            continue
        if (row.client_id not in best_by_client) or (
            row.date_time < best_by_client[row.client_id].date_time
        ):
            best_by_client[row.client_id] = row

    ad_data_list = []
    for row in best_by_client.values():
        data = row.model_dump()
        if data.get("url"):
            data["url"] = remove_query_params(
                data["url"], settings.get_query_params_to_remove()
            )
        ad_data_list.append(MetricaAdData(**data))
    return ad_data_list


def get_successful_entries(
    ym_raw_data: list[MetrikaHitRow],
) -> list[MetrikaSuccessfulEntries]:
    """Оставляет только записи с целевыми URL и группирует по page_view_id (самая ранняя запись)."""
    successful_hits: Dict[str, MetrikaHitRow] = {}
    for row in ym_raw_data:
        if row.url is None:
            continue
        key = row.page_view_id
        if key is None:
            continue
        if not is_url_target(
            row.url,
            target_netloc=settings.yandexmetrica.get_target_netloc_list(),
            target_path=settings.yandexmetrica.get_target_path_list(),
        ):
            continue
        if (key not in successful_hits) or (
            row.date_time < successful_hits[key].date_time
        ):
            successful_hits[key] = row

    result = []
    for hit in successful_hits.values():
        data = hit.model_dump()
        result.append(MetrikaSuccessfulEntries(**data))
    return result


def get_booking_visits(
    ym_raw_data: List[MetrikaHitRow], successful_entries: List[MetrikaSuccessfulEntries]
) -> List[BookingVisit]:
    """Формирует список визитов на booking с минимальным набором полей."""
    successful_visit_ids = {e.visit_id for e in successful_entries if e.visit_id}
    first_hit_by_visit = {}
    for row in ym_raw_data:
        if (
            not row.visit_id
            or not row.url
            or settings.yandexmetrica.booking_domain not in row.url
        ):
            continue
        if row.visit_id not in first_hit_by_visit or (
            row.date_time and first_hit_by_visit[row.visit_id].date_time > row.date_time
        ):
            first_hit_by_visit[row.visit_id] = row

    result = []
    for visit_id, hit in first_hit_by_visit.items():
        if not hit.client_id or not hit.date_time:
            continue
        data = hit.model_dump()
        if data.get("referer"):
            data["referer"] = remove_query_params(
                data["referer"], settings.get_query_params_to_remove()
            )
        data["visit_start_time"] = hit.date_time
        data["had_successful_entry"] = visit_id in successful_visit_ids
        result.append(BookingVisit(**data))
    return result


def get_booking_transitions(
    ym_raw_data: List[MetrikaHitRow],
) -> List[BookingTransition]:
    """Формирует список переходов на booking (с сайта или прямые)."""
    page_views = [
        r for r in ym_raw_data if r.client_id and r.date_time and r.is_page_view
    ]
    page_views.sort(key=lambda x: (x.client_id, x.date_time))

    transitions: list = []
    prev_hit_by_client: dict = {}
    for hit in page_views:
        if hit.url and settings.yandexmetrica.booking_domain in hit.url:
            prev_hit = prev_hit_by_client.get(hit.client_id)
            if prev_hit is None or (
                prev_hit.url
                and settings.yandexmetrica.booking_domain not in prev_hit.url
            ):
                booking_url_clean = remove_query_params(
                    hit.url, settings.get_query_params_to_remove()
                )
                prev_url_clean = (
                    remove_query_params(
                        prev_hit.url, settings.get_query_params_to_remove()
                    )
                    if prev_hit
                    else None
                )
                trans_type = "direct_booking" if prev_hit is None else "from_site"

                data: Dict[str, Any] = hit.model_dump()
                data["booking_url"] = booking_url_clean
                data["prev_url"] = prev_url_clean
                data["transition_type"] = trans_type
                data["hit_time"] = hit.date_time
                data.pop("url", None)
                data.pop("date_time", None)
                transitions.append(BookingTransition(**data))
        prev_hit_by_client[hit.client_id] = hit
    return transitions


def get_user_paths(
    ym_raw_data: List[MetrikaHitRow], successful_entries: List[MetrikaSuccessfulEntries]
) -> List[UserPath]:
    """Строит последовательность визитов для каждого клиента, помечая, был ли в этом или последующих визитах факт записи."""
    # Собираем первый хит для каждого visit_id
    visits_first_hit: dict = {}
    for row in ym_raw_data:
        if row.visit_id is None or row.client_id is None:
            continue
        if row.visit_id not in visits_first_hit or (
            row.date_time and visits_first_hit[row.visit_id].date_time > row.date_time
        ):
            visits_first_hit[row.visit_id] = row

    # Группируем по client_id
    from collections import defaultdict

    visits_by_client = defaultdict(list)
    for row in visits_first_hit.values():
        visits_by_client[row.client_id].append(row)

    # Время первой записи для клиента
    first_entry_time_by_client = {}
    for e in successful_entries:
        if e.client_id and e.date_time:
            if (
                e.client_id not in first_entry_time_by_client
                or e.date_time < first_entry_time_by_client[e.client_id]
            ):
                first_entry_time_by_client[e.client_id] = e.date_time

    paths: list = []
    for client_id, hits in visits_by_client.items():
        hits.sort(key=lambda x: x.date_time)
        for idx, hit in enumerate(hits, start=1):
            had_entry = False
            entry_time = None
            if client_id in first_entry_time_by_client:
                if hit.date_time <= first_entry_time_by_client[client_id]:
                    had_entry = True
                    entry_time = first_entry_time_by_client[client_id]
            data = hit.model_dump()
            data["visit_number"] = idx
            data["had_successful_entry"] = had_entry
            data["entry_time"] = entry_time
            data["visit_start_time"] = hit.date_time
            paths.append(UserPath(**data))
    return paths


def get_call_data(ym_raw_data: List[MetrikaHitRow]) -> List[CallData]:
    """
    Извлекает данные о звонках из сырых хитов.
    Считаем, что каждый хит с заполненными offline-полями соответствует одному звонку.
    """
    calls: list = []
    for row in ym_raw_data:
        if any(
            [
                row.offline_call_talk_duration is not None,
                row.offline_call_hold_duration is not None,
                row.offline_call_missed is not None,
                row.offline_call_tag is not None,
                row.offline_call_first_time_caller is not None,
                row.offline_call_url is not None,
                row.offline_uploading_id is not None,
            ]
        ):
            data: Dict[str, Any] = row.model_dump()
            calls.append(CallData(**data))
    return calls


def get_page_transitions(ym_raw_data: List[MetrikaHitRow]) -> List[PageTransition]:
    # Только просмотры страниц с URL
    page_views = [
        r
        for r in ym_raw_data
        if r.is_page_view and r.url and r.visit_id and r.client_id and r.date_time
    ]
    # Сортируем по визиту и времени
    page_views.sort(key=lambda x: (x.visit_id, x.date_time))

    transitions = []
    current_visit_id = None
    visit_pages = []  # список очищенных URL для текущего визита

    for hit in page_views:
        if hit.visit_id != current_visit_id:
            # Обрабатываем предыдущий визит
            if visit_pages:
                transitions.extend(
                    _process_visit_pages(current_visit_id, visit_pages, first_hit)
                )
            # Начинаем новый визит
            current_visit_id = hit.visit_id
            first_hit = hit  # запоминаем первый хит визита для client_id и даты
            visit_pages = []

        # Очищаем URL от параметров
        clean_url = remove_query_params(hit.url, settings.get_query_params_to_remove())
        # Убираем последовательные дубликаты: добавляем только если отличается от последнего
        if not visit_pages or clean_url != visit_pages[-1]:
            visit_pages.append(clean_url)

    # Обработка последнего визита
    if visit_pages:
        transitions.extend(
            _process_visit_pages(current_visit_id, visit_pages, first_hit)
        )

    return transitions


def _process_visit_pages(
    visit_id: int, pages: List[str], first_hit: MetrikaHitRow
) -> List[PageTransition]:
    """Преобразует список страниц визита в переходы, включая вход."""
    if len(pages) == 0:
        return []
    result = []
    client_id = first_hit.client_id
    visit_date = first_hit.date_time.date()
    result.append(
        PageTransition(
            visit_id=visit_id,
            client_id=client_id,
            transition_date=visit_date,
            source="(start)",
            target=pages[0],
            sequence_num=1,
        )
    )
    for i in range(len(pages) - 1):
        result.append(
            PageTransition(
                visit_id=visit_id,
                client_id=client_id,
                transition_date=visit_date,
                source=pages[i],
                target=pages[i + 1],
                sequence_num=i + 2,
            )
        )
    return result


async def get_ad_efficiency(
    token: str, counter_id: int, date: str, source: str, fields: list
) -> ProcessDayResponse:
    statistics = {}

    # Получить сырые данные
    ym_raw_data: list[MetrikaHitRow] = await get_metrika_hits(
        token=token,
        counter_id=counter_id,
        date1=date,
        date2=date,
        source=source,
        fields=fields,
    )
    repository.insert_batch(
        table="yandex_metrika.ym_raw_data",
        rows=[hit.model_dump(exclude_none=False) for hit in ym_raw_data],
    )
    statistics["ym_raw_data"] = len(ym_raw_data)

    # Первые визиты
    ym_ad_data_list: List[MetricaAdData] = get_earliest_visit(ym_raw_data)
    repository.insert_batch(
        table="yandex_metrika.ym_ad_data",
        rows=[ad.model_dump(exclude_none=False) for ad in ym_ad_data_list],
    )
    statistics["ym_ad_data"] = len(ym_ad_data_list)

    # Успешные записи
    ym_successful_entries: List[MetrikaSuccessfulEntries] = get_successful_entries(
        ym_raw_data
    )
    repository.insert_batch(
        table="yandex_metrika.ym_successful_entries",
        rows=[e.model_dump(exclude_none=False) for e in ym_successful_entries],
    )
    statistics["ym_successful_entries"] = len(ym_successful_entries)

    # Визиты на booking
    ym_booking_visits: List[BookingVisit] = get_booking_visits(
        ym_raw_data, ym_successful_entries
    )
    repository.insert_batch(
        table="yandex_metrika.ym_booking_visits",
        rows=[bv.model_dump(exclude_none=False) for bv in ym_booking_visits],
    )
    statistics["ym_booking_visits"] = len(ym_booking_visits)

    # Переходы на booking
    ym_booking_transitions: List[BookingTransition] = get_booking_transitions(
        ym_raw_data
    )
    repository.insert_batch(
        table="yandex_metrika.ym_booking_transitions",
        rows=[bt.model_dump(exclude_none=False) for bt in ym_booking_transitions],
    )
    statistics["ym_booking_transitions"] = len(ym_booking_transitions)

    # Пути пользователей
    ym_user_paths: List[UserPath] = get_user_paths(ym_raw_data, ym_successful_entries)
    repository.insert_batch(
        table="yandex_metrika.ym_user_paths",
        rows=[up.model_dump(exclude_none=False) for up in ym_user_paths],
    )
    statistics["ym_user_paths"] = len(ym_user_paths)

    # Звонки
    ym_call_data: List[CallData] = get_call_data(ym_raw_data)
    if ym_call_data:
        repository.insert_batch(
            table="yandex_metrika.ym_call_data",
            rows=[call.model_dump(exclude_none=False) for call in ym_call_data],
        )
    statistics["ym_call_data"] = len(ym_call_data)

    # Переходы для санкей-диаграммы
    ym_page_transitions: List[PageTransition] = get_page_transitions(ym_raw_data)
    if ym_page_transitions:
        repository.insert_batch(
            table="yandex_metrika.ym_page_transitions",
            rows=[pt.model_dump(exclude_none=False) for pt in ym_page_transitions],
        )
    statistics["ym_page_transitions"] = len(ym_page_transitions)

    return statistics
