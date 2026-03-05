from urllib.parse import ParseResult, urlparse, urlunparse, parse_qs, urlencode, unquote
from typing import Optional


def remove_query_params(url: str, params_to_remove: list):
    """Удаляет указанные query-параметры из URL."""
    parsed_url: ParseResult = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    for param in params_to_remove:
        if param in query_params:
            del query_params[param]

    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse(parsed_url._replace(query=new_query))
    return unquote(new_url)


def is_url_target(
    url: str,
    target_scheme: Optional[list] = None,
    target_netloc: Optional[list] = None,
    target_path: Optional[list] = None,
    target_params: Optional[list] = None,
    target_query: Optional[list] = None,
    target_fragment: Optional[list] = None,
) -> bool:
    """Проверяет, соответствуют ли отдельные компоненты URL заданным спискам допустимых значений"""
    parsed_url: ParseResult = urlparse(url)
    components = [
        ("scheme", target_scheme),
        ("netloc", target_netloc),
        ("path", target_path),
        ("params", target_params),
        ("query", target_query),
        ("fragment", target_fragment),
    ]

    for comp_name, allowed in components:
        if allowed is None:
            continue
        if isinstance(allowed, str):
            allowed = [allowed]
        actual = getattr(parsed_url, comp_name)
        if actual not in allowed:
            return False

    return True
