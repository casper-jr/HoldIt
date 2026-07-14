"""DART client — fetch, return the response body verbatim plus http_status.

The response body is stored untouched, including on failures: a DART
``status != '000'`` is data about pipeline health, not something to swallow. This
client parses nothing and derives nothing — Silver's stg_dart__* models parse the
JSON. KR wiring (corp_code resolution, the ticker universe, endpoint policy) lands
in Step 6; this is the minimal transport per the spec.
"""
import requests

BASE_URL = "https://opendart.fss.or.kr/api"

# Endpoints map 1:1 to Bronze tables raw_dart_{endpoint}.
ENDPOINT_PATHS = {
    "fnltt": "fnlttSinglAcnt.json",
    "alot_matter": "alotMatter.json",
    "stock_totqy": "stockTotqySttus.json",
    "disclosure": "list.json",
}


def fetch(endpoint: str, params: dict, *, api_key: str, timeout: int = 30):
    """GET one DART endpoint. Returns (response_text, http_status).

    ``params`` is passed through as given (minus the auth key, which is added
    here); the caller records it as request_params. On any transport error the
    body is None and http_status is None — a null payload, never a fabricated one.
    """
    path = ENDPOINT_PATHS[endpoint]
    query = {"crtfc_key": api_key, **params}
    try:
        response = requests.get(f"{BASE_URL}/{path}", params=query, timeout=timeout)
    except requests.RequestException:
        return None, None
    return response.text, response.status_code
