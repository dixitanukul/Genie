import time
import requests
from typing import Dict, Tuple, Any, List
from settings import (
    DATABRICKS_TOKEN, CONVERSATION_URL, MESSAGE_URL_TMPL,
    EXECUTE_URL_TMPL, RESULT_URL_TMPL,
    POLL_INTERVAL_SECONDS, POLL_TIMEOUT_SECONDS
)

def _headers() -> Dict[str, str]:
    if not DATABRICKS_TOKEN:
        raise RuntimeError("DATABRICKS_TOKEN not found in environment.")
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

def start_conversation(user_input: str) -> Tuple[str, str]:
    r = requests.post(CONVERSATION_URL, headers=_headers(), json={"content": user_input})
    r.raise_for_status()
    js = r.json()
    return js.get("conversation_id"), js.get("message_id")

def get_message(conversation_id: str, message_id: str) -> Dict[str, Any]:
    url = MESSAGE_URL_TMPL.format(conversation_id=conversation_id, message_id=message_id)
    r = requests.get(url, headers=_headers())
    r.raise_for_status()
    return r.json()

def execute_query(conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
    url = EXECUTE_URL_TMPL.format(conversation_id=conversation_id, message_id=message_id, attachment_id=attachment_id)
    r = requests.post(url, headers=_headers())
    r.raise_for_status()
    return r.json()

def get_query_result(conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
    url = RESULT_URL_TMPL.format(conversation_id=conversation_id, message_id=message_id, attachment_id=attachment_id)
    r = requests.get(url, headers=_headers())
    r.raise_for_status()
    return r.json()

def poll_until_message_complete(conversation_id: str, message_id: str) -> Dict[str, Any]:
    start = time.time()
    while True:
        js = get_message(conversation_id, message_id)
        status = js.get("status")
        if status in ("COMPLETED", "FAILED"):
            return js
        if time.time() - start > POLL_TIMEOUT_SECONDS:
            raise TimeoutError(f"Timed out waiting for Genie message status: {status}")
        time.sleep(POLL_INTERVAL_SECONDS)

def poll_until_statement_done(conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
    start = time.time()
    while True:
        js = get_query_result(conversation_id, message_id, attachment_id)
        state = (
            js.get("statement_response", {})
              .get("status", {})
              .get("state")
        )
        if state in ("SUCCEEDED", "FAILED"):
            return js
        if time.time() - start > POLL_TIMEOUT_SECONDS:
            raise TimeoutError(f"Timed out waiting for query state: {state}")
        time.sleep(POLL_INTERVAL_SECONDS)

def extract_sql_from_message(message_json: Dict[str, Any]) -> str:
    try:
        return message_json["attachments"][0]["query"]["query"]
    except Exception:
        return ""

def extract_rows_from_result(result_json: Dict[str, Any]) -> Tuple[List[str], List[List[Any]]]:
    cols = (
        result_json["statement_response"]["manifest"]["schema"]["columns"]
    )
    col_names = [c["name"] for c in cols]
    rows = result_json["statement_response"]["result"]["data_array"]
    return col_names, rows
