import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://adb-640321604414221.1.azuredatabricks.net")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f0a27dc74b1eb4a86f10883167c037")

CONVERSATION_URL = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
MESSAGE_URL_TMPL = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{{conversation_id}}/messages/{{message_id}}"
EXECUTE_URL_TMPL = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{{conversation_id}}/messages/{{message_id}}/attachments/{{attachment_id}}/execute-query"
RESULT_URL_TMPL   = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{{conversation_id}}/messages/{{message_id}}/attachments/{{attachment_id}}/query-result"

POLL_INTERVAL_SECONDS = float(os.getenv("GENIE_POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT_SECONDS  = int(os.getenv("GENIE_POLL_TIMEOUT_SECONDS", "180"))
