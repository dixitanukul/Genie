# Databricks notebook source
# Full fixed launcher for App Compute — avoids /tmp permission issues by using /databricks/driver
import sys, importlib.util, subprocess, os, shlex, time, pathlib, shutil, getpass, uuid

# ====== CONFIG: set this to where you store the three source files in Workspace ======
# Files expected in this folder: app_streamlit.py, genie_client.py, settings.py
WS_ROOT = "/Workspace/Users/pdevis01@blueshieldca.com/Genie_modular"

# ====== Dependencies (installed if missing) ======
REQUIRED_PYPI = [
    ("streamlit", "streamlit"),
    ("pandas", "pandas"),
    ("requests", "requests"),
]

def _is_installed(import_name: str) -> bool:
    return importlib.util.find_spec(import_name) is not None

def _ensure_deps():
    missing = [pip_name for import_name, pip_name in REQUIRED_PYPI if not _is_installed(import_name)]
    if not missing:
        print("✅ All required packages already installed.")
        return
    print("📦 Installing missing packages:", ", ".join(missing))
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])
    print("✅ Package install finished.")

_ensure_deps()

# ====== Private, writable workspace under /databricks/driver ======
USER = getpass.getuser() or "driver"
RUN_ID = uuid.uuid4().hex[:8]
BASE_DIR = f"/databricks/driver/genie_app_{USER}_{RUN_ID}"
APP_DIR  = f"{BASE_DIR}/app"
CONF_DIR = f"{BASE_DIR}/.streamlit"
LOG_PATH = f"{BASE_DIR}/streamlit_app.log"
PORT     = 8501

for d in (BASE_DIR, APP_DIR, CONF_DIR):
    os.makedirs(d, exist_ok=True)

# ====== Copy source files from Workspace to driver ======
files = {
    f"{WS_ROOT}/app_streamlit.py": f"{APP_DIR}/app_streamlit.py",
    f"{WS_ROOT}/genie_client.py":  f"{APP_DIR}/genie_client.py",
    f"{WS_ROOT}/settings.py":      f"{APP_DIR}/settings.py",
}

for src, dst in files.items():
    if not os.path.exists(src):
        raise FileNotFoundError(f"Expected file not found in Workspace: {src}")
    shutil.copy(src, dst)
    print("Copied:", dst)

# ====== Streamlit config (in a path we own) ======
pathlib.Path(f"{CONF_DIR}/secrets.toml").write_text("", encoding="utf-8")
pathlib.Path(f"{CONF_DIR}/config.toml").write_text("browser.gatherUsageStats = false\n", encoding="utf-8")

# ====== Databricks context for driver-proxy URL & token ======
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_get = lambda o: o.get() if o and o.isDefined() else None
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
cluster_id    = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
workspace_id  = _get(ctx.workspaceId()) or _get(ctx.tags().get("orgId"))
api_token     = _get(ctx.apiToken())

proxy_url = f"https://{workspace_url}/driver-proxy/o/{workspace_id}/{cluster_id}/{PORT}/"
print("🔗 Open your Streamlit app:", proxy_url)
print("📄 App:", f"{APP_DIR}/app_streamlit.py")
print("🗂️ Base dir:", BASE_DIR)

# ====== Environment for child process ======
env = os.environ.copy()
env["HOME"] = BASE_DIR
env["STREAMLIT_CONFIG_DIR"] = CONF_DIR
env["STREAMLIT_SECRETS_FILE"] = f"{CONF_DIR}/secrets.toml"
env["PYTHONPATH"] = (env.get("PYTHONPATH", "") + (":" if env.get("PYTHONPATH") else "") + APP_DIR)

# Export token + host for the app (same behavior as before)
if api_token:
    env["DATABRICKS_TOKEN"] = api_token
env["DATABRICKS_HOST"] = f"https://{workspace_url}"

# ====== Launch Streamlit from APP_DIR via driver-proxy ======
cmd = (
    f"cd {shlex.quote(APP_DIR)} && "
    f"streamlit run app_streamlit.py "
    f"--server.port {PORT} --server.address 0.0.0.0 --server.headless true "
    f"> {shlex.quote(LOG_PATH)} 2>&1 & echo $!"
)
pid = subprocess.check_output(cmd, shell=True, text=True, env=env).strip()
print("🚀 PID:", pid)
time.sleep(3)
print("📜 Logs:", LOG_PATH)
