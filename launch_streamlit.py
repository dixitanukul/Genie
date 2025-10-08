# Databricks notebook source
import sys, importlib.util, subprocess, os, shlex, time, pathlib, shutil

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

APP_PATH   = "/tmp/app_streamlit.py"
LOG_PATH   = "/tmp/streamlit_app.log"
PORT       = 8501

ws_root = "/Workspace/Users/pdevis01@blueshieldca.com/Genie_modular"
files = {
    f"{ws_root}/app_streamlit.py": "/tmp/app_streamlit.py",
    f"{ws_root}/genie_client.py":  "/tmp/genie_client.py",
    f"{ws_root}/settings.py":      "/tmp/settings.py",
}

os.makedirs("/tmp", exist_ok=True)
for src, dst in files.items():
    shutil.copy(src, dst)
    print("Copied:", dst)

os.makedirs("/tmp/.streamlit", exist_ok=True)
pathlib.Path("/tmp/.streamlit/secrets.toml").write_text("", encoding="utf-8")
pathlib.Path("/tmp/.streamlit/config.toml").write_text('browser.gatherUsageStats = false\n', encoding="utf-8")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_get = lambda o: o.get() if o and o.isDefined() else None
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
cluster_id    = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
workspace_id  = _get(ctx.workspaceId()) or _get(ctx.tags().get("orgId"))
api_token     = _get(ctx.apiToken())

proxy_url = f"https://{workspace_url}/driver-proxy/o/{workspace_id}/{cluster_id}/{PORT}/"
print("🔗 Open your Streamlit app:", proxy_url)
print("📄 App:", APP_PATH)

env = os.environ.copy()
env["HOME"] = "/tmp"
env["STREAMLIT_CONFIG_DIR"] = "/tmp/.streamlit"
env["STREAMLIT_SECRETS_FILE"] = "/tmp/.streamlit/secrets.toml"
env["PYTHONPATH"] = (env.get("PYTHONPATH", "") + (":" if env.get("PYTHONPATH") else "") + "/tmp")

if api_token:
    env["DATABRICKS_TOKEN"] = api_token
env["DATABRICKS_HOST"] = f"https://{workspace_url}"

cmd = (
    f"cd /tmp && "
    f"streamlit run {shlex.quote(APP_PATH)} "
    f"--server.port {PORT} --server.address 0.0.0.0 --server.headless true "
    f"> {shlex.quote(LOG_PATH)} 2>&1 & echo $!"
)
pid = subprocess.check_output(cmd, shell=True, text=True, env=env).strip()
print("🚀 PID:", pid)
time.sleep(3)
