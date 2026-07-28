@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo ERROR: .venv not found. Create it and install requirements.
  exit /b 1
)
".venv\Scripts\python.exe" -m daily_run --config "splunk_ingest\prod.toml" %*
exit /b %ERRORLEVEL%
