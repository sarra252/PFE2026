param(
  [string]$HostUrl = "127.0.0.1",
  [int]$Port = 8000
)

uvicorn backend.app.main:app --reload --host $HostUrl --port $Port