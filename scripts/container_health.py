"""Local HTTP and scheduler health check, independent of proxy environment."""
from datetime import datetime, timezone
import http.client
import json


def main():
    connection = http.client.HTTPConnection("127.0.0.1", 8000, timeout=3)
    try:
        connection.request("GET", "/api/health")
        response = connection.getresponse()
        if response.status != 200:
            return 1
        data = json.loads(response.read(65536))
        heartbeat = datetime.fromisoformat(data["scheduler_heartbeat"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - heartbeat).total_seconds()
        return 0 if data["status"] == "ok" and -30 <= age <= 90 else 1
    except (OSError, ValueError, TypeError, KeyError, http.client.HTTPException):
        return 1
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
