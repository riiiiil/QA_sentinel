# syntax=docker/dockerfile:1
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 QA_SENTINEL_DATA=/data
WORKDIR /app
COPY requirements.txt requirements-deploy.txt ./
# Optional certificate bundle for managed build proxies; never stored in a layer.
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir -r requirements-deploy.txt
RUN groupadd --gid 10001 sentinel \
    && useradd --uid 10001 --gid sentinel --no-create-home sentinel \
    && mkdir /data /backups && chown sentinel:sentinel /data /backups
COPY qa_sentinel ./qa_sentinel
COPY run.py ./
COPY scripts/container_health.py ./scripts/container_health.py
RUN chmod -R a+rX /app
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "scripts/container_health.py"]
CMD ["python", "run.py", "--host", "0.0.0.0", "--port", "8000"]
