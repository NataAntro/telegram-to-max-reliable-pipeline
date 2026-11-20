FROM python:3.13-slim@sha256:6771159cd4fa5d9bba1258caf0b82e6b73458c694d178ad97c5e925c2d0e1a91 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app
RUN addgroup --system pipeline && adduser --system --ingroup pipeline pipeline

COPY requirements-runtime.lock ./
RUN python -m pip install --no-deps -r requirements-runtime.lock

COPY src ./src

USER pipeline
EXPOSE 8000

CMD ["uvicorn", "reliable_pipeline.api:app", "--host", "0.0.0.0", "--port", "8000"]
