# worker/Dockerfile - Marathon Pulse cloud worker (Azure Container Apps)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY settings.py schemas.py processor.py consumer.py ./

# Run as a non-root user
RUN useradd --create-home appuser
USER appuser

CMD ["python", "consumer.py"]
