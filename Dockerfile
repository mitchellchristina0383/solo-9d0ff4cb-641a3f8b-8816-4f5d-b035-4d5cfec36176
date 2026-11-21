FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CI_PACKAGER_DATA_DIR=/data

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Unprivileged runtime user; the named volume inherits ownership on first use.
RUN useradd --system --uid 10001 --home /srv/app packager \
    && mkdir -p /data \
    && chown -R packager:packager /srv/app /data
USER packager

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
