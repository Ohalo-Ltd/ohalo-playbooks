FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY atlan_dxr_integration/ atlan_dxr_integration/

CMD ["python", "-m", "atlan_dxr_integration"]
