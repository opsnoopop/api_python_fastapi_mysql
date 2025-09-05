FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ติดตั้งระบบ และไลบรารีที่จำเป็น
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ติดตั้ง dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# คัดลอกโค้ด
COPY app /app/app

EXPOSE 3000

# รัน Uvicorn (production-ready พื้นฐาน)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000", "--workers", "2"]
