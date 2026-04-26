FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# DB und Uploads liegen unter /data — beim Deploy ein Volume mounten, z. B. -v wahlkampf-data:/data
# (sonst ist jeder neue Container leer und die SQLite-Datei wirkt „überschrieben“).
ENV DATABASE_URL=sqlite:////data/wahlkampf.db
ENV UPLOAD_DIR=/data/uploads

RUN mkdir -p /data/uploads

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
