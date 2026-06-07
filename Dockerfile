# Imagen de la app ILUS con OCR (Tesseract) para leer contratos ESCANEADOS.
# OCR determinista, open-source, SIN IA ni tokens. Reemplaza el buildpack de
# Google porque necesitamos instalar binarios de sistema (tesseract + poppler).
# Si esto fallara, basta BORRAR este Dockerfile y el deploy vuelve al buildpack.
FROM python:3.12-slim

# Binarios de sistema: Tesseract (OCR) + idioma español + poppler (pdf2image).
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-spa \
        poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Mismo arranque que el Procfile original (gunicorn app:app).
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 8 --worker-class gthread \
    --timeout 90 --graceful-timeout 30 --keep-alive 30 --max-requests 5000 \
    --max-requests-jitter 500 --access-logfile - --error-logfile - app:app
