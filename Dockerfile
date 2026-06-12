# Imagen de la app ILUS con OCR (Tesseract) para leer contratos ESCANEADOS,
# Y Chromium (Playwright headless) para generar PDFs de etiquetas / módulo Etiquetas.
# Todo determinista, open-source, SIN IA ni tokens. Reemplaza el buildpack de
# Google porque necesitamos instalar binarios de sistema (tesseract + poppler +
# las libs que pide Chromium para correr en modo headless).
# Si esto fallara, basta BORRAR este Dockerfile y el deploy vuelve al buildpack
# (tomá nota: si volves al buildpack, las etiquetas PDF van a fallar a menos
# que el buildpack instale Chromium tambien — el nixpacks.toml lo hace).
FROM python:3.12-slim

# Binarios de sistema:
#   - Tesseract (OCR) + idioma español
#   - poppler-utils (pdf2image)
#   - libs que necesita Chromium headless (Playwright instala el binario aparte)
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-spa \
        poppler-utils \
        # Chromium runtime deps (necesarias para Playwright headless en Linux slim).
        # Ver: https://playwright.dev/python/docs/browsers#install-system-dependencies
        libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
        libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 \
        fonts-liberation libappindicator3-1 libxss1 libnss3-tools wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Chromium para Playwright (el binario va a /root/.cache/ms-playwright
# por default; lo dejamos ahí porque corremos como root en el container).
# Si esto fallara en el build, la app sigue arrancando pero las PDFs de
# etiquetas dan 503 amigable (ver _pw_pdf en app.py).
RUN python -m playwright install chromium --with-deps || \
    echo "WARNING: Chromium install failed; PDF endpoints will return 503"

COPY . ./

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Mismo arranque que el Procfile original (gunicorn app:app).
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 8 --worker-class gthread \
    --timeout 90 --graceful-timeout 30 --keep-alive 30 --max-requests 5000 \
    --max-requests-jitter 500 --access-logfile - --error-logfile - app:app

