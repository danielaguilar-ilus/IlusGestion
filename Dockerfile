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

# ════════════════════════════════════════════════════════════════════
#  PLAYWRIGHT_BROWSERS_PATH — MISMA RUTA EN BUILD Y EN RUNTIME
#  ------------------------------------------------------------------
#  BUG REAL MEDIDO EN PRODUCCION (Cloud Run, 2026-08-12): en CADA arranque
#  en frio los logs mostraban
#     [playwright] Chromium no encontrado. Auto-install en /app/.pw-browsers...
#  y se bajaban ~170MB de Chromium (20-27s) 4 veces el mismo dia.
#
#  CAUSA: este Dockerfile SI instalaba Chromium en el build, pero sin fijar
#  PLAYWRIGHT_BROWSERS_PATH → iba a /root/.cache/ms-playwright. En cambio
#  app.py (linea ~20) hace
#     os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/app/.pw-browsers")
#  antes de importar playwright. Resultado: el binario horneado en la imagen
#  quedaba en una ruta que el runtime NUNCA miraba → "Executable doesn't
#  exist" → se disparaba el auto-install de runtime
#  (_pw_install_chromium_runtime) en cada instancia nueva. Como el FS del
#  contenedor de Cloud Run no persiste — y ademas es RAM — cada instancia
#  volvia a descargarlo y se comia ~350MB de los 2Gi de memoria.
#
#  FIX: declarar la variable ANTES del install para que build y runtime
#  apunten al mismo directorio. Se elige /app/.pw-browsers a proposito: es
#  exactamente el default que app.py ya tiene hardcodeado, asi que si algun
#  dia esta ENV se perdiera (deploy con --env-vars-file, override manual en
#  la consola, etc.) el setdefault de Python cae en la MISMA carpeta donde
#  el build dejo el binario. Una sola ruta, imposible que se desalineen.
#
#  ⚠️ No mover este ENV despues del install ni cambiar la ruta en un solo
#     lado: si build y runtime se separan otra vez, vuelve el bug.
# ════════════════════════════════════════════════════════════════════
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.pw-browsers

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Chromium para Playwright DENTRO de la imagen (build time), en la
# ruta de arriba. Con esto el auto-install de runtime queda como red de
# seguridad que en produccion NO se dispara nunca.
#
# Se reintenta 3 veces: una caida momentanea del CDN de Playwright no debe
# romper un deploy, pero tampoco queremos que falle en silencio (fallar
# callado es justo lo que produjo el bug de arriba). Si aun asi no queda,
# el build NO se rompe: la app arranca igual y degrada como hoy
# (auto-install en el primer PDF y, si tampoco puede, HTML imprimible /
# 503 amigable — ver _pw_pdf y PDFEngineUnavailable en app.py).
RUN set -eu; \
    ok=0; \
    for intento in 1 2 3; do \
        if python -m playwright install --with-deps chromium; then ok=1; break; fi; \
        echo "[build][playwright] intento $intento fallo; reintentando..."; \
        sleep 5; \
    done; \
    if [ "$ok" = "1" ] && [ -d "$PLAYWRIGHT_BROWSERS_PATH" ]; then \
        echo "[build][playwright] OK Chromium horneado en $PLAYWRIGHT_BROWSERS_PATH"; \
        ls -1 "$PLAYWRIGHT_BROWSERS_PATH" || true; \
        du -sh "$PLAYWRIGHT_BROWSERS_PATH" || true; \
    else \
        echo "[build][playwright] WARNING: Chromium NO quedo en la imagen."; \
        echo "[build][playwright] WARNING: la app arranca igual, pero cada instancia"; \
        echo "[build][playwright] WARNING: nueva lo bajara en runtime (~25s) o degradara"; \
        echo "[build][playwright] WARNING: a HTML imprimible. Revisar este build."; \
    fi

COPY . ./

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Mismo arranque que el Procfile original (gunicorn app:app).
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 8 --worker-class gthread \
    --timeout 90 --graceful-timeout 30 --keep-alive 30 --max-requests 5000 \
    --max-requests-jitter 500 --access-logfile - --error-logfile - app:app

