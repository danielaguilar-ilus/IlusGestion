# ════════════════════════════════════════════════════════════════════
#  Procfile — ⚠️ ESTE ARCHIVO LO USA *SOLO RAILWAY*
# ════════════════════════════════════════════════════════════════════
#  Google Cloud Run (producción) NO usa este Procfile: usa el Dockerfile,
#  que corre la app completa (gunicorn app:app). Ver .github/workflows/deploy.yml
#  (gcloud run deploy ilus-app --source . → detecta el Dockerfile).
#
#  Railway = SOLO REDIRECTOR a Google. Por eso acá arrancamos el redirector
#  liviano (railway_redirect:app), NO la app completa (app:app):
#    - app:app necesita greenlet/pymssql/playwright/etc. que el build de
#      Railway NO instala (nixpacks.toml solo instala flask+gunicorn) →
#      al arrancar daba ImportError → "Deployment crashed" en cada PR.
#    - railway_redirect:app solo necesita flask → arranca siempre y reenvía
#      el link viejo de Railway al nuevo de Google (302, preserva ruta+query).
#
#  🔴 NO volver a poner app:app aquí. Si Railway debe correr la app completa
#     algún día, se hace con el Dockerfile, no con este Procfile.
# ════════════════════════════════════════════════════════════════════
web: gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 4 --access-logfile - --error-logfile - railway_redirect:app
