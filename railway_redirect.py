# ════════════════════════════════════════════════════════════════════
#  REDIRECTOR DE RAILWAY → GOOGLE CLOUD RUN  (el "vigilante" del link viejo)
# ════════════════════════════════════════════════════════════════════
#  Railway dejó de servir la app completa: ahora es SOLO un redirector.
#  Cualquiera que entre por el link viejo de Railway
#  (web-production-85732.up.railway.app) se reenvía al nuevo de Google,
#  preservando ruta + query string.
#
#  Por qué un archivo separado y mínimo (no la app pesada):
#    - La app completa (greenlet/playwright/MySQL a Clever Cloud) NO levanta
#      en el build de Railway → daba 502. Este redirector solo necesita
#      flask + gunicorn → build rápido y a prueba de fallos.
#    - Lo arranca `nixpacks.toml` (que SOLO usa Railway). Google Cloud Run
#      usa el Dockerfile y la app completa — este archivo NO lo afecta.
#
#  302 (temporal), NO 301: el destino final cambiará al dominio propio
#  (sistema.ilusfitness.com) en el cutover; un 301 lo cachearían los
#  navegadores de forma permanente. Cambiar destino con la env var
#  ILUS_REDIRECT_TO en Railway (sin tocar código).
# ════════════════════════════════════════════════════════════════════
import os
from flask import Flask, redirect, request

app = Flask(__name__)

DESTINO = (os.environ.get("ILUS_REDIRECT_TO")
           or "https://ilus-app-469212710544.southamerica-west1.run.app").rstrip("/")


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def _redir(path):
    destino = DESTINO + "/" + path
    if request.query_string:
        destino += "?" + request.query_string.decode("utf-8", "ignore")
    return redirect(destino, code=302)


@app.route("/_redir_health")
def _health():
    return {"ok": True, "destino": DESTINO}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
