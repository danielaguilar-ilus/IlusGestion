# ─────────────────────────────────────────────────────────────
#  Cargador local — lee .env (si existe) ANTES de importar app
# ─────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()   # busca .env en el directorio actual
except ImportError:
    # En Railway no hay python-dotenv y no hace falta (env vars en dashboard)
    pass

from app import app

app.config['TEMPLATES_AUTO_RELOAD'] = True   # siempre sirve templates frescos del disco
app.run(debug=False, port=5000)
