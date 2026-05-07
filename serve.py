from app import app

app.config['TEMPLATES_AUTO_RELOAD'] = True   # siempre sirve templates frescos del disco
app.run(debug=False, port=5000)
