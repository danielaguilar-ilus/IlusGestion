import sqlite3
conn = sqlite3.connect("etiquetas.db")
p    = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
b    = conn.execute("SELECT COUNT(*) FROM bultos").fetchone()[0]
conf = conn.execute("SELECT COUNT(*) FROM products WHERE estado='Confirmado'").fetchone()[0]
pend = conn.execute("SELECT COUNT(*) FROM products WHERE estado='Pendiente'").fetchone()[0]
imp  = conn.execute("SELECT COUNT(*) FROM products WHERE estado='Impreso'").fetchone()[0]
s    = conn.execute("SELECT sync_at, nuevos FROM sync_log ORDER BY id DESC LIMIT 1").fetchone()
print("Productos:", p)
print("Bultos totales:", b)
print("Confirmados:", conf)
print("Pendientes:", pend)
print("Impresos:", imp)
if s: print("Ultima sync:", s[0], "- Nuevos:", s[1])
conn.close()
