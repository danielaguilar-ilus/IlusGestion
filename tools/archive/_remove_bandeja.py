"""
Elimina la sección "Bandeja: hacer hoy" del monitor de Retiros (Daniel 2026-06-01).
================================================================================
Daniel: "se ve feo y no me gusta". Quitamos:
  - Front: comentario + <style>.bandeja-* + <div class=bandeja-acc> + el <script>
           que la llena por AJAX. PERO el mismo <script> contiene el handler del
           botón "Enviar recordatorios 24h" (que vive en el header) → lo
           preservamos reescribiéndolo limpio.
  - Backend: endpoint /retiros/api/bandeja-hoy (pickup_bandeja_hoy). Se preserva
           el endpoint de recordatorios 24h que viene justo después.

Anclas de contenido (no líneas fijas). Idempotente-ish: si ya no encuentra las
anclas, aborta sin tocar.
"""
import io

TPL = "templates/retiros/internal_dashboard.html"
PY = "pickups_module.py"

# Script limpio que reemplaza al IIFE viejo: SOLO el handler de recordatorios 24h.
RECORD_SCRIPT = """<script>
// Botón "Enviar recordatorios 24h" (solo admins) — header del monitor.
document.addEventListener('DOMContentLoaded', function(){
  var btnRem = document.getElementById('btn-recordatorios-24h');
  if (!btnRem) return;
  btnRem.addEventListener('click', async function(){
    var ok = await ilusConfirm({
      title: 'Enviar recordatorios 24h',
      message: '¿Enviar recordatorios a todos los clientes con retiro confirmado para MAÑANA?',
      sub: 'Se envía un email a cada cliente recordándole el horario y bodega.',
      okLabel: 'Sí, enviar a todos',
    });
    if (!ok) return;
    btnRem.disabled = true;
    var orig = btnRem.innerHTML;
    btnRem.innerHTML = '<i class="bi bi-hourglass-split me-1"></i>Enviando...';
    try {
      var r = await fetch('{{ url_for("pickup_enviar_recordatorios_24h") }}', {
        method:'POST', credentials:'same-origin',
        headers:{'X-Requested-With':'fetch','Content-Type':'application/x-www-form-urlencoded'}
      });
      var data = await r.json();
      var errsTxt = '';
      if (data.errores && data.errores.length){
        errsTxt = '\\nErrores: ' + data.errores.slice(0,5).map(function(e){
          return (e.code || '?') + ': ' + (e.error || '');
        }).join('; ');
      }
      await ilusAlert({
        title: 'Recordatorios 24h',
        message: 'Enviados: ' + (data.enviados || 0) +
                 ' · Omitidos: ' + (data.omitidos || 0) +
                 ' · Candidatos: ' + (data.total_candidatos || 0),
        sub: errsTxt || undefined,
        type: (data.errores && data.errores.length) ? 'warning' : 'success',
      });
    } catch(err) {
      ilusToast('Error al enviar recordatorios. Revisa la consola.', { type:'error' });
      console.error('[recordatorios-24h]', err);
    } finally {
      btnRem.disabled = false; btnRem.innerHTML = orig;
    }
  });
});
</script>

"""


def find(lines, substr, start=0):
    for i in range(start, len(lines)):
        if substr in lines[i]:
            return i
    raise SystemExit(f"[ERR] ancla no encontrada: {substr!r}")


def do_template():
    with io.open(TPL, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    b = find(lines, 'BANDEJA "HACER HOY" — 4 secciones')
    start = b - 1                       # la línea '{# ════'
    if "{#" not in lines[start]:
        raise SystemExit(f"[ERR] esperaba '{{#' en linea {start+1}: {lines[start]!r}")
    e = find(lines, '<div class="row g-3 mb-3">', start)   # primeras stats cards (se quedan)
    n = e - start
    repl = RECORD_SCRIPT.splitlines(keepends=True)
    lines[start:e] = repl
    with io.open(TPL, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)
    print(f"[OK] template: removidas {n} lineas de bandeja, insertado script recordatorios ({len(repl)} lineas)")


def do_backend():
    with io.open(PY, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    bs = find(lines, 'BANDEJA "HACER HOY" — tareas prioritarias')
    start = bs - 1                      # la línea '# ════'
    if "═" not in lines[start]:
        raise SystemExit(f"[ERR] esperaba barra '#' en linea {start+1}: {lines[start]!r}")
    be = find(lines, "RECORDATORIO 24h — disparo manual", start)
    end = be - 1                        # la barra '# ════' de RECORDATORIO (se preserva)
    if "═" not in lines[end]:
        raise SystemExit(f"[ERR] esperaba barra '#' en linea {end+1}: {lines[end]!r}")
    n = end - start
    del lines[start:end]
    with io.open(PY, "w", encoding="utf-8", newline="\n") as fh:
        fh.writelines(lines)
    print(f"[OK] backend: removidas {n} lineas (pickup_bandeja_hoy). RECORDATORIO 24h preservado.")


if __name__ == "__main__":
    do_template()
    do_backend()
