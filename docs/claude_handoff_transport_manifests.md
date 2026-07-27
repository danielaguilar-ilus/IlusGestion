# Handoff: gestion de manifiestos de transporte

Contexto: esta fase separa el manifiesto operativo del chofer del respaldo
administrativo/financiero.

Regla de producto:
- Chofer/courier ve solo informacion fisica y operativa: documentos, cliente,
  direccion, comuna, bultos, peso, productos, estado, retiro y firma.
- Responsable interno recibe el respaldo administrativo con montos, costos,
  margen y control interno.
- No volver a exponer montos en `templates/transporte/manifiesto_firma.html`.

Puntos tocados:
- `app.py`: validacion de identidad de chofer, firma canvas, copia operativa al
  chofer, PDF administrativo separado para correo, hash de auditoria de firma y
  fallback cifrado Fernet si la firma no queda en storage persistente.
- `requirements.txt`: `cryptography` explicito para cifrado de firmas.
- `templates/transporte/manifiesto_firma.html`: manifiesto operativo sin datos
  financieros.
- `templates/transporte/manifiesto_admin_email.html`: respaldo administrativo.
- `templates/transporte/manifiesto_detalle.html`: modal con datos completos del
  chofer y firma tactil.
- `templates/transporte/firma_retiro_publico.html`: firma remota responsiva.

Continuacion sugerida:
- Cierre responsable ILUS con firma final cuando todos los items esten
  entregados o justificados.
- SLA por manifiesto/courier/chofer.
- Pantalla de asignacion previa de chofer antes de enviar el link.
- Limpieza gradual de alert/confirm/prompt nativos heredados en otros templates
  de transporte, fuera del alcance de esta fase.
