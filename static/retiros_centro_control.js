// ════════════════════════════════════════════════════════════════
//  Centro de control de Retiros (2026-09-25)
//  Pide /retiros/api/centro-control cada 20 s y pinta: KPIs, torre de
//  control del día (línea de tiempo + próximo retiro), alertas/SLA, mapa de
//  calor y embudo. El reloj, la línea "ahora" y la cuenta regresiva corren
//  cada segundo con la hora del SERVIDOR (hora Chile), no la del equipo.
// ════════════════════════════════════════════════════════════════
(function () {
  'use strict';
  const API = window.CC_API || '/retiros/api/centro-control';
  const INICIO = 9 * 60, FIN = 17 * 60, SPAN = FIN - INICIO;
  const $ = (id) => document.getElementById(id);
  let datos = null, desfase = 0, dias = 30, fallas = 0;

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  }
  function ahoraServidor() { return new Date(Date.now() + desfase); }
  function minDelDia(d) { return d.getHours() * 60 + d.getMinutes() + d.getSeconds() / 60; }
  function pct(min) { return Math.max(0, Math.min(100, (min - INICIO) / SPAN * 100)); }
  function horas(h) {
    if (h == null) return '';
    if (h < 1) return Math.round(h * 60) + ' min';
    return (Math.round(h * 10) / 10).toString().replace('.', ',') + ' h';
  }
  const CLASE = { agenda_confirmada: 'conf', en_preparacion: 'prep', retirada: 'ret', cerrada: 'ret', fallida: 'fall' };

  async function cargar() {
    try {
      const r = await fetch(API + '?dias=' + dias, { credentials: 'same-origin', cache: 'no-store' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const d = await r.json();
      if (!d || !d.ok) throw new Error('sin datos');
      // Desfase: reloj del servidor (hora Chile, sin zona) vs el del equipo
      const sv = new Date(d.ahora.replace(' ', 'T'));
      if (!isNaN(sv)) desfase = sv.getTime() - Date.now();
      datos = d; fallas = 0;
      $('ccErr').hidden = true;
      $('ccLiveTxt').textContent = 'EN VIVO';
      document.querySelector('.cc-live').classList.remove('is-off');
      pintar();
      $('ccUpd').textContent = 'Actualizado ' + d.ahora.slice(11, 16) + ' · se renueva cada 20 s';
    } catch (e) {
      fallas++;
      if (fallas >= 2) {
        $('ccErr').hidden = false;
        $('ccLiveTxt').textContent = 'SIN CONEXIÓN';
        document.querySelector('.cc-live').classList.add('is-off');
      }
    }
  }

  function pintar() {
    const d = datos, k = d.kpis;
    $('ccHoy').textContent = d.hoy_lbl;
    $('kHoy').textContent = k.hoy;
    $('kHoyS').textContent = k.hoy ? (k.hoy_retirados + ' ya retirados') : 'Sin citas confirmadas';
    $('kResp').textContent = k.por_responder;
    $('kRespS').textContent = k.fuera_sla ? (k.fuera_sla + ' fuera de plazo') : 'Todo dentro de plazo';
    $('kRespBox').classList.toggle('is-rojo', k.fuera_sla > 0);
    $('kRespBox').classList.toggle('is-ok', k.por_responder === 0);
    $('kRoj').textContent = k.alertas_rojas;
    $('kRojBox').classList.toggle('is-rojo', k.alertas_rojas > 0);
    $('kRojBox').classList.toggle('is-ok', k.alertas_rojas === 0);
    $('kAct').textContent = k.activos;
    const rd = d.rendimiento;
    $('kTasa').textContent = rd.tasa_conf == null ? '—' : rd.tasa_conf + '%';
    $('kTasaS').textContent = 'Solicitudes confirmadas · ' + rd.dias + ' días';
    pintarTorre(); pintarAlertas(); pintarMapa(); pintarEmbudo();
    tic();
  }

  // ── Torre de control ────────────────────────────────────────────
  function pintarTorre() {
    const t = datos.torre, items = t.items || [];
    const tl = $('ccTl');
    tl.querySelectorAll('.cc-ev').forEach((e) => e.remove());
    // Carriles: dos retiros en el mismo bloque (capacidad 2) van uno sobre otro
    const finCarril = [];
    items.slice().sort((a, b) => (a.ini_min || 0) - (b.ini_min || 0)).forEach((it) => {
      if (it.ini_min == null) return;
      let c = finCarril.findIndex((f) => f <= it.ini_min);
      if (c === -1) { c = finCarril.length; finCarril.push(0); }
      finCarril[c] = it.fin_min || it.ini_min + 30;
      const a = document.createElement('a');
      const cls = it.atrasado ? 'atr' : (CLASE[it.estado] || 'conf');
      a.className = 'cc-ev e-' + cls;
      a.href = it.url;
      a.style.left = pct(it.ini_min) + '%';
      a.style.width = 'calc(' + (pct(it.fin_min || it.ini_min + 30) - pct(it.ini_min)) + '% - 3px)';
      a.style.top = (8 + Math.min(c, 1) * 60) + 'px';
      a.title = it.code + ' · ' + it.cliente + ' · ' + it.desde + '–' + it.hasta + ' · ' + (it.atrasado ? 'Atrasado' : it.estado_lbl);
      // El bloque de 30 min es angosto: hora + código corto; el detalle va en
      // el title y en la lista de abajo.
      a.innerHTML = '<b>' + esc(it.desde) + '</b>' + esc(String(it.code || '').replace(/^RET-/, ''));
      tl.appendChild(a);
    });
    const ax = $('ccAxis');
    if (!ax.childElementCount) {
      for (let m = INICIO; m <= FIN; m += 60) {
        const s = document.createElement('span');
        s.style.left = pct(m) + '%';
        if (m === INICIO) s.style.transform = 'none';
        if (m === FIN) s.style.transform = 'translateX(-100%)';
        s.textContent = String(m / 60).padStart(2, '0') + ':00';
        ax.appendChild(s);
      }
    }
    const ret = items.filter((i) => i.estado === 'retirada' || i.estado === 'cerrada').length;
    $('torreS').textContent = items.length ? (items.length + ' citas · ' + ret + ' retiradas') : 'Sin citas confirmadas hoy';
    $('ccToday').innerHTML = items.length ? items.map((it) => {
      const cls = it.atrasado ? 'atr' : (CLASE[it.estado] || 'conf');
      const lbl = it.atrasado ? 'Atrasado' : it.estado_lbl;
      const bar = it.prep_total ? '<div class="cc-bar"><i style="width:' + Math.round(100 * it.prep_hechos / it.prep_total) + '%"></i></div>' : '';
      const extra = [it.bultos ? it.bultos + (it.bultos === 1 ? ' bulto' : ' bultos') : '', it.responsable].filter(Boolean).join(' · ');
      return '<a class="cc-row" href="' + esc(it.url) + '"><div class="cc-row-h">' + esc(it.desde) + '</div>' +
        '<div class="cc-row-c"><b>' + esc(it.code) + ' · ' + esc(it.cliente) + '</b><span>' + esc(extra) + '</span>' + bar + '</div>' +
        '<span class="cc-pill p-' + cls + '">' + esc(lbl) + '</span></a>';
    }).join('') : '<div class="cc-empty">No hay retiros confirmados para hoy.</div>';
  }

  // ── Alertas ─────────────────────────────────────────────────────
  function pintarAlertas() {
    const a = datos.alertas || [], s = datos.sla;
    $('ccSla').textContent = 'Plazo de respuesta en horas hábiles (lun–vie 09:00–18:00): verde menos de ' +
      s.ambar_h + ' h · ámbar hasta ' + s.rojo_h + ' h · rojo más de ' + s.rojo_h + ' h.';
    $('alertS').textContent = datos.alertas_total ? (datos.alertas_total + ' para revisar') : '';
    $('ccAlertas').innerHTML = a.length ? a.map((x) => {
      const meta = [x.code, x.cliente, x.canal, x.responsable].filter(Boolean).join(' · ');
      const der = x.horas_habiles != null
        ? '<div class="cc-al-h">' + horas(x.horas_habiles) + '<small>esperando</small></div>'
        : '<div class="cc-al-h" style="font-size:.85rem">' + esc(x.desde) + '</div>';
      return '<a class="cc-al n-' + x.nivel + '" href="' + esc(x.url) + '"><i class="s"></i><div><div class="cc-al-t">' +
        esc(x.titulo) + '</div><div class="cc-al-m">' + esc(meta) + '</div></div>' + der + '</a>';
    }).join('') : '<div class="cc-ok"><i class="bi bi-check2-circle me-1"></i>Todo al día: nada urgente por resolver.</div>';
  }

  // ── Mapa de calor ───────────────────────────────────────────────
  function pintarMapa() {
    const m = datos.mapa, cap = datos.capacidad || 2, bl = m.bloques || [];
    const h = $('ccHeat');
    h.style.gridTemplateColumns = '96px repeat(' + bl.length + ', minmax(0,1fr))';
    let html = '<div></div>' + bl.map((b) => '<div class="cc-hh">' + esc(b) + '</div>').join('');
    (m.dias || []).forEach((d) => {
      html += '<div class="cc-hd' + (d.es_hoy ? ' is-hoy' : '') + '">' + esc(d.lbl) + '</div>';
      d.celdas.forEach((c) => {
        let cls = 'cc-hc', txt = c.n ? c.n : '';
        if (c.bloqueo) { cls += ' blk'; txt = '×'; }
        else if (c.n >= cap) cls += (c.n > cap ? ' lx' : ' l2');
        else if (c.n > 0) cls += ' l1';
        if (c.pasado) cls += ' pas';
        const tit = d.lbl + ' ' + c.h + ' · ' + (c.bloqueo ? 'Bloqueado: ' + c.bloqueo : (c.n + ' de ' + cap + ' cupos usados'));
        html += '<div class="' + cls + '" title="' + esc(tit) + '">' + txt + '</div>';
      });
    });
    h.innerHTML = html;
    $('ccLegend').innerHTML =
      '<span><i style="background:rgba(255,255,255,.08)"></i>Libre</span>' +
      '<span><i style="background:rgba(245,158,11,.55)"></i>Medio cupo</span>' +
      '<span><i style="background:rgba(220,38,38,.8)"></i>Lleno (' + cap + ' de ' + cap + ')</span>' +
      '<span><i style="background:repeating-linear-gradient(135deg,rgba(248,113,113,.5) 0 3px,transparent 3px 6px)"></i>Bloqueado por ILUS</span>' +
      '<span>Cuenta solicitudes, propuestas y citas activas.</span>';
  }

  // ── Embudo ──────────────────────────────────────────────────────
  function pintarEmbudo() {
    const e = datos.embudo, r = datos.rendimiento, base = e.solicitudes || 0;
    const fila = (lbl, n, cls) => '<div class="cc-fr ' + (cls || '') + '"><div class="cc-fr-l">' + lbl + '</div>' +
      '<div class="cc-fr-b"><i style="width:' + (base ? Math.max(2, Math.round(100 * n / base)) : 0) + '%"></i></div>' +
      '<div class="cc-fr-n">' + n + '</div></div>';
    $('ccFunnel').innerHTML = base ? (
      fila('Solicitudes', e.solicitudes) + fila('Con propuesta', e.con_propuesta) +
      fila('Confirmadas', e.confirmadas, 'f-ok') + fila('Retiradas', e.retiradas, 'f-ok') +
      fila('Canceladas', e.canceladas, 'f-mal') + fila('Fallidas', e.fallidas, 'f-mal')
    ) : '<div class="cc-empty">Sin solicitudes en los últimos ' + r.dias + ' días.</div>';
    const st = (v, l) => '<div class="cc-st"><b>' + v + '</b><span>' + l + '</span></div>';
    $('ccStats').innerHTML =
      st(r.resp_prom_h == null ? '—' : horas(r.resp_prom_h), 'Primera respuesta (prom.)') +
      st(r.resp_mediana_h == null ? '—' : horas(r.resp_mediana_h), 'Primera respuesta (mediana)') +
      st(r.tasa_ret == null ? '—' : r.tasa_ret + '%', 'Confirmadas que retiraron');
    const tabla = (lista) => lista.length ? lista.map((x) => '<div class="cc-tr"><b>' + esc(x.nombre) + '</b><span>' +
      x.solicitudes + ' sol. · ' + x.confirmadas + ' conf. · ' + x.retiradas + ' ret.</span></div>').join('')
      : '<div class="cc-tr"><span>Sin datos</span></div>';
    $('ccCanal').innerHTML = tabla(datos.por_canal || []);
    $('ccResp').innerHTML = tabla(datos.por_responsable || []);
  }

  // ── Cada segundo: reloj, línea "ahora" y cuenta regresiva ───────
  function tic() {
    const n = ahoraServidor();
    $('ccClock').textContent = n.toTimeString().slice(0, 8);
    if (!datos) return;
    const mm = minDelDia(n), now = $('ccNow');
    if (mm >= INICIO && mm <= FIN) {
      now.style.display = 'block';
      now.style.left = pct(mm) + '%';
      now.querySelector('span').textContent = n.toTimeString().slice(0, 5);
    } else { now.style.display = 'none'; }
    const px = datos.torre.proximo, box = $('ccNext');
    if (!px) {
      box.className = 'cc-next is-vacio';
      box.innerHTML = '<i class="bi bi-calendar-check"></i> No quedan citas pendientes para hoy.';
      return;
    }
    const falta = px.ini_min - mm;
    let cd, lbl;
    if (falta > 0) {
      const h = Math.floor(falta / 60), mi = Math.floor(falta % 60), se = Math.floor((falta * 60) % 60);
      cd = h ? (h + ' h ' + String(mi).padStart(2, '0') + ' min')
             : (mi + ' min ' + String(se).padStart(2, '0') + ' s');
      lbl = 'Próximo retiro en';
    } else { cd = 'AHORA'; lbl = 'En su bloque'; }
    box.className = 'cc-next';
    box.innerHTML = '<div class="cc-next-cd"><small>' + lbl + '</small>' + cd + '</div>' +
      '<div class="cc-next-d"><b>' + esc(px.code) + '</b> · ' + esc(px.cliente) + '<br>' + esc(px.desde) + '–' + esc(px.hasta) +
      (px.bultos ? ' · ' + px.bultos + (px.bultos === 1 ? ' bulto' : ' bultos') : '') + ' · ' + esc(px.estado_lbl) + '</div>' +
      '<a href="' + esc(px.url) + '">Abrir ficha <i class="bi bi-arrow-right"></i></a>';
  }

  document.querySelectorAll('.cc-seg button').forEach((b) => b.addEventListener('click', () => {
    document.querySelectorAll('.cc-seg button').forEach((x) => x.classList.toggle('is-on', x === b));
    dias = parseInt(b.dataset.dias, 10) || 30;
    cargar();
  }));
  $('ccFull').addEventListener('click', () => {
    const el = $('ccShell');
    if (document.fullscreenElement) document.exitFullscreen();
    else if (el.requestFullscreen) el.requestFullscreen().catch(() => {});
  });

  cargar();
  setInterval(() => { if (!document.hidden) cargar(); }, 20000);
  setInterval(tic, 1000);
})();
