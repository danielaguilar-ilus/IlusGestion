'use strict';

// ─── Peso Volumétrico ───────────────────────────────────────────
function calcPV(l, a, h) {
  l = parseFloat(l) || 0;
  a = parseFloat(a) || 0;
  h = parseFloat(h) || 0;
  return l && a && h ? (l * a * h / 4000).toFixed(2) : '—';
}

// ─── Row state management ────────────────────────────────────────
let rowCount = 0;

function updateTotals() {
  const rows = document.querySelectorAll('#bultosBody tr');
  if (!rows.length) return;

  let totalPeso = 0, totalPV = 0, allZero = true;
  rows.forEach(row => {
    const peso = parseFloat(row.querySelector('[data-field="peso"]')?.value) || 0;
    const pv   = parseFloat(row.querySelector('.pv-cell')?.dataset.val) || 0;
    totalPeso += peso;
    totalPV   += pv;
    if (peso > 0) allZero = false;
  });

  document.getElementById('totalPeso').textContent = totalPeso.toFixed(2) + ' kg';
  document.getElementById('totalPV').textContent   = totalPV.toFixed(2);
  document.getElementById('totalsRow').style.display = rows.length ? '' : 'none';
  document.getElementById('bultoCountBadge').textContent = rows.length;
  document.getElementById('emptyBultos').style.display   = rows.length ? 'none' : '';
}

function refreshPV(rowNum) {
  const row  = document.getElementById(`row_${rowNum}`);
  if (!row) return;
  const l    = row.querySelector('[data-field="largo"]')?.value;
  const a    = row.querySelector('[data-field="ancho"]')?.value;
  const h    = row.querySelector('[data-field="alto"]')?.value;
  const pv   = calcPV(l, a, h);
  const cell = row.querySelector('.pv-cell');
  cell.textContent   = pv;
  cell.dataset.val   = parseFloat(pv) || 0;
  updateTotals();
}

// ─── Add row ────────────────────────────────────────────────────
function addBultoRow(num, data) {
  if (rowCount >= MAX_BULTOS) {
    alert(`Máximo ${MAX_BULTOS} bultos permitidos.`);
    return;
  }
  rowCount++;
  const n    = num || rowCount;
  const d    = data || {};
  const tbody = document.getElementById('bultosBody');

  const tr = document.createElement('tr');
  tr.id = `row_${n}`;

  tr.innerHTML = `
    <td class="text-center fw-bold text-muted">${n}</td>
    <td><input type="number" name="largo_${n}" min="0" step="0.1"
               class="form-control form-control-sm" data-field="largo" data-row="${n}"
               value="${d.largo || ''}" placeholder="0"></td>
    <td><input type="number" name="ancho_${n}" min="0" step="0.1"
               class="form-control form-control-sm" data-field="ancho" data-row="${n}"
               value="${d.ancho || ''}" placeholder="0"></td>
    <td><input type="number" name="alto_${n}"  min="0" step="0.1"
               class="form-control form-control-sm" data-field="alto"  data-row="${n}"
               value="${d.alto || ''}" placeholder="0"></td>
    <td><input type="number" name="peso_${n}"  min="0" step="0.01"
               class="form-control form-control-sm" data-field="peso"  data-row="${n}"
               value="${d.peso || ''}" placeholder="0"></td>
    <td class="pv-cell" data-val="0">—</td>
    <td>
      <button type="button" class="btn btn-outline-danger btn-sm px-2"
              onclick="removeBultoRow(${n})" title="Eliminar bulto">
        <i class="bi bi-trash"></i>
      </button>
    </td>
  `;

  tbody.appendChild(tr);

  // Wire up live PV calculation
  tr.querySelectorAll('input[data-field="largo"], input[data-field="ancho"], input[data-field="alto"]')
    .forEach(inp => inp.addEventListener('input', () => refreshPV(n)));

  tr.querySelector('input[data-field="peso"]')
    .addEventListener('input', updateTotals);

  // Trigger initial PV if pre-populated
  if (d.largo || d.ancho || d.alto) refreshPV(n);
  else updateTotals();

  document.getElementById('emptyBultos').style.display   = 'none';
  document.getElementById('totalsRow').style.display     = '';
  document.getElementById('bultoCountBadge').textContent = rowCount;
}

// ─── Remove row ─────────────────────────────────────────────────
function removeBultoRow(n) {
  const row = document.getElementById(`row_${n}`);
  if (row) {
    row.remove();
    rowCount--;
    updateTotals();
    const remaining = document.querySelectorAll('#bultosBody tr').length;
    document.getElementById('emptyBultos').style.display = remaining ? 'none' : '';
    document.getElementById('totalsRow').style.display   = remaining ? ''     : 'none';
    document.getElementById('bultoCountBadge').textContent = remaining;
  }
}

// ─── Client-side form validation ────────────────────────────────
document.addEventListener('DOMContentLoaded', function () {
  // Populate from existing bultos (edit mode) or form errors
  if (typeof EXISTING_BULTOS !== 'undefined' && EXISTING_BULTOS.length) {
    EXISTING_BULTOS.forEach(b => addBultoRow(b.bulto_num, b));
    rowCount = Math.max(...EXISTING_BULTOS.map(b => b.bulto_num));
  } else if (typeof FORM_DATA !== 'undefined' && Object.keys(FORM_DATA).length) {
    Object.keys(FORM_DATA).sort((a, b) => +a - +b).forEach(n => {
      addBultoRow(+n, FORM_DATA[n]);
    });
  }

  // Form submit validation
  const form = document.getElementById('productForm');
  if (form) {
    form.addEventListener('submit', function (e) {
      const rows = document.querySelectorAll('#bultosBody tr');
      let hasIssue = false;

      rows.forEach(row => {
        const l = parseFloat(row.querySelector('[data-field="largo"]')?.value) || 0;
        const a = parseFloat(row.querySelector('[data-field="ancho"]')?.value) || 0;
        const h = parseFloat(row.querySelector('[data-field="alto"]')?.value) || 0;
        // Warn if partial: some dimensions but not all
        const filled = [l, a, h].filter(v => v > 0).length;
        if (filled > 0 && filled < 3) {
          hasIssue = true;
          row.classList.add('table-danger');
        } else {
          row.classList.remove('table-danger');
        }
      });

      if (hasIssue) {
        e.preventDefault();
        alert('Algunos bultos tienen medidas incompletas (largo, ancho y alto deben estar todos completos o todos en 0). Revisa las filas marcadas en rojo.');
      }
    });
  }
});
