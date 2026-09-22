"use strict";
const $ = (id) => document.getElementById(id);
const fmtTime = (ms) =>
  new Date(ms).toLocaleString("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
const fmtValue = (v) =>
  v == null ? "—" : Number.isInteger(v) ? String(v) : Number(v).toFixed(1);

let plot = null;

function renderCards(cards) {
  for (const [key, c] of Object.entries(cards)) {
    const valueEl = $(`card-${key}`);
    if (!valueEl) continue;
    valueEl.textContent = fmtValue(c.value);
    if (key === "heart_rate" && c.min != null && c.max != null) {
      $(`card-heart_rate-range`).textContent = `${c.min}–${c.max}`;
    }
    const countEl = $(`card-${key}-count`);
    if (countEl) countEl.textContent = `${c.count} 条`;
    const timeEl = $(`card-${key}-time`);
    if (timeEl && c.time_ms) timeEl.textContent = fmtTime(c.time_ms);
  }
}

function renderChart(series) {
  const el = $("heart-rate-chart");
  if (!series || series.length < 2) {
    el.textContent = "暂无足够数据绘制曲线";
    return;
  }
  el.replaceChildren();
  const xs = series.map((p) => p[0]);
  const ys = series.map((p) => p[1]);
  plot = new uPlot(
    {
      width: Math.max(320, el.clientWidth),
      height: 280,
      padding: [8, 10, 0, 0],
      cursor: { drag: { x: true, y: false } },
      scales: {
        x: { time: true },
        y: { range: (u, min, max) => [min - 5, max + 5] },
      },
      axes: [
        {
          values: (u, vals) => vals.map((v) => fmtTime(v)),
        },
        { size: 40 },
      ],
      series: [
        {},
        { label: "心率", stroke: "#347c66", width: 2, points: { show: true } },
      ],
    },
    [xs, ys],
    el
  );
}

function renderRecords(records) {
  const body = $("record-body");
  body.replaceChildren();
  if (!records.length) {
    $("record-summary").textContent = "暂无记录";
    return;
  }
  $("record-summary").textContent = `共 ${records.length} 条记录`;
  for (const r of records) {
    const tr = document.createElement("tr");
    const cells = [fmtTime(r.time_ms), r.metric, `${fmtValue(r.value)} ${r.unit}`, r.source || "—"];
    for (const text of cells) {
      const td = document.createElement("td");
      td.textContent = text;
      tr.append(td);
    }
    body.append(tr);
  }
}

async function load() {
  $("sync-status").textContent = "正在读取记录…";
  try {
    const res = await fetch("/api/dashboard", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    $("sync-status").textContent = `共 ${data.count} 条样本 · 更新于 ${fmtTime(data.generated_at_ms)}`;
    $("dashboard").hidden = false;
    renderCards(data.cards);
    renderChart(data.heart_rate_series);
    renderRecords(data.records);
    $("hr-caption").textContent = data.heart_rate_series.length
      ? `${data.heart_rate_series.length} 个读数`
      : "暂无心率数据";
    $("error").hidden = true;
  } catch (err) {
    $("sync-status").textContent = "";
    $("error").hidden = false;
    $("error").textContent = `读取失败：${err.message}`;
  }
}

load();
setInterval(load, 30000);
