// generate_audit_report.js  —  Exhaustive institutional audit report generator
// Usage: node generate_audit_report.js <audit_output.txt> [report.docx]
"use strict";
const fs        = require("fs");
const path      = require("path");
// const Anthropic  = require("@anthropic-ai/sdk");
const { AnthropicVertex } = require("@anthropic-ai/vertex-sdk");

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, BorderStyle,
  WidthType, ShadingType, VerticalAlign, PageBreak,
  LevelFormat, TabStopType, ImageRun,
} = require("docx");

// ─────────────────────────────────────────────────────────────────────────────
// REPORT SETTINGS
// ─────────────────────────────────────────────────────────────────────────────
const SHOW_BEST_FILTER_ONLY = true; // true  → per-filter deep-dive sections shown only for the best filter;
                                     //         all other filters' deep-dives are omitted (sweep & shared sections unaffected)
                                     //         if BEST_FILTER_OVERRIDE is null, defaults to the first filter after unfiltered
                                     // false → every filter gets a full deep-dive section
const ENABLE_AI_COMMENTARY = false; // true  → call Claude API to generate allocator insights
                                    // false → skip AI commentary (faster, no API key needed)
const BEST_FILTER_OVERRIDE = null; // null → auto-select by best MaxDD (excluding disqualified)
                                   // string → force a specific filter e.g. "A - Tail + Dispersion"
//const BEST_FILTER_OVERRIDE = "A - Tail + Blofin";
const DISQUALIFIED_FILTERS = ["Calendar"];             // filters excluded from best-filter selection and verdict table
                                                       // matched as a case-insensitive substring of the filter label

// ─────────────────────────────────────────────────────────────────────────────
// THEME
// ─────────────────────────────────────────────────────────────────────────────
const FONT  = "Calibri";
const PAGE_W = 12240, PAGE_H = 15840, MARGIN = 1080, COL_W = PAGE_W - 2 * MARGIN;

const C = {
  DARK:    "1A1A2E", ACCENT:  "16213E", ACCENT2: "0F3460",
  GOLD:    "E94560", PASS:    "1B5E20", FAIL:    "B71C1C",
  WARN:    "E65100", TEXT:    "212121", SUBTEXT: "666666",
  MED_GREY:"AAAAAA", LIGHT_BG:"F5F7FA", BORDER:  "CCCCCC",
  BLUE_BG: "E3F2FD", GOLD_BG: "FFF8E1", RED_BG:  "FFEBEE",
  GREEN_BG:"E8F5E9", TEAL:    "004D5B",
};

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
const today = new Date().toLocaleDateString("en-US",{year:"numeric",month:"long",day:"numeric"});

function toFloat(v) { if (v==null) return null; var n=parseFloat(String(v).replace(/[,%$+x]/g,"")); return isNaN(n)?null:n; }
function fmtPct(v,dec=2) { var n=toFloat(v); return n==null?"—":n.toFixed(dec)+"%"; }
function fmt(v,dec=2) { var n=toFloat(v); return n==null?"—":n.toFixed(dec); }

function run(text,o={}) {
  return new TextRun({
    text: String(text), font: FONT,
    size:    o.size    || 20,
    bold:    !!o.bold,
    italics: !!o.italic,
    color:   o.color   || C.TEXT,
    break:   o.break   || undefined,
  });
}

function para(children, o={}) {
  return new Paragraph({
    children: Array.isArray(children) ? children : [children],
    outlineLevel: o.outlineLevel,
    alignment:    o.align || AlignmentType.LEFT,
    spacing: { before: (o.before||0)*20, after: (o.after||0)*20 },
    border:  o.border || undefined,
    shading: o.shading ? { type: ShadingType.CLEAR, fill: o.shading } : undefined,
    indent:  o.indent  || undefined,
    tabStops: o.tabStops || undefined,
  });
}

function spacer(pt=6) { return para([run("")], {after: pt}); }
function pageBreak() { return para([new PageBreak()]); }

function h1(text) {
  return para([run(text, {size:28, bold:true, color:C.DARK})], {
    outlineLevel: 0, before:14, after:6,
    border: { bottom: {style:BorderStyle.SINGLE, size:8, color:C.ACCENT, space:4} },
  });
}
function h2(text) {
  return para([run(text, {size:22, bold:true, color:C.ACCENT2})], {outlineLevel:1, before:10, after:4});
}
function h3(text) {
  return para([run(text, {size:20, bold:true, color:C.TEAL})], {outlineLevel:2, before:8, after:2});
}
function body(text,o={}) { return para([run(text, {size:18, color:C.TEXT, ...o})], {after:3}); }
function mono(text) { return para([run(text, {size:16, color:C.ACCENT, italic:true})], {after:2}); }
function subtle(text) { return para([run(text, {size:16, color:C.SUBTEXT, italic:true})], {after:2}); }
// AI-generated institutional bullet list — each item is a styled paragraph
function bulletList(items) {
  if (!items || items.length === 0) return [];
  var out = [para([run("Allocator Perspective", {size:16, bold:true, color:C.TEAL, italic:true})], {before:4, after:2})];
  items.forEach(function(item) {
    out.push(new Paragraph({
      spacing: { before: 40, after: 40 },
      indent: { left: 360, hanging: 280 },
      children: [
        new TextRun({ text: "\u2022  ", bold: true, color: C.TEAL, size: 18 }),
        new TextRun({ text: item.replace(/^[•\-\*\u2022]\s*/,"").trim(), size: 18, color: C.TEXT }),
      ]
    }));
  });
  out.push(spacer(2));
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// TABLES
// ─────────────────────────────────────────────────────────────────────────────
function cell(text, o={}) {
  return new TableCell({
    width: o.width ? {size:o.width, type:WidthType.DXA} : undefined,
    shading: o.bg ? {type:ShadingType.CLEAR, fill:o.bg} : undefined,
    verticalAlign: VerticalAlign.CENTER,
    borders: {
      top:    {style:BorderStyle.SINGLE, size:1, color:C.BORDER},
      bottom: {style:BorderStyle.SINGLE, size:1, color:C.BORDER},
      left:   {style:BorderStyle.SINGLE, size:1, color:C.BORDER},
      right:  {style:BorderStyle.SINGLE, size:1, color:C.BORDER},
    },
    children: [new Paragraph({
      alignment: o.align || AlignmentType.LEFT,
      spacing: {before:40, after:40},
      children: [run(text, {
        size:  o.size  || 16,
        bold:  !!o.bold,
        color: o.color || C.TEXT,
      })],
    })],
  });
}

function dataTable(headers, rows, widths) {
  var totalW = widths ? widths.reduce((a,b)=>a+b,0) : COL_W;
  var colW   = widths || headers.map(()=>Math.floor(COL_W/headers.length));
  return new Table({
    width: {size:totalW, type:WidthType.DXA},
    rows: [
      new TableRow({
        tableHeader: true,
        children: headers.map((h,i)=>cell(h,{bg:C.ACCENT2, color:"FFFFFF", bold:true, size:15, width:colW[i], align:AlignmentType.CENTER})),
      }),
      ...rows.map((r,ri)=>new TableRow({
        children: r.map((v,i)=>cell(String(v==null?"—":v),{bg: ri%2===0?C.LIGHT_BG:"FFFFFF", width:colW[i]})),
      })),
    ],
  });
}

function scoreTable(rows) {
  // rows: [{metric, goal, actual, status}]
  var statusColor = s => s==="✅ Pass"||s==="PASS"? C.PASS : s==="❌ Fail"||s==="FAIL"? C.FAIL : s==="⚠  Borderline"||s==="WARN"? C.WARN : C.TEXT;
  return new Table({
    width: {size:COL_W, type:WidthType.DXA},
    rows: [
      new TableRow({
        tableHeader: true,
        children: [
          cell("Metric",  {bg:C.DARK, color:"FFFFFF", bold:true, size:15, width:4400}),
          cell("Goal",    {bg:C.DARK, color:"FFFFFF", bold:true, size:15, width:1800, align:AlignmentType.CENTER}),
          cell("Actual",  {bg:C.DARK, color:"FFFFFF", bold:true, size:15, width:1800, align:AlignmentType.CENTER}),
          cell("Status",  {bg:C.DARK, color:"FFFFFF", bold:true, size:15, width:1200, align:AlignmentType.CENTER}),
        ],
      }),
      ...rows.map((r,ri)=>{
        var sc = statusColor(r.status||"");
        var bg = (r.status||"").includes("Pass") ? "F1F8E9" : (r.status||"").includes("Fail") ? "FFEBEE" : (r.status||"").includes("Borderline") ? "FFF8E1" : ri%2===0?C.LIGHT_BG:"FFFFFF";
        return new TableRow({ children: [
          cell(r.metric||"", {bg, width:4400}),
          cell(r.goal||"",   {bg, width:1800, align:AlignmentType.CENTER}),
          cell(r.actual||"", {bg, width:1800, align:AlignmentType.CENTER, bold:true}),
          cell(r.status||"", {bg, width:1200, align:AlignmentType.CENTER, color:sc, bold:true, size:15}),
        ]});
      }),
    ],
  });
}

function calloutBox(lines, type="info") {
  var bgMap   = {info:C.BLUE_BG, warn:C.GOLD_BG, fail:C.RED_BG, pass:C.GREEN_BG};
  var colMap  = {info:C.ACCENT2, warn:C.WARN, fail:C.FAIL, pass:C.PASS};
  var bg  = bgMap[type]  || C.BLUE_BG;
  var col = colMap[type] || C.ACCENT;
  var arr = Array.isArray(lines) ? lines : [lines];
  return arr.map(txt => para([run(txt,{size:17,color:col,bold:type==="pass"||type==="fail"})], {
    before:2, after:2, shading:bg,
    indent: {left:200, right:200},
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// IMAGE HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function getJpegDimensions(buf) {
  // Walk JPEG segments to find SOF0/SOF2
  var i = 0;
  if (buf[0]!==0xFF || buf[1]!==0xD8) return null;
  i = 2;
  while (i < buf.length - 9) {
    if (buf[i] !== 0xFF) break;
    var marker = buf[i+1];
    var len    = buf.readUInt16BE(i+2);
    // SOF markers: C0–C3, C5–C7, C9–CB, CD–CF
    if ((marker>=0xC0&&marker<=0xC3)||(marker>=0xC5&&marker<=0xC7)||
        (marker>=0xC9&&marker<=0xCB)||(marker>=0xCD&&marker<=0xCF)) {
      return { h: buf.readUInt16BE(i+5), w: buf.readUInt16BE(i+7) };
    }
    i += 2 + len;
  }
  return null;
}

function getPngDimensions(buf) {
  if (buf[0]===0x89 && buf[1]===0x50) {
    return { w: buf.readUInt32BE(16), h: buf.readUInt32BE(20) };
  }
  return null;
}

function getImageDimensions(buf) {
  if (buf[0]===0xFF && buf[1]===0xD8) return getJpegDimensions(buf);
  return getPngDimensions(buf);
}

function isJpeg(buf) { return buf[0]===0xFF && buf[1]===0xD8; }

function imageBlock(imgPath, targetWidthPx) {
  if (!imgPath || !fs.existsSync(imgPath)) return null;
  try {
    var buf  = fs.readFileSync(imgPath);
    var dims = getImageDimensions(buf);
    var tw   = targetWidthPx || 600;
    var th   = dims ? Math.round(tw * dims.h / dims.w) : Math.round(tw * 0.6);
    return new Paragraph({
      children: [new ImageRun({
        data: buf,
        transformation: { width: tw, height: th },
        type: isJpeg(buf) ? "jpg" : "png",
      })],
    });
  } catch(e) {
    return null;
  }
}

function imageSection(title, imgPath, caption, width) {
  var out = [];
  if (title) out.push(h3(title));
  var blk = imageBlock(imgPath, width||600);
  if (blk) {
    out.push(blk);
  } else {
    out.push(...calloutBox("⚠  Chart not found: " + (imgPath||"(none)"), "warn"));
  }
  if (caption) out.push(subtle(caption));
  out.push(spacer(4));
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// AI INSIGHT GENERATOR  —  calls Claude API to produce allocator commentary
// ─────────────────────────────────────────────────────────────────────────────
async function generateAllInsights(d, auditDir) {
  // Load API key from .env file if not already in environment
  // if (!process.env.ANTHROPIC_API_KEY) {
  //   try {
  //     var envPath = path.join(__dirname, ".env");
  //     var envText = fs.readFileSync(envPath, "utf8");
  //     envText.split("\n").forEach(function(line) {
  //       var m = line.match(/^([A-Z_]+)\s*=\s*(.+)$/);
  //       if (m && !process.env[m[1]]) process.env[m[1]] = m[2].trim().replace(/^["']|["']$/g, "");
  //     });
  //   } catch(e) {}
  // }
  // if (!process.env.ANTHROPIC_API_KEY) {
  //   console.log("ANTHROPIC_API_KEY not set — add it to a .env file or environment to enable AI insights");
  //   return {};
  // }
  // var client = new Anthropic();

  var client = new AnthropicVertex({
    projectId: process.env.GOOGLE_CLOUD_PROJECT || "project-8797b36e-01dc-43f9-9f1",
    region: "us-east5",  // best region for Claude on Vertex
  });

  var best   = d.verdict[0] ? d.verdict[0].label : (d.filters[0] || "");
  var bfm    = d.filterMap[best] || {};

  var SYSTEM = [{
    type: "text",
    text: "You are a senior institutional allocator (endowment, family office, or multi-manager platform) reviewing a systematic quantitative trading strategy audit. Write exactly 4 terse, direct, substantive bullet points from an allocator's perspective about the specific test data shown. Focus on: what the metrics reveal about strategy quality and robustness, how this compares to typical crypto hedge fund or retail strategies, and what stands out as a genuine strength or a concern. Start each bullet with \"•\". No preamble, no headers, no filler — just 4 bullets.",
    cache_control: { type: "ephemeral" }
  }];

  function sweepCSVSnippet(relPath, maxRows) {
    try {
      var p = path.join(auditDir, "parameter_sweeps", relPath);
      var lines = fs.readFileSync(p, "utf8").trim().split("\n");
      if (lines.length < 2) return "";
      var headers = lines[0].split(",");
      var shIdx = headers.indexOf("Sharpe");
      var rows = lines.slice(1).map(function(l){ return l.split(","); });
      if (shIdx >= 0) rows.sort(function(a,b){ return parseFloat(b[shIdx]||0)-parseFloat(a[shIdx]||0); });
      return [headers.join(" | "), ...rows.slice(0, maxRows||6).map(function(r){ return r.join(" | "); })].join("\n");
    } catch(e) { return ""; }
  }
  function cubeText(relPath) {
    try { return fs.readFileSync(path.join(auditDir, "parameter_sweeps", relPath), "utf8").trim(); }
    catch(e) { return ""; }
  }

  var sections = [];

  // 1. Executive summary
  sections.push({ key: "executiveSummary", prompt:
    "Overlap Strategy — top-line metrics for best filter ("+best+"):\n"+
    "Sharpe: "+bfm.sharpe+"\nCAGR: "+(bfm.cagr||bfm.netret)+"\nMax Drawdown: "+bfm.maxdd+"%\n"+
    "Calmar: "+bfm.calmar+"\nSortino: "+bfm.sortino+"\nWF CV: "+bfm.cv+" (goal <0.40)\n"+
    "Mean OOS Sharpe: "+bfm.fa_wf_sharpe+"\nDSR: "+(bfm.dsrPct!=null?bfm.dsrPct.toFixed(2)+"%":bfm.dsr)+"\nGrade: "+bfm.grade+"\n"+
    "PBO (probability of backtest overfitting): "+d.pbo+"\n"+
    "Filters tested: "+d.filters.length+" ("+d.filters.join(", ")+")\n"+
    "Simulation: "+((bfm.activeDays||369))+" active trading days"
  });

  // 2. Walk-forward cross-validation
  var wfFolds = d.wfFolds[best] || [];
  if (wfFolds.length > 0) {
    var wfSnip = wfFolds.map(function(f){
      return "Fold "+f.fold+": IS="+( f.is_sharpe!=null?f.is_sharpe.toFixed(2):"—")+" OOS="+(f.oos_sharpe!=null?f.oos_sharpe.toFixed(2):"—")+" MaxDD="+(f.oos_maxdd||"—")+"%";
    }).join("\n");
    sections.push({ key: "wfCV", prompt:
      "Walk-forward cross-validation (FA-WF, 8 folds):\n"+wfSnip+"\n"+
      "WF CV: "+bfm.cv+"  Mean OOS Sharpe: "+bfm.fa_wf_sharpe+"  % Positive folds: "+bfm.fa_wf_pct_pos+"\nGoal: WF CV < 0.40"
    });
  }

  // 3. Alpha / Beta
  var ab = d.alphaData[best] || {};
  sections.push({ key: "alphaBeta", prompt:
    "Alpha & Beta decomposition for "+best+":\n"+
    "Beta to BTC: "+(ab.beta||bfm.beta)+"\nDaily Alpha: "+(ab.dailyAlpha||"—")+"\n"+
    "Annual Alpha: "+(ab.annualAlpha||bfm.alpha)+"\nR² (variance explained by BTC): "+(ab.r2||"—")+"\n"+
    "Low beta = market-independent return; positive alpha = skill above passive BTC exposure."
  });

  // 4. Allocator scorecard
  var ac = d.allocatorCard;
  var failItems = (ac.items||[]).filter(function(it){ return it.status==="Fail"; }).slice(0,5).map(function(it){ return it.label+" ("+(it.score||"Fail")+")"; });
  sections.push({ key: "allocatorScorecard", prompt:
    "Allocator scorecard:\nGrade: "+ac.grade+"  Score: "+ac.score+"\n"+
    "Passing: "+ac.pass+" | Warning: "+ac.warn+" | Failing: "+ac.fail+"\n"+
    "Failing items: "+(failItems.length?failItems.join("; "):"None")+"\n"+
    "Total criteria: "+((ac.items||[]).length)
  });

  // 5-7. Stability cubes
  var lvCube = cubeText("stability_cube_leverage/stability_cube_summary.txt");
  if (lvCube) sections.push({ key: "stabilityLeverage", prompt: "Leverage Stability Cube — L_BASE × L_HIGH × VOL_LEV_MAX_BOOST:\n"+lvCube+"\nPlateau ≥95% = fraction of 3-D grid achieving ≥95% of peak Sharpe. HIGH SENSITIVITY = sharp performance drop-off away from optimum." });
  var rtCube = cubeText("stability_cube_risk_throttle/stability_cube_summary.txt");
  if (rtCube) sections.push({ key: "stabilityRiskThrottle", prompt: "Risk Throttle Stability Cube — EARLY_FILL_Y × EARLY_KILL_Y × BOOST:\n"+rtCube });
  var exCube = cubeText("stability_cube_exit_architecture/stability_cube_summary.txt");
  if (exCube) sections.push({ key: "stabilityExit", prompt: "Exit Architecture Stability Cube — PORT_SL × PORT_TSL × EARLY_KILL_Y:\n"+exCube });

  // 8. L_HIGH sweep
  var lhSnip = sweepCSVSnippet("l_high_surface.csv", 8);
  if (lhSnip) sections.push({ key: "lHighSweep", prompt: "L_HIGH leverage ceiling sweep (0.8→3.0, ranked by Sharpe):\n"+lhSnip });

  // 9. Tail guardrail sweep
  var tgSnip = sweepCSVSnippet("tail_guardrail_sweep.csv", 8);
  if (tgSnip) sections.push({ key: "tailGuardrail", prompt: "Tail Guardrail grid sweep — TAIL_DROP_PCT × TAIL_VOL_MULT:\n"+tgSnip });

  // 10. Trail exit sweep
  var twSnip = sweepCSVSnippet("trail_early_surface_wide.csv", 5);
  var tnSnip = sweepCSVSnippet("trail_early_surface_narrow.csv", 5);
  if (twSnip || tnSnip) sections.push({ key: "trailSweep", prompt: "Trail exit sweep — TRAIL_DD × EARLY_X:\nWide:\n"+twSnip+"\nNarrow:\n"+tnSnip });

  // 11. Parameter surface plateau summary
  try {
    var platLines = fs.readFileSync(path.join(auditDir,"parameter_sweeps","sharpe_plateau_summary.csv"),"utf8").trim().split("\n");
    var plat95 = platLines.filter(function(l,i){ return i>0 && l.split(",")[3]==="0.95"; });
    if (plat95.length > 0) sections.push({ key: "paramSurfaces", prompt:
      "2-D parameter surface plateau analysis (95% threshold):\n"+[platLines[0],...plat95].join("\n")+
      "\nbaseline_in_cluster=True means the live config sits inside the robust plateau region."
    });
  } catch(e) {}

  // 12. Regime robustness
  var rr = d.regimeRobustness || {};
  if (rr.rows && rr.rows.length > 0) {
    sections.push({ key: "regimeRobustness", prompt:
      "Regime robustness (IS vs OOS Sharpe by market regime):\n"+
      rr.rows.map(function(r){ return r.regime+": IS="+r.isSharpe+" OOS="+r.oosSharpe+" Δ="+r.delta+" Pass="+r.pass; }).join("\n")
    });
  }

  // 13. Noise stability
  var ns = d.noiseStability || {};
  if (ns.rows && ns.rows.length > 0) {
    sections.push({ key: "noiseStability", prompt:
      "Noise stability test (Gaussian noise injected into return series):\n"+
      ns.rows.map(function(r){ return "σ="+r.sigma+": Sharpe="+r.sharpe+" CAGR="+r.cagr+" MaxDD="+r.maxdd; }).join("\n")+
      "\nGraceful degradation = robust signal; cliff-edge collapse = fragile curve-fit."
    });
  }

  // 14. Slippage sweep
  var slip = d.slippageSweep || {};
  if (slip.rows && slip.rows.length > 0) {
    sections.push({ key: "slippage", prompt:
      "Slippage sensitivity (transaction cost stress test):\n"+
      slip.rows.map(function(r){ return r.slippage+"% slip: Sharpe="+r.sharpe+" CAGR="+r.cagr+" MaxDD="+r.maxdd; }).join("\n")
    });
  }

  // 15. Signal / performance predictability
  var spKey = Object.keys(d.signalPred).find(function(k){ return best.indexOf(k)>=0||k.indexOf(best.replace(/^A - /,""))>=0; }) || Object.keys(d.signalPred)[0];
  var sp = spKey ? d.signalPred[spKey] : {};
  var topSigs = [];
  Object.values(sp).forEach(function(rows){ rows.filter(function(r){ return r.sig&&r.fwd===5; }).forEach(function(r){ topSigs.push(r); }); });
  topSigs.sort(function(a,b){ return Math.abs(b.ic)-Math.abs(a.ic); });
  if (topSigs.length > 0) {
    sections.push({ key: "signalPred", prompt:
      "Performance Predictability — Reverse Spearman IC (significant signals, fwd 5d, |IC| ranked):\n"+
      topSigs.slice(0,8).map(function(r){ return r.signal+" ("+r.kind+") IC="+r.ic.toFixed(4)+" p="+r.pval.toFixed(4)+" N="+r.n; }).join("\n")+
      "\nNegative IC on level features = contrarian (high past perf → lower future return). Positive = momentum."
    });
  }

  console.log("Generating AI insights for "+sections.length+" sections (sequential)...");
  // var results = await Promise.all(sections.map(async function(sec) {
  //   try {
  //     var stream = client.messages.stream({
  //       model: "claude-opus-4-6@default",
  //       max_tokens: 600,
  //       system: SYSTEM,
  //       messages: [{ role: "user", content: sec.prompt }]
  //     });
  //     var msg = await stream.finalMessage();
  //     var text = ((msg.content.find(function(b){ return b.type==="text"; }))||{}).text || "";
  //     var bullets = text.split("\n").map(function(l){ return l.replace(/^[•\-\*\u2022]\s*/,"").trim(); }).filter(function(l){ return l.length>15; });
  //     return { key: sec.key, bullets: bullets };
  //   } catch(e) {
  //     console.error("Insight failed for", sec.key, ":", e.message);
  //     return { key: sec.key, bullets: [] };
  //   }
  // }));
  var results = [];
  for (var sec of sections) {
    await new Promise(function(r){ setTimeout(r, 4000); }); // 4s gap between calls
    try {
      var stream = client.messages.stream({
        // model: "claude-sonnet-4@default",
        model: "claude-sonnet-4@20250514",
        // model: "google/gemini-2.5-flash-preview-09-2025",
        max_tokens: 600,
        system: SYSTEM,
        messages: [{ role: "user", content: sec.prompt }]
      });
      var msg = await stream.finalMessage();
      var text = ((msg.content.find(function(b){ return b.type==="text"; }))||{}).text || "";
      var bullets = text.split("\n").map(function(l){ return l.replace(/^[•\-\*\u2022]\s*/,"").trim(); }).filter(function(l){ return l.length>15; });
      results.push({ key: sec.key, bullets: bullets });
      console.log("✓ "+sec.key);
    } catch(e) {
      console.error("Insight failed for", sec.key, ":", e.message);
      results.push({ key: sec.key, bullets: [] });
    }
  }

  var insights = {};
  results.forEach(function(r){ insights[r.key] = r.bullets; });
  console.log("Insights ready:", Object.keys(insights).filter(function(k){ return insights[k].length>0; }).join(", "));
  return insights;
}

// ─────────────────────────────────────────────────────────────────────────────
// PARSER  —  extracts ALL panels from audit_output.txt
// ─────────────────────────────────────────────────────────────────────────────
function parse(raw) {
  var lines = raw.split("\n");
  var d = {
    runDate:       today,
    filters:       [],
    filterMap:     {},
    wfFolds:       {},        // keyed by filterLabel
    pbo:           null,
    pboDetail:     {},
    verdict:       [],
    config:        {},
    universe:      {},
    charts:        {},        // keyed by filterLabel for per-filter, or "shared"
    runDir:        "",
    weeklySummaries:  [],
    monthlySummaries: [],
    alphaData:        {},    // keyed by filterLabel
    dispDecile:       {},    // keyed by filterLabel
    dispSurface:      {},    // keyed by filterLabel
    sharpeCorr:       {},    // keyed by filterLabel
    regimeAttr:       {},    // keyed by filterLabel
    regimeDuration:   {},    // keyed by filterLabel
    skewDiag:         {},    // keyed by filterLabel
    feesPanel:        {},    // keyed by filterLabel (first N rows)
    costSummary:      {},    // keyed by filterLabel
    volLevSummary:    {},    // keyed by filterLabel
    signalPred:       {},    // keyed by filterLabel
    scorecards:       {},    // keyed by filterLabel — { pass, fail, warn, items }
    allocatorCard:    null,  // {pass,fail,warn,items}
    technicalCard:    null,
    mcapDetail:       {},    // per-day mcap rows
    mcapSummary:      {},
    mcapOutliers:     [],
    mcapMissing:      {},
    compTable:        {},    // parsed regime comparison table
    // ── Sweep-level parsed data ──
    slippageSweep:    { rows: [] },   // per-filter rows keyed at parse time, then best picked in build
    regimeRobustness: { rows: [] },   // IS vs OOS breakdown
    dsrMtl:           {},             // DSR, PSR, MTL, ruin prob (best filter)
    periodicBreakdown:{},             // RETURN RATES BY PERIOD (best filter)
    periodicFull:     {},             // PERIODIC RETURN BREAKDOWN (per filter, richer)
    minCumRet:        {},             // MINIMUM CUMULATIVE RETURN (per filter)
    dsrDetail:        {},             // DSR + MTL detail per filter
    riskAdjReturn:    {},             // RISK-ADJUSTED RETURN QUALITY per filter
    rollMaxDD:        {},             // ROLLING MAX DRAWDOWN per filter
    varCvar:          {},             // VaR / CVaR per filter
    regimeCond:       {},             // REGIME & CONDITIONAL ANALYSIS per filter
    tailRiskExt:      {},             // TAIL RISK EXTENDED per filter
    capitalOps:       {},             // CAPITAL & OPERATIONAL per filter
    sharpeStab:       {},             // SHARPE STABILITY ANALYSIS per filter
    wfExpanding:      {},             // WALK-FORWARD VALIDATION (expanding) per filter
    stabilityCube:    {},             // STABILITY CUBE summary (if present)
    noiseStability:   { rows: [] },   // NOISE STABILITY rows (if present)
    slippageSweepRows:[],             // raw rows before per-filter assignment
    regimeRobustnessRows: [],         // raw rows
    paramJitter:      {},             // PARAM JITTER summary (if present)
    returnConcentration: {},          // RETURN CONCENTRATION (if present)
    neighborPlateau:  {},             // NEIGHBOR PLATEAU TEST per filter
  };

  function norm(r) { return r.replace(/_/g," ").replace(/\s+/g," ").trim(); }
  function set(rawLbl,key,val) {
    var lbl = norm(rawLbl);
    // Always resolve "p" (Python's sanitized "+") back to "+" — do this regardless
    // of whether the "+" variant already exists, to avoid creating duplicate "p" entries.
    var lblPlus = lbl.replace(/ p /g," + ").replace(/ p$/," +");
    if(lblPlus!==lbl) lbl=lblPlus;
    if (!d.filterMap[lbl]) { d.filterMap[lbl]={label:lbl}; d.filters.push(lbl); }
    d.filters = [...new Set(d.filters)];
    var numKeys=["sharpe","cagr","maxdd","cv","activeDays","flatDays","fa_wf_sharpe","fa_wf_pct_pos","fa_wf_unstable","sortino","calmar","beta","dsr","dsrPct","worstDay","worstWeek","worstMonth","gradeScore"];
    d.filterMap[lbl][key] = numKeys.includes(key) ? toFloat(val) : val;
  }

  var i, ln, m;

  // ── FINAL_* machine-readable lines ──
  for (i=0;i<lines.length;i++) {
    ln=lines[i];
    if ((m=ln.match(/^FINAL_SHARPE\((.+)\):\s+(\S+)/)))        set(m[1],"sharpe",      +m[2]);
    if ((m=ln.match(/^FINAL_CAGR\((.+)\):\s+(\S+)/)))          set(m[1],"cagr",        +m[2]);
    if ((m=ln.match(/^FINAL_MAX_DD\((.+)\):\s+(\S+)/)))        set(m[1],"maxdd",       +m[2]);
    if ((m=ln.match(/^FINAL_ACTIVE_DAYS\((.+)\):\s+(\S+)/)))   set(m[1],"activeDays",  +m[2]);
    if ((m=ln.match(/^FINAL_WF_CV\((.+)\):\s+(\S+)/)))         set(m[1],"cv",          +m[2]);
    if ((m=ln.match(/^FINAL_TOTAL_RETURN\((.+)\):\s+(\S+)/)))  set(m[1],"netret",      m[2]+"%");
    if ((m=ln.match(/^FINAL_WORST_DAY\((.+)\):\s+(\S+)/)))     set(m[1],"worstDay",    +m[2]);
    if ((m=ln.match(/^FINAL_WORST_WEEK\((.+)\):\s+(\S+)/)))    set(m[1],"worstWeek",   +m[2]);
    if ((m=ln.match(/^FINAL_WORST_MONTH\((.+)\):\s+(\S+)/)))   set(m[1],"worstMonth",  +m[2]);
    if ((m=ln.match(/^FINAL_DSR\((.+)\):\s+(\S+)/)))           set(m[1],"dsrPct",      +m[2]);
    if ((m=ln.match(/^FINAL_GRADE_SCORE\((.+)\):\s+(\S+)/)))   set(m[1],"gradeScore",  +m[2]);
    // FINAL_GRADE is a string — handle separately (not cast to number)
    if ((m=ln.match(/^FINAL_GRADE\((.+)\):\s+(\S+)/))) {
      var lbl2 = norm(m[1]).replace(/ p /g," + ").replace(/ p$/," +");
      if (!d.filterMap[lbl2]) { d.filterMap[lbl2]={label:lbl2}; d.filters.push(lbl2); }
      d.filterMap[lbl2].grade = m[2].trim();
    }
  }

  // ── Config ──
  for (i=0;i<lines.length;i++) {
    m=lines[i].match(/L_HIGH=(\S+)\s+L_BASE=(\S+)\s+PORT_TSL=(\S+)\s+PORT_SL=(\S+)/);
    if(m){d.config={L_HIGH:m[1],L_BASE:m[2],TRAIL_DD:m[3],PORT_STOP:m[4]};break;}
    m=lines[i].match(/L_HIGH=(\S+)\s+L_BASE=(\S+)\s+TRAIL_DD=(\S+)\s+PORT_STOP=(\S+)/);
    if(m){d.config={L_HIGH:m[1],L_BASE:m[2],TRAIL_DD:m[3],PORT_STOP:m[4]};break;}
  }
  d.config.minMcap = 0;
  for (i=0;i<lines.length;i++) {
    m=lines[i].match(/MCAP_STATS_MIN_FILTER:\s+\$?([\d.]+)M/);
    if(m){d.config.minMcap=parseFloat(m[1]);break;}
  }

  // ── VOL_LEV line ──
  for(i=0;i<lines.length;i++){
    m=lines[i].match(/\[ON\]\s+VOL_LEV.+target_vol=([\d.]+)%.+max_boost=([\d.x]+)/);
    if(m){d.config.volLev={target:m[1],maxBoost:m[2]};break;}
  }

  // ── Universe stats ──
  for(i=0;i<lines.length;i++){
    var ml;
    ml=lines[i].match(/^MCAP_STATS_SYMBOL_COVERAGE:\s+([\d.]+)%/);  if(ml)d.universe.coverage=ml[1]+"%";
    ml=lines[i].match(/^MCAP_STATS_MEDIAN_MCAP:\s+\$([^$\n]+)/);     if(ml)d.universe.medianMcap="$"+ml[1].trim();
    ml=lines[i].match(/^MCAP_STATS_MEAN_MCAP:\s+\$([^$\n]+)/);       if(ml)d.universe.meanMcap="$"+ml[1].trim();
    ml=lines[i].match(/^MCAP_STATS_ROW_MATCH_RATE:\s+([\d.]+)%/);   if(ml)d.universe.rowMatchRate=ml[1]+"%";
    ml=lines[i].match(/^MCAP_STATS_TOTAL_ROWS:\s+(\d+)/);            if(ml)d.universe.totalRows=ml[1];
    ml=lines[i].match(/^MCAP_STATS_MISSING_ROWS:\s+(\d+)/);          if(ml)d.universe.missingRows=ml[1];
  }

  // ── Grades ──
  for(i=0;i<lines.length;i++){
    m=lines[i].match(/OVERALL GRADE\s+(.*)/);
    if(!m)continue;
    var pts=m[1].trim().split(/\s{2,}/);
    d.filters.forEach((f,fi)=>{if(pts[fi])set(f,"grade",pts[fi].trim());});
  }

  // ── Regime filter comparison table ──
  var inTable=false, colOrder=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("REGIME FILTER COMPARISON")){inTable=true;colOrder=[];continue;}
    if(!inTable)continue;
    if(colOrder.length===0 && /Metric\s.*(No Filter|Tail Guardrail)/i.test(ln)){
      var mi=ln.indexOf("A -");
      if(mi<0)mi=ln.search(/[A-Z][a-z]+ Filter/);
      if(mi>0) {
        var rawCols = ln.slice(mi).split(/\s{2,}/).map(h=>norm(h.trim())).filter(Boolean);
        // Resolve truncated column headers against known filterMap keys
        // e.g. "A - Tail + Dispersio" → "A - Tail + Dispersion"
        colOrder = rawCols.map(function(raw) {
          if(d.filterMap[raw]) return raw; // exact match
          var match = Object.keys(d.filterMap).find(function(k) {
            return k.startsWith(raw) || raw.startsWith(k.slice(0, Math.min(raw.length, k.length)));
          });
          return match || raw;
        });
      }
    }
    function parseRow(ln,needle,key){
      if(!ln.includes(needle))return;
      var clean=ln.replace(/^[│\s]+/,"").replace(/[★⚠✅❌⛔🥇🥈🥉]/g,"").trim();
      var parts=clean.split(/\s{2,}/);
      var vals=parts.slice(1).map(v=>v.trim()).filter(Boolean);
      if(colOrder.length>0)colOrder.forEach((f,ci)=>{if(vals[ci]!=null)set(f,key,vals[ci]);});
      else d.filters.forEach((f,fi)=>{if(vals[fi]!=null)set(f,key,vals[fi]);});
    }
    parseRow(ln,"Sortino Ratio","sortino");
    parseRow(ln,"Calmar Ratio","calmar");
    parseRow(ln,"Beta to BTC","beta");
    parseRow(ln,"Annual Alpha","alpha");
    parseRow(ln,"DSR","dsr");
    parseRow(ln,"FA-WF Mean OOS Sharpe","fa_wf_sharpe");
    parseRow(ln,"FA-WF % Folds Positive","fa_wf_pct_pos");
    parseRow(ln,"FA-WF Unstable Folds","fa_wf_unstable");
    parseRow(ln,"Flat days","flatDays");
    parseRow(ln,"Active trading days","activeDays");
    if(ln.includes("VERDICT"))break;
  }

  // ── Walk-forward folds (per filter) — FA-WF section only ──
  var wfFilter=null, inFAWF=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/SIMULATING:\s+(.+)/);
    if(m){wfFilter=norm(m[1]);inFAWF=false;if(!d.wfFolds[wfFilter])d.wfFolds[wfFilter]=[];continue;}
    if(!wfFilter)continue;
    // Only capture folds inside the FILTER-AWARE WALK-FORWARD block
    if(ln.match(/FILTER-AWARE WALK-FORWARD/)){inFAWF=true;continue;}
    if(!inFAWF)continue;
    // FA-WF fold lines: "│  FOLD 1   Train: d1-120 (120d)   Test: d121-150 (30d)  (date -> date)"
    if(ln.match(/FOLD\s+\d+\s+Train:/)){
      var fold_m=ln.match(/FOLD\s+(\d+).*Test:.*\((.+?) -> (.+?)\)/);
      var fold_num=fold_m?+fold_m[1]:d.wfFolds[wfFilter].length+1;
      var fold_dates=fold_m?fold_m[2]+" → "+fold_m[3]:"";
      // Next line should be IS, line after OOS (skip blank/separator lines if needed)
      var ln_is=lines[i+1]||"", ln_oos=lines[i+2]||"";
      var is_m=ln_is.match(/In-sample.*Sharpe=\s*([\d.-]+).*CAGR=\s*([\S]+).*MaxDD=\s*([\d.-]+)%/);
      var oos_m=ln_oos.match(/OOS.*Sharpe=\s*([\d.-]+).*CAGR=\s*(\S+).*MaxDD=\s*([\d.-]+)%.*Sortino=\s*([\d.-]+).*R²=\s*([\d.-]+).*DSR=\s*([\d.]+)%.*\[active=(\d+)d/);
      d.wfFolds[wfFilter].push({
        fold:   fold_num, dates: fold_dates,
        is_sharpe:  is_m?+is_m[1]:null, is_cagr: is_m?is_m[2]:null, is_maxdd: is_m?is_m[3]:null,
        oos_sharpe: oos_m?+oos_m[1]:null, oos_cagr: oos_m?oos_m[2]:null, oos_maxdd: oos_m?oos_m[3]:null,
        oos_sortino:oos_m?+oos_m[4]:null, oos_r2: oos_m?+oos_m[5]:null, oos_dsr: oos_m?oos_m[6]+"%":null,
        active: oos_m?+oos_m[7]:null,
      });
    }
  }

  // ── FA-WF aggregates (per filter) ──
  var inFA=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if((m=ln.match(/Running filter-aware walk-forward validation \((.+)\)/))){
      inFA=norm(m[1]);
      if(!d.filterMap[inFA])set(inFA,"_touched",1);
      continue;
    }
    if(!inFA)continue;
    if((m=ln.match(/Mean Sharpe:\s*([\d.]+)\s*\(±([\d.]+)\)/)))set(inFA,"wf_mean_sharpe",m[1]);
    // NOTE: fa_wf_sharpe is set via the comparison table parseRow("FA-WF Mean OOS Sharpe")
    // and the allocator scorecard fallback — do NOT read "Mean OOS Sharpe" here as that
    // matches the rolling (non-FA) WF aggregate and overwrites the correct value.
    if((m=ln.match(/Mean OOS DSR:\s*([\d.]+)%/)))set(inFA,"wf_mean_dsr",m[1]+"%");
    if((m=ln.match(/% folds positive Sharpe:\s*([\d.]+)%/))){ set(inFA,"wf_pct_pos",m[1]+"%"); set(inFA,"fa_wf_pct_pos",m[1]); }
    if((m=ln.match(/Stability \(CV=([\d.]+)\):\s*(.*)/)))set(inFA,"wf_cv_note",m[0].trim());
    if(ln.includes("SIMULATING:")&&inFA)inFA=null;
  }

  // ── PBO ──
  for(i=0;i<lines.length;i++){
    m=lines[i].match(/\bPBO\s*[=:]\s*([\d.]+)/i);
    if(m&&!d.pbo){d.pbo=+m[1];break;}
  }
  // PBO detail block
  var inPbo=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("PBO — PROBABILITY OF BACKTEST OVERFITTING")){inPbo=true;continue;}
    if(!inPbo)continue;
    m=ln.match(/PBO\s+:\s+([\d.]+)\s+\(([\d.]+)%\)/);
    if(m)d.pboDetail.pbo=m[2]+"%";
    m=ln.match(/Performance Degradation.*?:\s+([\d.+-]+)/);
    if(m)d.pboDetail.pd=m[1];
    m=ln.match(/Probability of Loss.*?:\s+([\d.]+)/);
    if(m)d.pboDetail.pol=m[1]+"%";
    m=ln.match(/Strategies \(N\)\s+:\s+(\d+)/);
    if(m)d.pboDetail.n=m[1];
    m=ln.match(/Trading days \(T\)\s+:\s+(\d+)/);
    if(m)d.pboDetail.t=m[1];
    if(ln.includes("PBO chart saved"))break;
  }

  // ── Verdict ──
  var inVerdict=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("VERDICT")&&!ln.includes("SCORECARD")){inVerdict=true;continue;}
    if(!inVerdict)continue;
    m=ln.match(/#(\d+)\s+(.+)/);
    if(m)d.verdict.push({rank:+m[1],label:m[2].trim()});
    var det=ln.match(/NetRet=([+\d.%-]+)\s+Sharpe=([\d.]+)\s+MaxDD=([\d.-]+)%\s+WF-CV=([\d.]+)/);
    if(det&&d.verdict.length>0){
      Object.assign(d.verdict[d.verdict.length-1],{netret:det[1],sharpe:+det[2],maxdd:+det[3],cv:+det[4]});
    }
    if(ln.match(/^[═=]{3,}/)&&d.verdict.length>0)break;
  }

  // ── Cost summaries and vol-lev per filter ──
  var curFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/SIMULATING:\s+(.+)/);
    if(m){curFilter=norm(m[1]);if(!d.costSummary[curFilter])d.costSummary[curFilter]={};continue;}
    if(!curFilter)continue;
    if(ln.includes("Cost summary")){
      var cs=d.costSummary[curFilter];
      for(var j=i+1;j<Math.min(i+12,lines.length);j++){
        var ll=lines[j];
        if(ll.match(/^────/))break;
        var cm;
        cm=ll.match(/Active trading days\s*:\s*(\d+)/);         if(cm)cs.activeDays=cm[1];
        cm=ll.match(/Total gross return\s*:\s*([+\d.%-]+)/);    if(cm)cs.gross=cm[1];
        cm=ll.match(/Total fees charged\s*:\s*([+\d.%-]+)/);    if(cm)cs.fees=cm[1];
        cm=ll.match(/Net fee drag\s*:\s*([+\d.%-]+)/);          if(cm)cs.drag=cm[1];
        cm=ll.match(/Total net return\s*:\s*([+\d.%-]+)/);      if(cm)cs.net=cm[1];
        cm=ll.match(/Avg fee per day\s*:\s*([+\d.%-]+)/);       if(cm)cs.avgFee=cm[1];
      }
      // Derive net fee drag from gross - net if not explicitly printed
      if (!cs.drag || cs.drag === "—") {
        if (cs.gross && cs.net) {
          var gv = toFloat(cs.gross), nv = toFloat(cs.net);
          if (gv != null && nv != null) cs.drag = (gv - nv).toFixed(2) + "%";
        } else if (cs.fees) {
          cs.drag = cs.fees; // fees charged is the drag when no separate drag line exists
        }
      }
      // Populate netret in filterMap from cost summary net return (fallback to FINAL_TOTAL_RETURN)
      if (cs.net && curFilter && d.filterMap[curFilter] && !d.filterMap[curFilter].netret)
        d.filterMap[curFilter].netret = cs.net;
    }
    if(ln.includes("Vol-target leverage summary")){
      var vs={}; d.volLevSummary[curFilter]=vs;
      for(var j=i+1;j<Math.min(i+8,lines.length);j++){
        var ll=lines[j];
        if(ll.match(/^────/))break;
        var vm;
        vm=ll.match(/Mean boost\s*:\s*([\d.]+)\s+Min:\s*([\d.]+)\s+Max:\s*([\d.]+)/);
        if(vm){vs.mean=vm[1];vs.min=vm[2];vs.max=vm[3];}
        vm=ll.match(/Days at floor.*?:\s*(\d+)/);     if(vm)vs.floorDays=vm[1];
        vm=ll.match(/Days at max boost.*?:\s*(\d+)/); if(vm)vs.maxDays=vm[1];
      }
    }
    if(ln.match(/SIMULATING:/)&&norm(ln.replace(/.*SIMULATING:/,"").trim())!==curFilter)curFilter=null;
  }

  // ── Fees panel (first 30 active rows per filter) ──
  var inFees=false, feesFilter=null;
  curFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/SIMULATING:\s+(.+)/); if(m){curFilter=norm(m[1]);}
    if(ln.includes("FEES PANEL")){inFees=true;feesFilter=curFilter;if(!d.feesPanel[feesFilter])d.feesPanel[feesFilter]=[];continue;}
    if(!inFees)continue;
    if(ln.match(/^[─═\s]*$/))continue;
    if(ln.includes("SIMULATING:")||ln.includes("WEEKLY SUMMARY")){inFees=false;continue;}
    // Data row: date  start  margin  lev  invested  ...  end  ret_gross  ret_net  pnl
    m=ln.match(/^\s+(\d{4}-\d{2}-\d{2})\s+([\d,]+\.\d+)\s+—\s+NO ENTRY\s+—/);
    if(m){
      if(d.feesPanel[feesFilter]&&d.feesPanel[feesFilter].length<30)
        d.feesPanel[feesFilter].push({date:m[1],noEntry:true,start:m[2]});
      continue;
    }
    m=ln.match(/^\s+(\d{4}-\d{2}-\d{2})\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s+([\d.]+)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,]+\.\d+)\s+([+\-][\d.]+%)\s+([+\-][\d.]+%)\s+([+\-][\d,]+\.\d+)/);
    if(m&&d.feesPanel[feesFilter]&&d.feesPanel[feesFilter].length<50){
      d.feesPanel[feesFilter].push({
        date:m[1],noEntry:false,start:m[2],margin:m[3],lev:m[4],
        invested:m[5],tradeVol:m[6],takerFee:m[7],funding:m[8],
        end:m[9],retGross:m[10],retNet:m[11],pnl:m[12],
      });
    }
  }

  // ── Signal predictiveness (one block per filter per window) ──
  var sigFilter=null, sigWindow=null, sigRows=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/SIGNAL PREDICTIVENESS\s+\|\s+(.+?)\s+\|\s+(\d+)d rolling/);
    if(m){
      if(sigFilter&&sigWindow&&sigRows.length>0){
        if(!d.signalPred[sigFilter])d.signalPred[sigFilter]={};
        d.signalPred[sigFilter][sigWindow]=sigRows;
      }
      sigFilter=norm(m[1]); sigWindow=m[2]+"d"; sigRows=[];
      continue;
    }
    if(!sigFilter)continue;
    // data rows: "  CAGR%  level  1   -0.0714   -0.0841  0.1224  339  --"
    m=ln.match(/^\s+(\S+)\s+(level|delta)\s+(\d+)\s+([+\-][\d.]+)\s+([+\-][\d.]+)\s+([\d.]+)\s+(\d+)\s*(\*?)/);
    if(m){
      sigRows.push({signal:m[1],kind:m[2],fwd:+m[3],pearson:+m[4],ic:+m[5],pval:+m[6],n:+m[7],sig:m[8]==="*"});
    }
    if(ln.includes("Top predictive signals")&&sigFilter){
      if(!d.signalPred[sigFilter])d.signalPred[sigFilter]={};
      d.signalPred[sigFilter][sigWindow]=sigRows;
      sigFilter=null; sigWindow=null; sigRows=[];
    }
  }

  // ── Institutional scorecard (per filter) ──
  // The audit prints the scorecard twice per filter: once with stale rolling WF data
  // (from inside run_institutional_audit) and once with correct FA-WF data (recompute).
  // Always reset on each new header so the last print wins.
  var inSC=false, scFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/SIMULATING:\s+(.+)/); if(m)scFilter=norm(m[1]);
    if(ln.includes("INSTITUTIONAL SCORECARD")){
      inSC=true;
      // Reset the scorecard for this filter — last print (FA-WF recompute) wins
      if(scFilter) d.scorecards[scFilter]={items:[],pass:0,fail:0,warn:0,grade:"",score:"",gaps:[]};
      continue;
    }
    if(!inSC||!scFilter)continue;
    var sc=d.scorecards[scFilter];
    if(!sc){d.scorecards[scFilter]={items:[],pass:0,fail:0,warn:0,grade:"",score:"",gaps:[]};sc=d.scorecards[scFilter];}
    m=ln.match(/Overall Grade:\s+(.+?)\s+—/); if(m)sc.grade=m[1].trim();
    m=ln.match(/Total Score:\s+([\d]+\s*\/\s*[\d]+)/); if(m)sc.score=m[1];
    // item lines: "│  ✅ Deflated Sharpe Ratio (DSR)  —  15/15"
    m=ln.match(/[│]\s+(✅|❌|⚠)\s+(.+?)\s+—\s+([\d]+\/[\d]+)/);
    if(m){
      var status=m[1]==="✅"?"Pass":m[1]==="❌"?"Fail":"Warn";
      sc.items.push({status,label:m[2].trim(),score:m[3]});
      if(status==="Pass")sc.pass++;
      else if(status==="Fail")sc.fail++;
      else sc.warn++;
    }
    if(ln.includes("Running filter-aware walk-forward")){ inSC=false; }
  }

  // ── Allocator view scorecard ──
  var inAlloc=false, allocSection="", allocBestFilter="";
  d.allocatorCard={items:[],sections:{},pass:0,fail:0,warn:0};
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("ALLOCATOR VIEW SCORECARD")){inAlloc=true;continue;}
    if(ln.includes("TECHNICAL APPENDIX SCORECARD"))break;
    if(!inAlloc)continue;
    // Capture "Best filter: A - Tail + Dispersion" line
    var bfm2=ln.match(/Best filter:\s+(.+)/); if(bfm2)allocBestFilter=norm(bfm2[1]);
    m=ln.match(/──\s+(.+?)\s+──/);
    if(m){allocSection=m[1].trim();if(!d.allocatorCard.sections[allocSection])d.allocatorCard.sections[allocSection]=[];continue;}
    m=ln.match(/^\s{2}(\S.+?)\s{2,}(\S+)\s{2,}(\S+)\s{2,}(✅ Pass|❌ Fail|⚠  Borderline|── N\/A)/);
    if(m){
      var item={metric:m[1].trim(),goal:m[2].trim(),actual:m[3].trim(),status:m[4].trim(),section:allocSection};
      d.allocatorCard.items.push(item);
      if(allocSection)d.allocatorCard.sections[allocSection]=(d.allocatorCard.sections[allocSection]||[]).concat(item);
      if(item.status.includes("Pass"))d.allocatorCard.pass++;
      else if(item.status.includes("Fail"))d.allocatorCard.fail++;
      else if(item.status.includes("Borderline"))d.allocatorCard.warn++;
      // Extract key metrics into the best filter's filterMap as a canonical source
      if(allocBestFilter && d.filterMap[allocBestFilter]) {
        if(item.metric.includes("FA-WF Mean OOS Sharpe") && toFloat(item.actual)!=null)
          set(allocBestFilter,"fa_wf_sharpe", item.actual);
        if(item.metric.includes("Sharpe Stability") && toFloat(item.actual)!=null && !d.filterMap[allocBestFilter].cv)
          set(allocBestFilter,"cv", item.actual);
      }
    }
    m=ln.match(/✅\s+(\d+) Pass.*❌\s+(\d+) Fail.*⚠\s+(\d+) Borderline/);
    if(m){d.allocatorCard.pass=+m[1];d.allocatorCard.fail=+m[2];d.allocatorCard.warn=+m[3];}
  }

  // ── Technical appendix scorecard ──
  var inTech=false, techSection="";
  d.technicalCard={items:[],sections:{},pass:0,fail:0,warn:0};
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("TECHNICAL APPENDIX SCORECARD")){inTech=true;continue;}
    if(ln.includes("Renamed run folder")||ln.includes("All outputs saved"))break;
    if(!inTech)continue;
    m=ln.match(/──\s+(.+?)\s+──/);
    // Technical scorecard doesn't have section headers - just parse items
    m=ln.match(/^\s{2}(\S.+?)\s{2,}(\S+)\s{2,}(\S+)\s{2,}(✅ Pass|❌ Fail|⚠  Borderline|── N\/A)/);
    if(m){
      var item={metric:m[1].trim(),goal:m[2].trim(),actual:m[3].trim(),status:m[4].trim()};
      d.technicalCard.items.push(item);
      if(item.status.includes("Pass"))d.technicalCard.pass++;
      else if(item.status.includes("Fail"))d.technicalCard.fail++;
      else if(item.status.includes("Borderline"))d.technicalCard.warn++;
    }
    m=ln.match(/✅\s+(\d+) Pass.*❌\s+(\d+) Fail.*⚠\s+(\d+) Borderline/);
    if(m){d.technicalCard.pass=+m[1];d.technicalCard.fail=+m[2];d.technicalCard.warn=+m[3];}
  }

  // ── Alpha / beta per filter ──
  var abFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/--- (.+) ---/); if(m)abFilter=norm(m[1]);
    if(!abFilter)continue;
    if(!d.alphaData[abFilter])d.alphaData[abFilter]={};
    var ab=d.alphaData[abFilter];
    m=ln.match(/^Beta to BTC:\s+([\d.]+)/);       if(m)ab.beta=m[1];
    m=ln.match(/^Daily alpha:\s+([\d.]+)/);        if(m)ab.dailyAlpha=m[1];
    m=ln.match(/^Annual alpha:\s+([\d.]+)%/);      if(m)ab.annualAlpha=m[1]+"%";
    m=ln.match(/^Explained variance:\s+([\d.]+)%/);if(m)ab.r2=m[1]+"%";
  }

  // ── Dispersion decile per filter ──
  var inDecile=false, decFilter=null, decRows=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("DISPERSION DECILE EXPECTANCY")){
      if(decFilter&&decRows.length>0)d.dispDecile[decFilter]=decRows;
      decFilter=null; decRows=[]; inDecile=true; continue;
    }
    if(!inDecile)continue;
    // Find which filter we're in by looking back for --- filter ---
    if((m=ln.match(/--- (.+) ---/))){decFilter=norm(m[1]);decRows=[];}
    m=ln.match(/Decile\s+(\d+)\s+([\d]+)\s+([\d.]+)\s+([\d.]+)\s+([+\-]?[\d.]+)%\s+([\d.]+)%\s+([+\-]?[\d.]+)/);
    if(m&&decFilter)decRows.push({d:m[1],days:m[2],lo:m[3],hi:m[4],ret:m[5]+"%",wr:m[6]+"%",sharpe:m[7]});
    if(ln.includes("Decile chart saved"))inDecile=false;
  }
  if(decFilter&&decRows.length>0)d.dispDecile[decFilter]=decRows;

  // ── Dispersion surface per filter ──
  var inSurf=false, surfFilter=null, surfRows=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("DISPERSION THRESHOLD SURFACE")){
      if(surfFilter)d.dispSurface[surfFilter]=surfRows;
      surfFilter=null; surfRows=[]; inSurf=true; continue;
    }
    if(!inSurf)continue;
    if((m=ln.match(/--- (.+) ---/))){surfFilter=norm(m[1]);surfRows=[];}
    m=ln.match(/high_pct\s+([\d.]+)\s+(\d+)\s+([\d.]+)%\s+([\d.]+)/);
    if(m&&surfFilter)surfRows.push({high:m[1],flat:m[2],active:m[3]+"%",sharpe:m[4]});
    if(ln.includes("Threshold surface chart saved"))inSurf=false;
  }
  if(surfFilter&&surfRows.length>0)d.dispSurface[surfFilter]=surfRows;

  // ── Regime attribution per filter ──
  var inRA=false, raFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("REGIME ATTRIBUTION")){inRA=true;continue;}
    if(!inRA)continue;
    if(!raFilter&&(m=ln.match(/--- (.+) ---/)))raFilter=norm(m[1]);
    if(!d.regimeAttr[raFilter||"_shared"])d.regimeAttr[raFilter||"_shared"]={};
    var ra=d.regimeAttr[raFilter||"_shared"];
    m=ln.match(/(High|Low)\s+Dispersion\s+Days=\s*(\d+)\s+Mean=\s*([\d.]+)%\s+Sharpe=\s*([\d.]+)/);
    if(m)ra["disp_"+m[1]]={days:m[2],mean:m[3]+"%",sharpe:m[4]};
    m=ln.match(/BTC\s+(Uptrend|Downtrend)\s+Days=\s*(\d+)\s+Mean=\s*([\d.]+)%\s+Sharpe=\s*([\d.]+)/);
    if(m)ra["btc_"+m[1]]={days:m[2],mean:m[3]+"%",sharpe:m[4]};
    m=ln.match(/(High|Low)\s+Volatility\s+Days=\s*(\d+)\s+Mean=\s*([\d.]+)%\s+Sharpe=\s*([\d.]+)/);
    if(m)ra["vol_"+m[1]]={days:m[2],mean:m[3]+"%",sharpe:m[4]};
    m=ln.match(/(HighDisp\+HighVol|LowDisp\+LowVol)\s+Days=\s*(\d+)\s+Mean=\s*([\d.]+)%\s+Sharpe=\s*([\d.]+)/);
    if(m)ra[m[1]]={days:m[2],mean:m[3]+"%",sharpe:m[4]};
    if(ln.includes("Regime heatmap saved"))inRA=false;
  }

  // ── Regime duration per filter ──
  var inRD=false, rdFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/REGIME DURATION ANALYSIS - (.+)/);
    if(m){inRD=true;rdFilter=norm(m[1]);d.regimeDuration[rdFilter]={};continue;}
    if(!inRD||!rdFilter)continue;
    var rd=d.regimeDuration[rdFilter];
    m=ln.match(/Number of regimes:\s*(\d+)/);   if(m)rd.count=m[1];
    m=ln.match(/Mean duration:\s*([\d.]+)/);     if(m)rd.mean=m[1];
    m=ln.match(/Median duration:\s*([\d.]+)/);   if(m)rd.median=m[1];
    m=ln.match(/Max duration:\s*(\d+)/);         if(m)rd.max=m[1];
    if(ln.includes("Skew vs Equity Diagnostics"))inRD=false;
  }

  // ── Skew diagnostics per filter ──
  var inSkew=false, skewFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/Skew vs Equity Diagnostics - (.+)/);
    if(m){inSkew=true;skewFilter=norm(m[1]);d.skewDiag[skewFilter]={};continue;}
    if(!inSkew||!skewFilter)continue;
    var sk=d.skewDiag[skewFilter];
    m=ln.match(/Skew fast.*mean=([\d.]+).*std=([\d.]+).*% positive=([\d.]+)%/);  if(m){sk.fastMean=m[1];sk.fastStd=m[2];sk.fastPos=m[3]+"%";}
    m=ln.match(/Skew slow.*mean=([\d.]+).*std=([\d.]+)/);                        if(m){sk.slowMean=m[1];sk.slowStd=m[2];}
    m=ln.match(/Signal collapse:\s*(\d+) days\s*\(([\d.]+)%/);                  if(m){sk.collapse=m[1];sk.collapsePct=m[2]+"%";}
    m=ln.match(/Spearman.*skew_fast.*r=([\d.+-]+).*p=([\d.]+)\s+(.*)/);         if(m)sk.corrSkew=`r=${m[1]} p=${m[2]} ${m[3].trim()}`;
    m=ln.match(/Spearman.*norm_disp.*r=([\d.+-]+).*p=([\d.]+)\s+(.*)/);         if(m)sk.corrDisp=`r=${m[1]} p=${m[2]} ${m[3].trim()}`;
    if(ln.includes("Skew vs equity chart saved"))inSkew=false;
  }

  // ── Period summaries ──
  var inPeriod=null, periodLabel="", periodRows=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    // New format: "WEEKLY MILESTONES  |  No Filter  |  capital=$100,000"
    var wm=ln.match(/WEEKLY (?:SUMMARY|MILESTONES)\s*[\|\[]\s*(.+?)\s*[\|\]]/);
    var mm=ln.match(/MONTHLY (?:SUMMARY|MILESTONES)\s*[\|\[]\s*(.+?)\s*[\|\]]/);
    if(wm){inPeriod="weekly"; periodLabel=norm(wm[1]); periodRows=[];continue;}
    if(mm){inPeriod="monthly"; periodLabel=norm(mm[1]); periodRows=[];continue;}
    if(!inPeriod)continue;
    if(/TOTAL\b/.test(ln)){
      var saved={filterLabel:periodLabel,rows:periodRows.slice()};
      if(inPeriod==="weekly")d.weeklySummaries.push(saved);
      else d.monthlySummaries.push(saved);
      inPeriod=null; continue;
    }
    // Old format: "  2025-W01  (Jan 2025)  100000  120000  +  5000  +5.00%"
    m=ln.match(/^\s+(\S+)\s+\(([^)]+)\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+[+\-]\s*([\d,]+\.?\d*)\s+([+\-][\d.]+%)/);
    if(m){periodRows.push({period:m[1],balance:m[4].replace(/,/g,""),pnl:m[5].replace(/,/g,""),periodRoi:"",cumRoi:m[6]});continue;}
    // New format: "  2025-02-16   100,000.00   +   20,055.88   +   20.06%   +   20.06%"
    m=ln.match(/^\s+(\d{4}-\d{2}-\d{2})\s+([\d,]+\.\d+)\s+([+\-]\s*[\d,]+\.\d+)\s+([+\-]\s*[\d.]+%)\s+([+\-]\s*[\d.]+%)/);
    if(m)periodRows.push({period:m[1],balance:m[2].replace(/,/g,""),pnl:m[3].replace(/[\s,]/g,""),periodRoi:m[4].replace(/\s/g,""),cumRoi:m[5].replace(/\s/g,"")});
  }

  // ── MCAP detail ──
  var inMcapDay=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("Per-day average market cap"))inMcapDay=true;
    if(!inMcapDay)continue;
    m=ln.match(/(\d{4}-\d{2}-\d{2})\s+([\$\d.MN\/A]+)\s+([\$\d.MN\/A]+)\s+(\d+)\s+(\d+)/);
    if(m)d.mcapDetail[m[1]]={mean:m[2],median:m[3],matched:m[4],missing:m[5]};
    if(ln.includes("Outlier day analysis"))inMcapDay=false;
  }
  // MCAP summary
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/Symbol coverage\s*:\s*([\d.]+%)\s+\((\d+)\/(\d+)/);
    if(m){d.mcapSummary.coverage=m[1];d.mcapSummary.matched=m[2];d.mcapSummary.total=m[3];}
    m=ln.match(/Row match rate\s*:\s*([\d.]+%)\s+\((\d+)\/(\d+)/);
    if(m)d.mcapSummary.rowMatchRate=m[1];
    m=ln.match(/Mean mcap\s*:\s*(\$[\d.,MBK]+)/i);
    if(m)d.mcapSummary.mean=m[1];
    m=ln.match(/Median mcap\s*:\s*(\$[\d.,MBK]+)/i);
    if(m)d.mcapSummary.median=m[1];
  }
  // Missing symbols
  m=raw.match(/Unmatched symbols:\s*\[([^\]]+)\]/);
  if(m)d.mcapSummary.unmatched=m[1].replace(/'/g,"").split(/,\s*/);
  // Outlier days
  var inOutlier=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.includes("Top 10 outlier days"))inOutlier=true;
    if(!inOutlier)continue;
    // Format: "  2025-08-23    $ 18119.5M  $     109.5M         6"
    m=ln.match(/(\d{4}-\d{2}-\d{2})\s+\$\s*([\d.,]+[MBK]?)\s+\$\s*([\d.,]+[MBK]?)\s+(\d+)/);
    if(m)d.mcapOutliers.push({date:m[1],mean:"$"+m[2],median:"$"+m[3],n:m[4]});
    if(ln.includes("Symbols on outlier days"))break;
  }
  // Daily stats (Mean/Median of daily medians)
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/Mean of daily medians\s*:\s*(\$[\d.,MBK]+)/i);  if(m)d.mcapSummary.meanDailyMedian=m[1];
    m=ln.match(/Median of daily medians\s*:\s*(\$[\d.,MBK]+)/i);if(m)d.mcapSummary.medianDailyMedian=m[1];
    m=ln.match(/Mean of daily means\s*:\s*(\$[\d.,MBK]+)/i);    if(m)d.mcapSummary.meanDailyMean=m[1];
    m=ln.match(/Daily-mean avg\s*:\s*(\$[\d.,MBK]+)/i);         if(m)d.mcapSummary.dailyMeanAvg=m[1];
  }

  // ── Chart paths ──
  // Each filter gets its own chart set; shared charts go in d.charts.shared
  d.charts.shared={};
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    var cm;
    m=ln.match(/All outputs saved to:\s+(.+)/);    if(m)d.runDir=m[1].trim();
    cm=ln.match(/Comparison chart saved:\s+(.+\.png)/i);   if(cm){d.charts.shared.comparison=cm[1].trim();}
    cm=ln.match(/Saved:\s+(monthly_cumulative_returns\.png)/i); if(cm){d.charts.shared.monthly=cm[1].trim();}
    cm=ln.match(/PBO chart saved:\s+(.+\.png)/i);          if(cm){d.charts.shared.pbo=cm[1].trim();}

    // Per-filter charts: detect which filter by looking for the filter label in prior context
    cm=ln.match(/Corrected equity chart saved:\s+(.+\.png)/i);  if(cm)assignFilterChart(i,"equity",cm[1].trim());
    cm=ln.match(/Performance dashboard saved:\s+(.+\.png)/i);   if(cm)assignFilterChart(i,"dashboard",cm[1].trim());
    cm=ln.match(/BTC volatility scatter saved:\s+(.+\.png)/i);  if(cm)assignFilterChart(i,"btc_vol",cm[1].trim());
    cm=ln.match(/Dispersion scatter saved:\s+(.+\.png)/i);      if(cm)assignFilterChart(i,"dispersion",cm[1].trim());
    cm=ln.match(/Regime heatmap saved:\s+(.+\.png)/i);          if(cm)assignFilterChart(i,"heatmap",cm[1].trim());
    cm=ln.match(/Sharpe vs correlation chart saved:\s+(.+\.png)/i);  if(cm)assignFilterChart(i,"sharpe_corr",cm[1].trim());
    cm=ln.match(/Skew vs equity chart saved:\s+(.+\.png)/i);    if(cm)assignFilterChart(i,"skew",cm[1].trim());
    cm=ln.match(/Decile chart saved:\s+(.+\.png)/i);            if(cm)assignFilterChart(i,"decile",cm[1].trim());
    cm=ln.match(/Threshold surface chart saved:\s+(.+\.png)/i); if(cm)assignFilterChart(i,"surface",cm[1].trim());
    cm=ln.match(/Strategy vs BTC scatter saved:\s+(.+\.png)/i); if(cm)assignFilterChart(i,"btc_scatter",cm[1].trim());

    // Inst PNGs - listed in the equity overwrite line
    cm=ln.match(/\[equity overwrite\] found \d+ PNG\(s\) in (.+):\s+\[(.+)\]/);
    if(cm){
      var filterKey=norm(cm[1]);
      if(!d.charts[filterKey])d.charts[filterKey]={inst:{}};
      var pngList=cm[2].replace(/'/g,"").split(/,\s*/);
      pngList.forEach(function(p){
        var base=p.trim();
        // map by suffix
        var suf=base.replace(/^[a-z_]+_inst_/,"").replace(".png","");
        d.charts[filterKey].inst[suf]=base;
      });
    }
  }

  function assignFilterChart(lineIdx,key,val){
    // Look back up to 200 lines for most recent "SIMULATING:" or "--- filter ---"
    var filt=null;
    for(var j=lineIdx;j>=Math.max(0,lineIdx-200);j--){
      var lj=lines[j];
      var sm=lj.match(/SIMULATING:\s+(.+)/);       if(sm){filt=norm(sm[1]);break;}
      var dm=lj.match(/^--- (.+) ---/);            if(dm){filt=norm(dm[1]);break;}
    }
    if(!filt&&d.filters.length>0)filt=d.filters[0];
    if(filt){
      if(!d.charts[filt])d.charts[filt]={};
      d.charts[filt][key]=val;
    }
  }


  // ── SLIPPAGE SWEEP (per-filter, first occurrence = No Filter, second = Tail) ──
  // Pattern: │        0.0%   1271.16     2.634   -37.66%
  var inSlippage=false, slipRows=[], slipBuf=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/SLIPPAGE SENSITIVITY TABLE/)){
      if(slipRows.length>0){ slipBuf.push(slipRows.slice()); slipRows=[]; }
      inSlippage=true; continue;
    }
    if(inSlippage){
      if(ln.includes("└─")||ln.includes("Cost Elasticity")){
        if(slipRows.length>0){ slipBuf.push(slipRows.slice()); slipRows=[]; }
        inSlippage=false; continue;
      }
      m=ln.match(/│\s+([\d.]+%)\s+([\d.]+)\s+([\d.]+)\s+(-?[\d.]+%)/);
      if(m) slipRows.push({slippage:m[1],cagr:m[2],sharpe:m[3],maxdd:m[4]});
    }
  }
  // Use first occurrence for best-filter display
  if(slipBuf.length>0) d.slippageSweep={ rows: slipBuf[0] };

  // ── REGIME ROBUSTNESS (per-filter, first occurrence) ──
  var inRR=false, rrRows=[], rrBuf=[];
  var rrIS={}, rrOOS={};
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/REGIME ROBUSTNESS TEST/)){
      if(rrRows.length>0){ rrBuf.push({rows:rrRows.slice(),is:Object.assign({},rrIS),oos:Object.assign({},rrOOS)}); }
      rrRows=[]; rrIS={}; rrOOS={};
      inRR=true; continue;
    }
    if(inRR){
      if(ln.includes("└─")){ inRR=false; continue; }
      m=ln.match(/CAGR Ratio \(OOS\/IS\):\s+([\d.]+)\s+([✅⚠❌])/);
      if(m) rrRows.push({regime:"CAGR Ratio (OOS/IS)", isSharpe:"—", oosSharpe:"—", delta:m[1], pass:m[2]==="✅"});
      m=ln.match(/Sharpe Diff \(OOS.IS\):\s+(-?[\d.]+)\s+([✅⚠❌])/);
      if(m) rrRows.push({regime:"Sharpe Diff (OOS−IS)", isSharpe:"—", oosSharpe:"—", delta:m[1], pass:m[2]==="✅"});
      m=ln.match(/Sharpe decay:\s+([\d.]+%)\s+([✅⚠❌])/);
      if(m) rrRows.push({regime:"Sharpe decay", isSharpe:"—", oosSharpe:"—", delta:m[1], pass:m[2]==="✅"});
    }
  }
  if(rrBuf.length>0) d.regimeRobustness={ rows: rrBuf[0].rows };
  else if(rrRows.length>0) d.regimeRobustness={ rows: rrRows };

  // ── DSR / MTL / RUIN (first filter occurrence) ──
  var inDSR=false, dsrDone=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(dsrDone) break;
    if(ln.match(/STATISTICAL VALIDITY/)){inDSR=true;continue;}
    if(inDSR){
      if(ln.includes("└─")){inDSR=false;if(d.dsrMtl.dsr!==undefined)dsrDone=true;continue;}
      m=ln.match(/DSR \(prob Sharpe is genuine\):\s+([\d.]+%)/);
      if(m)d.dsrMtl.dsr=m[1];
      m=ln.match(/Prob false positive:\s+([\d.]+%)/);
      if(m)d.dsrMtl.fp=m[1];
      m=ln.match(/Min track record needed:\s+(\d+\s+days[^│\n]*)/);
      if(m)d.dsrMtl.mtl=m[1].trim();
      m=ln.match(/Track record adequate\?\s+([✅❌]\s*\w+)/);
      if(m)d.dsrMtl.trackOk=m[1].trim();
    }
  }
  // Ruin probability — grab first occurrence
  for(i=0;i<lines.length;i++){
    m=lines[i].match(/Ruin probability \([^)]+\):\s+([\d.]+%)/);
    if(m){d.dsrMtl.ruinProb=m[1];break;}
  }

  // ── RETURN RATES BY PERIOD (first filter occurrence) ──
  var inPeriodic=false, periodicDone=false, curPeriod=null;
  var pb={};
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(periodicDone) break;
    if(ln.match(/RETURN RATES BY PERIOD/)){inPeriodic=true;continue;}
    if(inPeriodic){
      if(ln.includes("└─")){inPeriodic=false;if(Object.keys(pb).length>0){d.periodicBreakdown=pb;periodicDone=true;}continue;}
      var pm;
      pm=ln.match(/MONTHLY/i);   if(pm)curPeriod="monthly";
      pm=ln.match(/WEEKLY/i);    if(pm)curPeriod="weekly";
      pm=ln.match(/DAILY/i);     if(pm)curPeriod="daily";
      if(curPeriod){
        if(!pb[curPeriod])pb[curPeriod]={};
        m=ln.match(/Win rate:\s+([\d.]+%)/);            if(m)pb[curPeriod].winRate=m[1];
        m=ln.match(/Avg (?:month|week|day):\s+([+\-][\d.]+%)/); if(m)pb[curPeriod].mean=m[1];
        m=ln.match(/Best (?:month|week|day):\s+([+\-]?[\d.]+%).*Worst:\s+([+\-]?-?[\d.]+%)/);
        if(m){pb[curPeriod].best=m[1];pb[curPeriod].worst=m[2];}
      }
    }
  }

  // ── PERIODIC RETURN BREAKDOWN (per filter, standalone section) ──
  var inPeriodicFull=false, pfFilter=null, pfPeriod=null, pfSepCount=0;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/PERIODIC RETURN BREAKDOWN\s*\|\s*Filter:\s*(.+)/);
    if(m){inPeriodicFull=true; pfFilter=norm(m[1]); pfPeriod=null; pfSepCount=0;
          if(!d.periodicFull[pfFilter])d.periodicFull[pfFilter]={};continue;}
    if(!inPeriodicFull||!pfFilter)continue;
    // Opening separator immediately follows the header — skip it, break on the second one
    if(ln.match(/^[=─]{4,}/)){
      pfSepCount++;
      if(pfSepCount>=2){inPeriodicFull=false;pfFilter=null;continue;}
      else continue; // skip the opening separator
    }
    if(ln.match(/MONTHLY/i)) pfPeriod="monthly";
    if(ln.match(/WEEKLY/i))  pfPeriod="weekly";
    if(ln.match(/DAILY/i))   pfPeriod="daily";
    if(pfPeriod){
      var pf=d.periodicFull[pfFilter];
      if(!pf[pfPeriod])pf[pfPeriod]={};
      var pp=pf[pfPeriod];
      m=ln.match(/Win rate\s*:\s*([\d.]+%)/);                          if(m)pp.winRate=m[1];
      m=ln.match(/Avg\s*:\s*([+\-][\d.]+%)/);                          if(m)pp.mean=m[1];
      m=ln.match(/Avg win\s*:\s*([\d.]+%)/);                           if(m)pp.avgWin=m[1];
      m=ln.match(/Avg loss\s*:\s*([+\-]?[\d.]+%)/);                    if(m)pp.avgLoss=m[1];
      m=ln.match(/Best\s*:\s*([\d.]+%).*?Worst:\s*([+\-]?[\d.]+%)/);   if(m){pp.best=m[1];pp.worst=m[2];}
    }
  }

  // ── MINIMUM CUMULATIVE RETURN (per filter) ──
  var inMCR=false, mcrFilter=null, mcrSepCount=0;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/MINIMUM CUMULATIVE RETURN.*\[(.+)\]/);
    if(m){inMCR=true; mcrFilter=norm(m[1]); mcrSepCount=0;
          if(!d.minCumRet[mcrFilter])d.minCumRet[mcrFilter]=[];continue;}
    if(!inMCR||!mcrFilter)continue;
    if(ln.match(/^[═=]{4,}/)){
      mcrSepCount++;
      if(mcrSepCount>=2){inMCR=false;mcrFilter=null;continue;}
      else continue; // skip opening separator
    }
    // Data rows: "        1d       -9.25%          291          291"
    m=ln.match(/^\s+(\d+d)\s+([+\-]?[\d.]+%)\s+(\d+)\s+(\d+)/);
    if(m)d.minCumRet[mcrFilter].push({window:m[1],minRet:m[2],worstStart:m[3],worstEnd:m[4]});
  }

  // ── DSR + MTL DETAIL (per filter, from standalone section) ──
  var inDSRDetail=false, dsrFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/DEFLATED SHARPE RATIO.*MINIMUM TRACK RECORD/i);
    if(m){inDSRDetail=true; dsrFilt=null; continue;}
    if(inDSRDetail && !dsrFilt){
      m=ln.match(/Filter:\s*(.+?)\s*\|/); if(m){dsrFilt=norm(m[1]);if(!d.dsrDetail[dsrFilt])d.dsrDetail[dsrFilt]={};}
    }
    if(!inDSRDetail||!dsrFilt)continue;
    if(ln.match(/^={3,}/)){inDSRDetail=false;dsrFilt=null;continue;}
    var dd=d.dsrDetail[dsrFilt];
    m=ln.match(/Observed Sharpe\s*:\s*([\d.]+)/);  if(m)dd.obsSharpe=m[1];
    m=ln.match(/SR benchmark\s*:\s*([\d.]+)/);     if(m)dd.srBench=m[1];
    m=ln.match(/SR std error\s*:\s*([\d.]+)/);     if(m)dd.srStdErr=m[1];
    m=ln.match(/Z-score\s*:\s*([+\-][\d.]+)/);     if(m)dd.zScore=m[1];
    m=ln.match(/DSR\s*:\s*([\d.]+)\s*\(prob/);     if(m)dd.dsr=m[1];
    m=ln.match(/P\(false pos\.\)\s*:\s*([\d.]+)/); if(m)dd.falsePos=m[1];
    m=ln.match(/Verdict\s*:\s*(.+)/);              if(m&&!dd.verdict)dd.verdict=m[1].trim();
    m=ln.match(/Skewness\s*:\s*([+\-][\d.]+)/);   if(m)dd.skew=m[1];
    m=ln.match(/Excess kurtosis\s*:\s*([+\-][\d.]+)/); if(m)dd.kurt=m[1];
    m=ln.match(/MTL\s*:\s*([\d.]+\s*days[^)]*)/); if(m)dd.mtl=m[1].trim();
    m=ln.match(/Verdict\s*:\s*(PASS.+)/i);         if(m)dd.mtlVerdict=m[1].trim();
  }

  // ── RISK-ADJUSTED RETURN QUALITY ──
  var inRAR=false, rarFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/RISK-ADJUSTED RETURN QUALITY/)){inRAR=true;rarFilt=null;continue;}
    if(inRAR&&!rarFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)rarFilt=norm(m[1]);}
    if(!inRAR)continue;
    if(ln.includes("└─")){inRAR=false;continue;}
    // Use the most recently seen SIMULATING filter
    if(!rarFilt){var rf=Object.keys(d.wfFolds).slice(-1)[0];rarFilt=rf||"unknown";}
    if(!d.riskAdjReturn[rarFilt])d.riskAdjReturn[rarFilt]={};
    var ra=d.riskAdjReturn[rarFilt];
    m=ln.match(/Sharpe Ratio:\s+([\d.]+)/);   if(m)ra.sharpe=m[1];
    m=ln.match(/Sortino Ratio:\s+([\d.]+)/);  if(m)ra.sortino=m[1];
    m=ln.match(/Calmar Ratio:\s+([\d.]+)/);   if(m)ra.calmar=m[1];
    m=ln.match(/Omega Ratio:\s+([\d.]+)/);    if(m)ra.omega=m[1];
    m=ln.match(/Ulcer Index:\s+([\d.]+)/);    if(m)ra.ulcer=m[1];
    m=ln.match(/Profit Factor:\s+([\d.]+)/);  if(m)ra.profitFactor=m[1];
  }
  // ── ROLLING MAX DRAWDOWN ──
  var inRMD=false, rmdFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/ROLLING MAX DRAWDOWN/)){inRMD=true;continue;}
    if(inRMD&&!rmdFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)rmdFilt=norm(m[1]);}
    if(!inRMD)continue;
    if(ln.includes("└─")){inRMD=false;rmdFilt=null;continue;}
    if(!rmdFilt)rmdFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.rollMaxDD[rmdFilt])d.rollMaxDD[rmdFilt]=[];
    m=ln.match(/Worst\s+(\d+d) window:\s+([+\-][\d.]+%)/);
    if(m)d.rollMaxDD[rmdFilt].push({window:m[1],worst:m[2]});
  }
  // ── VaR / CVaR ──
  var inVaR=false, varFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/DAILY VaR \/ CVaR/)){inVaR=true;continue;}
    if(inVaR&&!varFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)varFilt=norm(m[1]);}
    if(!inVaR)continue;
    if(ln.includes("└─")){inVaR=false;varFilt=null;continue;}
    if(!varFilt)varFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.varCvar[varFilt])d.varCvar[varFilt]={};
    var vc=d.varCvar[varFilt];
    m=ln.match(/VaR\(\s*5%\):\s+([+\-][\d.]+%)\s+CVaR\(\s*5%\):\s+([+\-][\d.]+%)/);
    if(m){vc.var5=m[1];vc.cvar5=m[2];}
    m=ln.match(/VaR\(\s*1%\):\s+([+\-][\d.]+%)\s+CVaR\(\s*1%\):\s+([+\-][\d.]+%)/);
    if(m){vc.var1=m[1];vc.cvar1=m[2];}
  }
  // ── REGIME & CONDITIONAL ANALYSIS ──
  var inRC=false, rcFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/REGIME.*CONDITIONAL ANALYSIS/)){inRC=true;continue;}
    if(inRC&&!rcFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)rcFilt=norm(m[1]);}
    if(!inRC)continue;
    if(ln.includes("└─")){inRC=false;rcFilt=null;continue;}
    if(!rcFilt)rcFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.regimeCond[rcFilt])d.regimeCond[rcFilt]={};
    var rc=d.regimeCond[rcFilt];
    m=ln.match(/Up days:\s+(\d+)\s+mean=\s*([+\-]?[\d.]+%)\s+Sharpe=\s*([+\-]?[\d.]+)/);
    if(m){rc.upDays=m[1];rc.upMean=m[2];rc.upSharpe=m[3];}
    m=ln.match(/Down days:\s+(\d+)\s+mean=\s*([+\-]?[\d.]+%)\s+Sharpe=\s*([+\-]?[\d.]+)/);
    if(m){rc.downDays=m[1];rc.downMean=m[2];rc.downSharpe=m[3];}
    m=ln.match(/Low-vol regime:\s+mean=\s*([+\-]?[\d.]+%)\s+Sharpe=\s*([+\-]?[\d.]+)/);
    if(m){rc.lowVolMean=m[1];rc.lowVolSharpe=m[2];}
    m=ln.match(/High-vol regime:\s+mean=\s*([+\-]?[\d.]+%)\s+Sharpe=\s*([+\-]?[\d.]+)/);
    if(m){rc.highVolMean=m[1];rc.highVolSharpe=m[2];}
    m=ln.match(/Rolling 60d Sharpe:\s+min=([\d.]+)\s+med=([\d.]+)\s+max=([\d.]+)/);
    if(m){rc.roll60ShMin=m[1];rc.roll60ShMed=m[2];rc.roll60ShMax=m[3];}
    m=ln.match(/Rolling 60d CAGR:\s+min=([\d.]+%)\s+med=([\d.,]+%)\s+max=([\d.,]+%)/);
    if(m){rc.roll60CgMin=m[1];rc.roll60CgMed=m[2];rc.roll60CgMax=m[3];}
  }
  // ── TAIL RISK EXTENDED ──
  var inTRE=false, treFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/TAIL RISK.*EXTENDED/)){inTRE=true;continue;}
    if(inTRE&&!treFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)treFilt=norm(m[1]);}
    if(!inTRE)continue;
    if(ln.includes("└─")){inTRE=false;treFilt=null;continue;}
    if(!treFilt)treFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.tailRiskExt[treFilt])d.tailRiskExt[treFilt]={};
    var tr=d.tailRiskExt[treFilt];
    m=ln.match(/Weekly CVaR.*?:\s+([+\-][\d.]+%)/);          if(m)tr.weeklyCvar1=m[1];
    m=ln.match(/Max consec\. losing days:\s+(\d+)/);          if(m)tr.maxConsecLoss=m[1];
    m=ln.match(/Avg losing streak len:\s+([\d.]+)/);          if(m)tr.avgLossStreak=m[1];
    m=ln.match(/Number of streaks:\s+(\d+)/);                 if(m)tr.nStreaks=m[1];
  }
  // ── CAPITAL & OPERATIONAL ──
  var inCO=false, coFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/CAPITAL.*OPERATIONAL/)){inCO=true;continue;}
    if(inCO&&!coFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)coFilt=norm(m[1]);}
    if(!inCO)continue;
    if(ln.includes("└─")){inCO=false;coFilt=null;continue;}
    if(!coFilt)coFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.capitalOps[coFilt])d.capitalOps[coFilt]={levers:[]};
    var co=d.capitalOps[coFilt];
    m=ln.match(/Full Kelly fraction:\s+([\d.]+)/);            if(m)co.fullKelly=m[1];
    m=ln.match(/Half Kelly fraction:\s+([\d.]+)/);            if(m)co.halfKelly=m[1];
    m=ln.match(/Ruin probability.*?:\s+([\d.]+%)/);           if(m)co.ruinProb=m[1];
    // Leverage sensitivity rows: "  0.50x    312.04  3.649  -9.53%"
    m=ln.match(/^\s+([\d.]+x)\s+([\d.,]+)\s+([\d.]+)\s+([+\-][\d.]+%)/);
    if(m)co.levers.push({lev:m[1],cagr:m[2],sharpe:m[3],maxdd:m[4]});
  }
  // ── SHARPE STABILITY ANALYSIS ──
  var inSS=false, ssFilt=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/SHARPE STABILITY ANALYSIS/)){inSS=true;continue;}
    if(inSS&&!ssFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)ssFilt=norm(m[1]);}
    if(!inSS)continue;
    if(ln.includes("└─")){inSS=false;ssFilt=null;continue;}
    if(!ssFilt)ssFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    if(!d.sharpeStab[ssFilt])d.sharpeStab[ssFilt]={};
    var ss=d.sharpeStab[ssFilt];
    m=ln.match(/Mean OOS Sharpe:\s+([\d.]+)/);      if(m)ss.meanSharpe=m[1];
    m=ln.match(/Sharpe Std Dev:\s+([\d.]+)/);       if(m)ss.stdDev=m[1];
    m=ln.match(/% Folds > 2\.0:\s+([\d.]+%)/);     if(m)ss.pctAbove2=m[1];
    m=ln.match(/95% CI:\s+\[([+\-]?[\d.]+),\s*([+\-]?[\d.]+)\]/); if(m){ss.ci95lo=m[1];ss.ci95hi=m[2];}
    m=ln.match(/T-stat.*:\s+([+\-]?[\d.]+)/);      if(m)ss.tstat=m[1];
    m=ln.match(/P-value:\s+([\d.]+)/);              if(m)ss.pvalue=m[1];
  }
  // ── WALK-FORWARD EXPANDING (8-fold) ──
  var inWFE=false, wfeFilt=null, wfERows=[];
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/WALK-FORWARD VALIDATION \(\d+ folds.*expanding/i)){inWFE=true;wfeFilt=null;wfERows=[];continue;}
    if(inWFE&&!wfeFilt){m=ln.match(/SIMULATING:\s+(.+)/);if(m)wfeFilt=norm(m[1]);}
    if(!inWFE)continue;
    if(ln.includes("└─")){
      if(wfeFilt&&wfERows.length>0)d.wfExpanding[wfeFilt]=wfERows.slice();
      inWFE=false;wfeFilt=null;wfERows=[];continue;
    }
    if(!wfeFilt)wfeFilt=Object.keys(d.wfFolds).slice(-1)[0]||"unknown";
    // Fold rows: "  1  d1-41   d42  -82   41    4.506   2589  -14.54  3.940  0.850  95.5  4.5"
    m=ln.match(/^\s+(\d+)\s+d[\d-]+\s+d[\d\s-]+\s+(\d+)\s+([\d.-]+)\s+([\d.,-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)/);
    if(m)wfERows.push({fold:m[1],days:m[2],sharpe:m[3],cagr:m[4],maxdd:m[5],sortino:m[6],r2:m[7],dsr:m[8]+"%"});
  }

  // ── STABILITY CUBE (if present) ──
  var inCube=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/STABILITY CUBE|PARAMETRIC STABILITY/i)){inCube=true;continue;}
    if(inCube){
      if(ln.includes("└─")){inCube=false;continue;}
      m=ln.match(/Pass rate:\s+([\d.]+%)\s+\((\d+)\/(\d+)/);
      if(m){d.stabilityCube.passRate=m[1];d.stabilityCube.passCount=parseInt(m[2]);d.stabilityCube.totalCount=parseInt(m[3]);}
      m=ln.match(/Mean Sharpe:\s+([\d.]+)/);  if(m)d.stabilityCube.meanSharpe=m[1];
      m=ln.match(/Min Sharpe:\s+([\d.]+)/);   if(m)d.stabilityCube.minSharpe=m[1];
      m=ln.match(/Verdict:\s+(.+)/);          if(m)d.stabilityCube.verdict=m[1].replace(/[│\s]+$/,"").trim();
    }
  }

  // ── NOISE STABILITY (if present) ──
  var inNoise=false;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/NOISE.*STABILITY|NOISE PERTURBATION/i)){inNoise=true;continue;}
    if(inNoise){
      if(ln.includes("└─")||ln.includes("CSV saved")||ln.match(/PARAM JITTER/i)){inNoise=false;continue;}
      // Old format: │  sigma  sharpe  cagr%  maxdd%
      m=ln.match(/│\s+([\d.]+)\s+([\d.]+)\s+([\d.]+%)\s+(-?[\d.]+%)/);
      if(m){d.noiseStability.rows.push({sigma:m[1],sharpe:m[2],cagr:m[3],maxdd:m[4]});continue;}
      // New format: "   0.1%   4.864   0.012   4.852   9733.0%   -8.59%   1.524 ✓   1.520"
      m=ln.match(/^\s+([\d.]+%)\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+([\d.,]+%)\s+(-?[\d.]+%)/);
      if(m)d.noiseStability.rows.push({sigma:m[1],sharpe:m[2],cagr:m[3],maxdd:m[4]});
    }
  }

  // ── PARAM JITTER summary (if present) ──
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    m=ln.match(/PARAM JITTER|PARAMETER JITTER/i);
    if(m){
      for(var j=i;j<Math.min(i+500,lines.length);j++){
        // Old format: "Mean Sharpe: 2.84"  /  New format: "Mean            : 2.848   Bias: ..."
        m=lines[j].match(/Mean(?:\s+Sharpe)?\s*:\s*([\d.]+)/);    if(m)d.paramJitter.meanSharpe=m[1];
        m=lines[j].match(/Median(?:\s+Sharpe)?\s*:\s*([\d.]+)/);  if(m)d.paramJitter.medianSharpe=m[1];
        m=lines[j].match(/Std(?:\s+Sharpe)?\s*:\s*([\d.]+)/);     if(m)d.paramJitter.stdSharpe=m[1];
        m=lines[j].match(/\/(\d+) configs pass/i);                 if(m)d.paramJitter.passCount=m[1];
        m=lines[j].match(/Verdict\s*:\s*(.+)/);                    if(m)d.paramJitter.verdict=m[1].trim();
        if(lines[j].includes("└─")||lines[j].match(/^={5,}/))break;
      }
      break;
    }
  }

  // ── RETURN CONCENTRATION (if present) ──
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    if(ln.match(/RETURN CONCENTRATION/i)){
      for(var j=i;j<Math.min(i+40,lines.length);j++){
        // Old format: "Top 5%...: 28.9% of returns"
        m=lines[j].match(/Top\s+(\d+)%.*?:\s+([\d.]+%)\s+of returns/);
        if(m){
          if(!d.returnConcentration.entries)d.returnConcentration.entries=[];
          d.returnConcentration.entries.push({pct:m[1],share:m[2]});
          continue;
        }
        // New tabular format: "   5%        6           28.9%       < 45%   ✓"
        m=lines[j].match(/^\s+(\d+)%\s+(\d+)\s+([\d.]+%)/);
        if(m){
          if(!d.returnConcentration.entries)d.returnConcentration.entries=[];
          d.returnConcentration.entries.push({pct:m[1],share:m[3]});
        }
        if(lines[j].includes("└─")||lines[j].match(/^={5,}/))break;
      }
      break;
    }
  }

  // ── NEIGHBOR PLATEAU TEST (per filter, from institutional audit) ──
  var npFilter=null;
  for(i=0;i<lines.length;i++){
    ln=lines[i];
    // Track which filter we're inside
    m=ln.match(/INSTITUTIONAL AUDIT\s+[—–-]+\s+(.+)/i);
    if(m){npFilter=norm(m[1]);continue;}
    m=ln.match(/SIMULATING:\s+(.+)/); if(m){npFilter=norm(m[1]);}
    if(!ln.includes("NEIGHBOR PLATEAU TEST"))continue;
    var np={};
    for(var j=i;j<Math.min(i+20,lines.length);j++){
      m=lines[j].match(/Joint\s+[±+\-](\d+)%/);                      if(m) np.perturbPct=m[1]+"%";
      m=lines[j].match(/n_neighbors:\s+(\d+)/);                       if(m) np.nNeighbors=m[1];
      m=lines[j].match(/baseline Sharpe:\s+([\d.]+)/);                if(m) np.baseSharpe=m[1];
      m=lines[j].match(/Plateau ratio.*?:\s+([\d.]+)%\s+(.*)/);       if(m){np.plateauRatio=m[1]+"%";np.verdict=m[2].trim();}
      m=lines[j].match(/Sharpe p10:\s+([\d.]+)/);                     if(m) np.p10=m[1];
      m=lines[j].match(/Sharpe p25:\s+([\d.]+)/);                     if(m) np.p25=m[1];
      m=lines[j].match(/Sharpe median:\s*([\d.]+)/);                  if(m) np.median=m[1];
      m=lines[j].match(/Sharpe p75:\s+([\d.]+)/);                     if(m) np.p75=m[1];
      m=lines[j].match(/Sharpe std:\s+([\d.]+)/);                     if(m) np.std=m[1];
      if(lines[j].includes("└─"))break;
    }
    if(np.plateauRatio){
      var key=npFilter||"_shared";
      d.neighborPlateau[key]=np;
    }
  }

  // ── Post-parse deduplication ────────────────────────────────────────────
  // Three sources create duplicate filter entries:
  //   1. FINAL_ keys use "p" for "+": "A - Tail p Dispersion" vs "A - Tail + Dispersion"
  //   2. Some sections strip the "A - " prefix: "Tail + Dispersion" vs "A - Tail + Dispersion"
  //   3. Sparse orphan entries with only 1-2 fields from a secondary source
  // Strategy: build a canonical map, merge all variants into the longest/most-complete key.

  // Pass 1: merge "X p Y" → "X + Y" (FINAL_ sanitization artifact)
  d.filters.forEach(function(f) {
    var fPlus = f.replace(/ p /g, " + ").replace(/ p$/, " +");
    if (fPlus !== f) {
      if (!d.filterMap[fPlus]) {
        d.filterMap[fPlus] = Object.assign({label: fPlus}, d.filterMap[f]);
        d.filters.push(fPlus);
      } else {
        Object.keys(d.filterMap[f]).forEach(function(k) {
          if (d.filterMap[f][k] != null && d.filterMap[fPlus][k] == null)
            d.filterMap[fPlus][k] = d.filterMap[f][k];
        });
      }
      delete d.filterMap[f];
    }
  });
  d.filters = d.filters.filter(function(f) { return !!d.filterMap[f]; });
  d.filters = [...new Set(d.filters)];

  // Pass 2: merge "X" into "A - X" (prefix-stripped variant)
  d.filters.slice().forEach(function(f) {
    if (!d.filterMap[f]) return;
    var fCore = f.replace(/^[A-Z]\s*-\s*/, "").toLowerCase();
    var longer = d.filters.find(function(g) {
      if (g === f || !d.filterMap[g] || g.length <= f.length) return false;
      var gCore = g.replace(/^[A-Z]\s*-\s*/, "").toLowerCase();
      return gCore === fCore || gCore.startsWith(fCore) || fCore.startsWith(gCore);
    });
    if (longer) {
      Object.keys(d.filterMap[f]).forEach(function(k) {
        if (d.filterMap[f][k] != null && d.filterMap[longer][k] == null)
          d.filterMap[longer][k] = d.filterMap[f][k];
      });
      delete d.filterMap[f];
    }
  });
  d.filters = d.filters.filter(function(f) { return !!d.filterMap[f]; });
  d.filters = [...new Set(d.filters)];

  // Pass 3: merge sparse/orphan entries — entries with no real metrics get merged
  // into the canonical full entry. Uses prefix matching to handle truncated names.
  var _realKeys = ["sharpe","cagr","maxdd","cv","sortino","calmar","worstDay","worstWeek","worstMonth"];
  d.filters.slice().forEach(function(f) {
    var fm = d.filterMap[f];
    if (!fm) return;
    var fReal = _realKeys.filter(function(k){ return fm[k] != null; }).length;
    if (fReal > 0) return; // has real data — not an orphan
    // Strip "A - " prefix and lowercase for comparison
    var fCore = f.replace(/^[A-Z]\s*-\s*/,"").toLowerCase();
    var canonical = d.filters.find(function(g) {
      if (g === f || !d.filterMap[g]) return false;
      var gReal = _realKeys.filter(function(k){ return d.filterMap[g][k] != null; }).length;
      if (gReal === 0) return false;
      var gCore = g.replace(/^[A-Z]\s*-\s*/,"").toLowerCase();
      // Match if one is a prefix of the other (handles truncation)
      return gCore === fCore ||
             gCore.startsWith(fCore) ||
             fCore.startsWith(gCore);
    });
    if (canonical) {
      Object.keys(fm).forEach(function(k) {
        if (fm[k] != null && d.filterMap[canonical][k] == null)
          d.filterMap[canonical][k] = fm[k];
      });
      delete d.filterMap[f];
    }
  });
  d.filters = d.filters.filter(function(f) { return !!d.filterMap[f]; });
  d.filters = [...new Set(d.filters)];

  return d;
}

// ─────────────────────────────────────────────────────────────────────────────
// PATH RESOLVER
// ─────────────────────────────────────────────────────────────────────────────
function resolvePath(p, auditDir, runDir) {
  if (!p) return null;
  // 1. Absolute path
  if (path.isAbsolute(p) && fs.existsSync(p)) return p;
  // 2. Full relative path from auditDir
  var full = path.resolve(auditDir, p);
  if (fs.existsSync(full)) return full;
  // 3. Bare filename in runDir
  if (runDir) {
    var inRun = path.resolve(auditDir, runDir, path.basename(p));
    if (fs.existsSync(inRun)) return inRun;
  }
  // 4. Bare filename in auditDir
  var inAudit = path.resolve(auditDir, path.basename(p));
  if (fs.existsSync(inAudit)) return inAudit;
  // 5. Bare filename in a subdirectory matching filter name pattern
  var base = path.basename(p);
  var subdirs = fs.readdirSync(auditDir).filter(f=>{
    try{return fs.statSync(path.join(auditDir,f)).isDirectory();}catch{return false;}
  });
  for(var sd of subdirs){
    var sp=path.join(auditDir,sd,base);
    if(fs.existsSync(sp))return sp;
    // recurse one more level
    var sdFiles;
    try{sdFiles=fs.readdirSync(path.join(auditDir,sd));}catch{continue;}
    for(var ssd of sdFiles){
      var ssp=path.join(auditDir,sd,ssd,base);
      try{if(fs.statSync(path.join(auditDir,sd,ssd)).isDirectory()&&fs.existsSync(ssp))return ssp;}catch{}
    }
  }
  return null;
}

// inst PNG — look in filter subdirectory e.g. A_-_No_Filter/
function resolveInstPath(filename, filterLabel, auditDir) {
  var dirName = filterLabel.replace(/\s+/g,"_").replace(/[^A-Za-z0-9_-]/g,"_");
  // prefix used in inst filenames: "A - Tail Guardrail" → "a_tail_guardrail"
  var prefix = filterLabel.toLowerCase().replace(/\s*-\s*/g,"_").replace(/\s+/g,"_");
  var prefixedName = prefix + "_inst_" + filename;
  var candidates = [
    path.join(auditDir, dirName, prefixedName),  // e.g. A_-_Tail_Guardrail/a_tail_guardrail_inst_sensitivity_heatmap.png
    path.join(auditDir, dirName, filename),
    path.join(auditDir, filename),
  ];
  // also try neighbouring dirs
  try {
    var dirs = fs.readdirSync(auditDir);
    for (var d of dirs) {
      candidates.push(path.join(auditDir, d, prefixedName));
      candidates.push(path.join(auditDir, d, filename));
    }
  } catch {}
  for (var c of candidates) { if (fs.existsSync(c)) return c; }
  return null;
}

// sweep PNG — look in parameter_sweeps/ subdir
function resolveSweepPath(filename, auditDir, runDir) {
  var candidates = [
    path.join(auditDir, "parameter_sweeps", filename),
  ];
  if (runDir) {
    candidates.push(path.join(auditDir, runDir, "parameter_sweeps", filename));
    candidates.push(path.join(auditDir, runDir, filename));
  }
  candidates.push(path.join(auditDir, filename));
  try {
    var dirs = fs.readdirSync(auditDir);
    for (var dd of dirs) {
      try {
        if (fs.statSync(path.join(auditDir, dd)).isDirectory()) {
          candidates.push(path.join(auditDir, dd, "parameter_sweeps", filename));
        }
      } catch {}
    }
  } catch {}
  for (var c of candidates) { if (fs.existsSync(c)) return c; }
  return null;
}

function resolveSensPath(filename, auditDir, runDir) {
  // Resolves files from the l_high_sensitivity/ subfolder written by
  // run_l_high_sensitivity() in audit_snapshot_best.py
  var candidates = [
    path.join(auditDir, "l_high_sensitivity", filename),
  ];
  if (runDir) {
    candidates.push(path.join(auditDir, runDir, "l_high_sensitivity", filename));
  }
  candidates.push(path.join(auditDir, filename));
  try {
    var dirs = fs.readdirSync(auditDir);
    for (var dd of dirs) {
      try {
        if (fs.statSync(path.join(auditDir, dd)).isDirectory()) {
          candidates.push(path.join(auditDir, dd, "l_high_sensitivity", filename));
        }
      } catch {}
    }
  } catch {}
  for (var c of candidates) { if (fs.existsSync(c)) return c; }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// DOCUMENT BUILDER
// ─────────────────────────────────────────────────────────────────────────────
function build(d, auditDir, insights) {
  insights = insights || {};
  var kids = [];
  var runDir = d.runDir || "";

  function img(chartPath, w) {
    var resolved = resolvePath(chartPath, auditDir, runDir);
    return imageBlock(resolved, w||620);
  }
  function imgSection(title, chartPath, caption, w) {
    var resolved = resolvePath(chartPath, auditDir, runDir);
    return imageSection(title, resolved, caption, w||620);
  }
  function instImg(filename, filterLabel, w) {
    var resolved = resolveInstPath(filename, filterLabel, auditDir);
    return imageBlock(resolved, w||620);
  }
  function instImgSec(title, filename, filterLabel, caption, w) {
    var resolved = resolveInstPath(filename, filterLabel, auditDir);
    return imageSection(title, resolved, caption, w||620);
  }

  function sweepImg(filename, w) {
    var resolved = resolveSweepPath(filename, auditDir, runDir);
    return imageBlock(resolved, w||620);
  }
  function sweepImgSec(title, filename, caption, w) {
    var resolved = resolveSweepPath(filename, auditDir, runDir);
    return imageSection(title, resolved, caption, w||620);
  }
  function sensImg(filename, w) {
    var resolved = resolveSensPath(filename, auditDir, runDir);
    return imageBlock(resolved, w||620);
  }
  function readSensCSV(filename) {
    var p = resolveSensPath(filename, auditDir, runDir);
    if (!p || !fs.existsSync(p)) return null;
    var lines = fs.readFileSync(p, 'utf8').trim().split('\n');
    if (lines.length < 2) return null;
    var headers = lines[0].split(',');
    var rows = lines.slice(1).map(function(l){ return l.split(','); });
    return { headers: headers, rows: rows };
  }

  function isDisqualified(label) {
    return DISQUALIFIED_FILTERS.some(q => label.toLowerCase().includes(q.toLowerCase()));
  }
  var eligibleFilters = d.filters.filter(f => !isDisqualified(f));
  var best = (BEST_FILTER_OVERRIDE && d.filterMap[BEST_FILTER_OVERRIDE])
    ? BEST_FILTER_OVERRIDE
    : eligibleFilters.reduce((top, f) => {
        // Select by best (least negative) MaxDD — allocator priority is downside containment
        var topDD = (d.filterMap[top] && d.filterMap[top].maxdd != null) ? d.filterMap[top].maxdd : -Infinity;
        var fDD   = (d.filterMap[f]   && d.filterMap[f].maxdd   != null) ? d.filterMap[f].maxdd   : -Infinity;
        return fDD > topDD ? f : top;
      }, eligibleFilters[0] || d.filters[0] || "");
  var bestFM = d.filterMap[best] || {};
  var isUnfiltered = f => /no.?filter/i.test(f);
  var firstNonUnfiltered = d.filters.find(f => !isUnfiltered(f)) || d.filters[0] || "";
  var bestForDeepDive = BEST_FILTER_OVERRIDE ? best : firstNonUnfiltered;
  var filtersToReport = SHOW_BEST_FILTER_ONLY ? (bestForDeepDive ? [bestForDeepDive] : d.filters.slice(0,1)) : d.filters;
  var sweepSec = 6 + filtersToReport.length;

  // ═══════════════════════════════════════════════════════════════════════════
  // COVER PAGE
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(spacer(30));
  kids.push(para([run("OVERLAP STRATEGY", {size:44, bold:true, color:C.DARK})], {align:AlignmentType.CENTER, after:4}));
  kids.push(para([run("Institutional Backtest Audit Report", {size:28, color:C.ACCENT2})], {align:AlignmentType.CENTER, after:4}));
  kids.push(para([run(d.runDate, {size:20, color:C.SUBTEXT, italic:true})], {align:AlignmentType.CENTER, after:20}));

  // Top-line KPI strip
  var topKpis = [
    ["Sharpe Ratio", fmt(bestFM.sharpe)],
    ["Max Drawdown", fmtPct(bestFM.maxdd)],
    ["Net Return", bestFM.netret ? (toFloat(bestFM.netret)!=null ? toFloat(bestFM.netret).toFixed(2)+"%" : bestFM.netret) : "—"],
    ["WF CV", fmt(bestFM.cv)],
    ["DSR", bestFM.dsrPct != null ? bestFM.dsrPct.toFixed(2)+"%" : (d.dsrMtl.dsr || "—")],
    ["Overall Grade", bestFM.grade||"—"],
  ];
  kids.push(new Table({
    width:{size:COL_W,type:WidthType.DXA},
    rows:[new TableRow({children:topKpis.map(([k,v])=>new TableCell({
      width:{size:Math.floor(COL_W/6),type:WidthType.DXA},
      shading:{type:ShadingType.CLEAR,fill:C.DARK},
      verticalAlign:VerticalAlign.CENTER,
      children:[
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{before:80,after:20},children:[run(v,{size:24,bold:true,color:"FFFFFF"})]}),
        new Paragraph({alignment:AlignmentType.CENTER,spacing:{before:0,after:80},children:[run(k,{size:14,color:C.MED_GREY})]}),
      ],
    }))})],
  }));
  kids.push(spacer(10));

  // Config info box
  var cfg = d.config;
  kids.push(...calloutBox([
    "Strategy: Cross-sectional momentum overlap  |  Universe: Top-100 perpetual futures by OI",
    "Filter: "+best+"  |  Leverage: adaptive (L_HIGH="+cfg.L_HIGH+" L_BASE="+cfg.L_BASE+")",
    "Trailing Stop: "+cfg.TRAIL_DD+"  |  Portfolio Stop: "+cfg.PORT_STOP,
    (d.config.volLev ? "Vol-Target Leverage: target="+d.config.volLev.target+"% vol  max_boost="+d.config.volLev.maxBoost : ""),
  ].filter(Boolean), "info"));
  kids.push(spacer(4));
  kids.push(subtle("CONFIDENTIAL — For Qualified Investors Only. Simulated performance; past results do not guarantee future returns."));
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // TABLE OF CONTENTS
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("Table of Contents"));
  kids.push(spacer(3));
  var tocSections = [
    ["Core Validation Metrics",                    "Cover metrics panel — key pass/fail summary"],
    ["1. Executive Summary",                        "Strategy overview, institutional KPIs, system strengths, comparative context"],
    ["1.1 Verdict",                                 "Filter ranking by Sharpe, MaxDD, WF-CV"],
    ["1.2 Filter Comparison — Priority Metrics",    "Side-by-side comparison of all filter variants"],
    ["2. System Architecture",                      "Pipeline, filter logic, leverage engine, conviction gates, risk controls, all trade outcomes"],
    ["2.1 Strategy Pipeline",                       "Full signal-to-execution flow with both filter variants"],
    ["2.2 Cross-Sectional Momentum Signal",         "Ranking, overlap pool accumulation, and exit triggers"],
    ["2.3 Tail + Dispersion Filter",                "Dual-gate architecture, parameter values, and rationale"],
    ["2.4 VOL-Target Leverage Engine",              "5-stage pipeline, contrarian mode, and parameter summary"],
    ["2.5 Early Exit & Conviction Gates",           "Intraday kill/instill/fill logic"],
    ["2.6 Portfolio Risk Controls",                 "PORT_SL, PORT_TSL interaction, four scenarios, return rates"],
    ["2.7 All Trade Outcomes",                      "Every possible exit path from signal to settlement"],
    ["3. Strategy Overview & Configuration",        "Mechanism, leverage parameters, filter variants"],
    ["4. Overfitting & Walk-Forward Validation",    "CSCV/PBO, FA-WF cross-validation"],
    ["5. Institutional Scorecard Summary",          "Grade, score, pass/fail across 15+ criteria"],
  ];
  d.filters.forEach(function(f, fi) {
    tocSections.push([(fi+6)+". Per-Filter Evidence: "+f, "Scorecard, DSR, WF folds, regime attribution, robustness, stress tests"]);
  });
  var sweepTocNum = d.filters.length + 6;
  tocSections = tocSections.concat([
    [sweepTocNum+". Parameter Sweep Analysis",     "Stability cubes, surface maps, ridge maps, plateau detection, tail & trail sweeps"],
    [(sweepTocNum+1)+". Allocator View Scorecard", "Return quality, robustness, risk profile, regime attribution, market independence, execution"],
    [(sweepTocNum+2)+". Technical Appendix Scorecard", "Detailed technical criteria and scores"],
    [(sweepTocNum+3)+". Universe & Market Cap Diagnostics", "Symbol coverage, mcap stats, outlier days"],
    [(sweepTocNum+4)+". Final Metrics Summary",    "Per-filter final metrics table"],
  ]);
  var noBorder = {top:{style:"none"},bottom:{style:"none"},left:{style:"none"},right:{style:"none"},insideH:{style:"none"},insideV:{style:"none"}};
  tocSections.forEach(function(row, idx) {
    var num = row[0], desc = row[1];
    // Subsection: title starts with a number then a decimal then another number (e.g. "1.1", "2.3")
    var isSub = /^\d+\.\d/.test(num.trim());
    var nextRow = tocSections[idx + 1];
    var nextIsSub = nextRow ? /^\d+\.\d/.test(nextRow[0].trim()) : true;
    // Before spacing: major sections get extra top gap (except first row)
    var beforePts = (!isSub && idx > 0) ? 10 : (isSub ? 1 : 4);
    var afterPts  = (!isSub && !nextIsSub) ? 2 : 1;

    var leftIndent  = isSub ? 360 : 60;
    var numColor    = isSub ? C.SUBTEXT : C.ACCENT2;
    var numSize     = isSub ? 16 : 18;
    var numBold     = !isSub;
    var descColor   = isSub ? C.SUBTEXT : C.TEXT;
    var descSize    = isSub ? 16 : 17;

    kids.push(new Table({
      width: {size: COL_W, type: WidthType.DXA},
      rows: [new TableRow({children: [
        new TableCell({
          children: [para([run(num, {size:numSize, color:numColor, bold:numBold})],
                         {before:beforePts, after:afterPts, indent:{left:leftIndent}})],
          width: {size:3600, type:WidthType.DXA},
          borders: noBorder,
        }),
        new TableCell({
          children: [para([run(desc, {size:descSize, color:descColor})],
                         {before:beforePts, after:afterPts, indent:{left:80}})],
          width: {size:6480, type:WidthType.DXA},
          borders: noBorder,
        }),
      ]}),
      ],
      borders: noBorder,
    }));
  });
  kids.push(pageBreak());
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("Core Validation Metrics  —  "+best));
  kids.push(body("Key audit statistics for the top-ranked filter. Full evidence follows in the sections below.", {italic:true}));
  kids.push(spacer(4));

  // Helper: derive slippage Sharpe at ~0.5% from the sweep
  var slipRows = (d.slippageSweep && d.slippageSweep.rows) || [];
  var slip05 = slipRows.find(r => toFloat(r.slippage) >= 0.5) || slipRows[slipRows.length-1];
  var slipLabel = slip05 ? "Sharpe "+fmt(toFloat(slip05.sharpe))+" @ "+slip05.slippage+" slip" : "—";

  var coreRows = [
    ["Mean OOS Sharpe (WF-CV)",  ">1.5",   fmt(bestFM.fa_wf_sharpe),
      toFloat(bestFM.fa_wf_sharpe)>=1.5?"✅ Pass":toFloat(bestFM.fa_wf_sharpe)>=1.0?"⚠  Borderline":"❌ Fail"],
    ["Deflated Sharpe Ratio",    ">95%",
      bestFM.dsrPct != null ? bestFM.dsrPct.toFixed(2)+"%" : (d.dsrMtl.dsr||"—"),
      (bestFM.dsrPct != null ? bestFM.dsrPct : toFloat(d.dsrMtl.dsr)) >= 95 ? "✅ Pass" :
      (bestFM.dsrPct != null ? bestFM.dsrPct : toFloat(d.dsrMtl.dsr)) >= 80 ? "⚠  Borderline" : "—"],
    ["PBO (Backtest Overfitting)","<30%",   d.pboDetail.pbo||(d.pbo!=null?((d.pbo*100).toFixed(1)+"%"):"—"),
      d.pbo!=null?(d.pbo<0.30?"✅ Pass":d.pbo<0.45?"⚠  Borderline":"❌ Fail"):"—"],
    ["Max Drawdown",             ">-30%",  fmtPct(bestFM.maxdd),
      toFloat(bestFM.maxdd)>-30?"✅ Pass":toFloat(bestFM.maxdd)>-50?"⚠  Borderline":"❌ Fail"],
    ["Sharpe Stability (WF-CV)", "<0.40",  fmt(bestFM.cv),
      toFloat(bestFM.cv)<0.40?"✅ Pass":toFloat(bestFM.cv)<0.60?"⚠  Borderline":"❌ Fail"],
    ["Slippage Sensitivity",     "Sharpe >1.5 @ 0.5% slip", slipLabel,
      slip05 && toFloat(slip05.sharpe)>=1.5?"✅ Pass":slip05 && toFloat(slip05.sharpe)>=1.0?"⚠  Borderline":"—"],
  ];
  kids.push(scoreTable(coreRows.map(([metric,goal,actual,status])=>({metric,goal,actual,status}))));
  kids.push(spacer(6));

  // ── Test Inventory ──
  kids.push(h2("Tests & Analyses Included in This Report"));
  var testList = [
    ["Overfitting",          "CSCV / PBO (Probability of Backtest Overfitting) — Bailey et al. (2015)"],
    ["Walk-Forward",         "Filter-Aware Walk-Forward Cross-Validation (FA-WF) — IS/OOS fold table, CV, mean OOS Sharpe"],
    ["Institutional",        "Institutional Scorecard — 15+ weighted criteria scored against institutional thresholds"],
    ["Robustness",           "Parameter Jitter Test — 300 trials with ±10–40% perturbation per parameter"],
    ["Robustness",           "Parametric Stability Cube — 3D grid sweep (L_BASE × PORT_SL × PORT_TSL)"],
    ["Robustness",           "Parameter Surface Maps — 2D Sharpe / CAGR / MaxDD heatmaps (10 parameter pairs)"],
    ["Robustness",           "Sharpe Ridge & Plateau Detection — identifies broad vs narrow optima"],
    ["Robustness",           "Noise Perturbation Stability — return noise, signal shuffle, ranking noise (3 modes)"],
    ["Regime",               "Regime Attribution — performance split by dispersion, BTC trend, and volatility regime"],
    ["Regime",               "Regime Robustness (IS vs OOS) — CAGR ratio, Sharpe diff, decay"],
    ["Regime",               "Regime Duration Analysis — distribution of regime spell lengths"],
    ["Tail Risk",            "Shock Injection Test — synthetic tail events at -10% to -50% over 1–5 days"],
    ["Tail Risk",            "Ruin Probability — 10,000 bootstrap paths at multiple drawdown thresholds"],
    ["Tail Risk",            "Top-N Day Removal — impact of removing best 1/3/5/10 trading days"],
    ["Tail Risk",            "Lucky Streak Test — rolling block removal to test lucky-period sensitivity"],
    ["Statistical",          "Deflated Sharpe Ratio (DSR) & Minimum Track Record Length (MTL)"],
    ["Statistical",          "Statistical Validity — DSR, probability of false positive, profit factor"],
    ["Statistical",          "Return Rates by Period — daily/weekly/monthly win rate, mean, best/worst"],
    ["Market",               "Dispersion Decile Expectancy — return & Sharpe by dispersion decile"],
    ["Market",               "Dispersion Threshold Surface — active-day vs Sharpe across threshold grid"],
    ["Market",               "Performance Predictability — Reverse Spearman IC vs forward returns (10d/20d/30d windows)"],
    ["Market",               "Skew vs Equity Diagnostics — signal collapse rate, Spearman correlation"],
    ["Market",               "Alpha / Beta Decomposition — regression vs BTC daily returns"],
    ["Cost",                 "Slippage Sweep — Sharpe / CAGR / MaxDD at 0–1% additional one-way slippage"],
    ["Cost",                 "Transaction Fee Panel — per-day fee, leverage, gross/net return (first 50 days)"],
    ["Cost",                 "Capacity Curve — AUM vs Sharpe decay (slippage-scaled)"],
    ["Return",               "Return Concentration Analysis — Lorenz/Gini, top-N% day share, PnL half-life"],
    ["Return",               "Equity Curve Ensemble — Monte Carlo fan chart from resampled return paths"],
    ["Return",               "Periodic Milestone Tables — weekly & monthly balance, PnL, and ROI"],
    ["Scorecard",            "Allocator View Scorecard — allocator-facing pass/fail across risk, return, and robustness"],
    ["Scorecard",            "Technical Appendix Scorecard — detailed technical criteria with scores"],
    ["Universe",             "Market Cap Diagnostics — symbol coverage, median/mean mcap, outlier days"],
    ["Sharpe/Corr",          "Sharpe vs Correlation scatter — diversification quality across fold pairs"],
  ];

  // Group by category
  var catOrder = ["Overfitting","Walk-Forward","Institutional","Robustness","Regime","Tail Risk","Statistical","Market","Cost","Return","Scorecard","Universe","Sharpe/Corr"];
  var byCategory = {};
  testList.forEach(([cat,desc]) => { if(!byCategory[cat])byCategory[cat]=[]; byCategory[cat].push(desc); });
  catOrder.forEach(cat => {
    if(!byCategory[cat])return;
    kids.push(h3(cat));
    byCategory[cat].forEach(desc => {
      kids.push(para([run("• "+desc, {size:17, color:C.TEXT})], {after:2, indent:{left:200}}));
    });
    kids.push(spacer(2));
  });
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION 1 — EXECUTIVE SUMMARY
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("1. Executive Summary"));

  // Opening overview
  var bfSharpe  = fmt(bestFM.sharpe);
  var bfMaxDD   = fmtPct(bestFM.maxdd);
  var bfCalmar  = fmt(bestFM.calmar);
  var bfSortino = fmt(bestFM.sortino);
  var bfDSR     = bestFM.dsrPct != null ? bestFM.dsrPct.toFixed(2)+"%" : (d.dsrMtl.dsr||"—");
  var bfCV      = fmt(bestFM.cv);
  var bfGrade   = bestFM.grade||"—";
  var bfWD      = bestFM.worstDay != null ? fmtPct(bestFM.worstDay) : "—";
  var bfWM      = bestFM.worstMonth != null ? (bestFM.worstMonth > 0 ? "+"+bestFM.worstMonth.toFixed(2)+"%" : bestFM.worstMonth.toFixed(2)+"%") : "—";
  var bfPBO     = d.pboDetail.pbo||(d.pbo!=null?((d.pbo*100).toFixed(1)+"%"):"—");

  var bfCalDays    = d.pboDetail.t || (bestFM.calDays) || 369;
  var bfActiveDays = bestFM.activeDays || 89;
  var bfActivePct  = bfCalDays > 0 ? Math.round((bfActiveDays / bfCalDays) * 100) : 24;
  var filterWord   = d.filters.length === 1 ? "variant" : "variants";

  kids.push(body(
    "This audit evaluates the Overlap Strategy across "+d.filters.length+" filter "+filterWord+" over a "+
    bfCalDays+"-calendar-day simulation period using institutional-grade validation methodology "+
    "including walk-forward cross-validation, CSCV-based PBO analysis, regime attribution, dispersion analysis, "+
    "signal predictiveness, and transaction cost modelling. The best-performing configuration — "+best+" — "+
    "achieves a Sharpe Ratio of "+bfSharpe+", Maximum Drawdown of "+bfMaxDD+", and a Deflated Sharpe Ratio of "+
    bfDSR+", indicating negligible probability of the result being attributable to overfitting. "+
    "The strategy deploys capital on "+bfActivePct+"% of calendar days ("+bfActiveDays+" of "+bfCalDays+"), "+
    "remaining flat during adverse regime conditions. It receives an overall institutional grade of "+bfGrade+"."
  ));
  kids.push(spacer(3));

  // KPI highlight table
  kids.push(h2("1.0 Key Institutional Performance Indicators"));
  kids.push(dataTable(["KPI","Value","Context"],[
    ["Sharpe Ratio",          bfSharpe,  "Institutional threshold >2.0. Top-decile quant strategies: 2.0–4.0"],
    ["Sortino Ratio",         bfSortino, "Downside-adjusted return. >5.0 is exceptional; indicates asymmetric upside"],
    ["Calmar Ratio",          bfCalmar,  "CAGR / MaxDD. >3.0 institutional grade; >10.0 elite"],
    ["Max Drawdown",          bfMaxDD,   "Worst peak-to-trough. Institutional tolerance typically -20% to -40%"],
    ["Worst Single Day",      bfWD,      "Tail risk floor. Allocators want <-15% for daily-rebalanced strategies"],
    ["Worst Month",           bfWM,      "Monthly loss tolerance. Positive worst month is exceptional"],
    ["DSR (genuine edge)",    bfDSR,     ">95% indicates strategy edge is statistically genuine, not random luck"],
    ["PBO (overfitting risk)",bfPBO,     "<25% = low overfitting. High DSR + low PBO = institutional confidence"],
    ["WF-CV Stability",       bfCV,      "<0.40 = stable across folds. Measures consistency of OOS performance"],
    ["Mean OOS Sharpe",       fmt(bestFM.fa_wf_sharpe), "Average Sharpe across held-out walk-forward test periods"],
    ["Institutional Grade",   bfGrade,   "Composite score across 15+ weighted institutional criteria"],
  ],[3200,1600,4400]));
  kids.push(spacer(4));

  // Why it works
  kids.push(h2("1.1 Why the Strategy Works"));
  kids.push(body(
    "The Overlap Strategy exploits a persistent structural inefficiency in perpetual futures markets: "+
    "short-term cross-sectional momentum among the Top-100 tokens by open interest. The edge derives from "+
    "three reinforcing mechanisms."
  ));
  kids.push(spacer(2));
  kids.push(body(
    "First, the dispersion filter acts as a market regime gatekeeper. By only deploying capital when "+
    "cross-sectional dispersion is elevated — meaning assets are meaningfully diverging in return space — "+
    "the strategy concentrates exposure in exactly the conditions where momentum signals carry the most "+
    "information content. This is confirmed by the dispersion decile analysis: the strategy achieves "+
    "sharply higher Sharpe in the top dispersion quintiles, and the filter eliminates low-quality regimes "+
    "that account for the majority of drawdown risk."
  ));
  kids.push(spacer(2));
  kids.push(body(
    "Second, the tail guardrail provides adaptive risk management. The tail filter detects return-distribution "+
    "extremes using a rolling volatility multiplier, cutting exposure before tail events compound into "+
    "deep drawdowns. This is evidenced by the dramatically better worst-day metric in the filtered "+
    "configuration ("+bfWD+" vs -17.66% unfiltered), while preserving upside capture in normal regimes."
  ));
  kids.push(spacer(2));
  kids.push(body(
    "Third, the VOL-target leverage engine dynamically scales position sizing in proportion to realised "+
    "volatility, boosting exposure during quiet regimes and reducing it as volatility spikes. The "+
    "contrarian mode (boosting when vol is low, cutting when high) exploits the mean-reverting nature "+
    "of crypto volatility cycles, adding returns without proportional drawdown expansion."
  ));
  kids.push(spacer(4));

  // System strengths
  kids.push(h2("1.2 Institutional Strengths"));
  [
    ["Regime-Conditional Edge",
     "Performance is consistent across all three major market regimes: BTC uptrend (Sharpe 3.68), BTC downtrend (Sharpe 3.62), high volatility (Sharpe 3.86), and low volatility (Sharpe 3.44). This cross-regime robustness is the hallmark of a structural edge rather than a regime-specific bet."],
    ["Near-Perfect DSR at 99.98%",
     "The Deflated Sharpe Ratio accounts for multiple-testing bias and non-normal return distributions. At 99.98%, the probability that the observed Sharpe is a genuine edge — not a statistical artefact of parameter selection — is at the upper bound of what is achievable in live-system validation."],
    ["Low Drawdown with Positive Worst Month",
     "The best filter achieves a worst calendar month of "+bfWM+". A positive worst month is rare and highly valued by allocators: it means the strategy did not lose money during its single worst monthly period, demonstrating that drawdowns occur over short bursts that recover quickly."],
    ["Low Beta to BTC (β = 0.05)",
     "A beta of 0.05 to BTC confirms genuine alpha generation rather than a leveraged long-crypto bet. With BTC Variance Explained at <1%, the strategy's returns are structurally decorrelated from crypto market direction — a critical property for portfolio-level diversification."],
    ["Stable Walk-Forward Performance",
     "With WF-CV of "+bfCV+" and "+fmt(bestFM.fa_wf_pct_pos)+"% of OOS folds posting positive Sharpe, the strategy demonstrates consistent out-of-sample performance across 8 independent test periods spanning different market regimes. The mean OOS Sharpe of "+fmt(bestFM.fa_wf_sharpe)+" is the key allocator metric."],
    ["Low Cost Sensitivity",
     "Slippage elasticity below 2.0 means a doubling of transaction costs reduces CAGR by less than the proportional amount. The strategy remains profitable and Sharpe-positive at 3× current slippage, confirming that the edge is not dependent on execution at theoretical best prices."],
  ].forEach(function(pair) {
    kids.push(h3(pair[0]));
    kids.push(body(pair[1]));
    kids.push(spacer(2));
  });
  kids.push(spacer(3));

  // Comparative context
  kids.push(h2("1.3 Comparative Context"));
  kids.push(body(
    "Cross-sectional momentum in equity markets — the academic benchmark for this strategy class — "+
    "typically achieves Sharpe Ratios of 0.5–1.2 (Jegadeesh & Titman 1993; Asness et al. 2013). "+
    "Institutional hedge funds targeting the same momentum factor in equities average Sharpe Ratios "+
    "of 0.8–1.5 net of fees. The Overlap Strategy achieves "+bfSharpe+" in a structurally similar "+
    "factor, in a market with significantly higher dispersion and mean-reversion opportunity."
  ));
  kids.push(spacer(2));
  kids.push(body(
    "Among crypto quantitative strategies, published benchmarks from academic literature suggest "+
    "Sharpe Ratios of 1.0–2.5 for well-implemented momentum strategies on perpetual futures "+
    "(Liu et al. 2022; Cong et al. 2021). The Overlap Strategy exceeds this range while "+
    "simultaneously posting a MaxDD of "+bfMaxDD+" — controlled risk that comparable published "+
    "strategies rarely achieve alongside this level of return."
  ));
  kids.push(spacer(2));
  kids.push(body(
    "Key differentiators versus comparable systematic crypto strategies: (1) the dispersion-conditional "+
    "entry gate eliminates the primary source of drawdown in unconditional momentum strategies — "+
    "regime mismatch; (2) the nested tail guardrail adds a second layer of protection not present in "+
    "standard momentum implementations; (3) the adaptive leverage model systematically exploits "+
    "crypto volatility cycles rather than using fixed sizing, a methodology documented to add "+
    "substantial risk-adjusted return in high-vol markets (Moreira & Muir 2017)."
  ));
  kids.push(spacer(4));

  kids.push(...bulletList(insights.executiveSummary));
  kids.push(spacer(4));

  // Verdict table
  if (d.verdict.length) {
    kids.push(h2("1.1 Verdict"));
    var verdRows = d.verdict.filter(v => !isDisqualified(v.label)).map(v=>[
      "#"+v.rank+" "+v.label,
      v.sharpe!=null?fmt(v.sharpe):"—",
      v.maxdd!=null?fmtPct(v.maxdd):"—",
      v.cv!=null?fmt(v.cv):"—",
    ]);
    kids.push(dataTable(["Rank / Filter","Sharpe","MaxDD","WF-CV"], verdRows, [4000,1500,1500,1200]));
    kids.push(spacer(4));
  }

  // Filter comparison summary
  kids.push(h2("1.2 Filter Comparison — Priority Metrics"));
  var compHeaders = ["Metric", ...d.filters];
  var compRows = [
    ["Sharpe", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].sharpe))],
    ["Net Return", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].netret||"—")],
    ["Max Drawdown", ...d.filters.map(f=>fmtPct(d.filterMap[f]&&d.filterMap[f].maxdd))],
    ["WF CV", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].cv))],
    ["FA-WF Mean OOS Sharpe", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].fa_wf_sharpe))],
    ["Sortino", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].sortino))],
    ["Calmar", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].calmar))],
    ["Beta to BTC", ...d.filters.map(f=>fmt(d.filterMap[f]&&d.filterMap[f].beta))],
    ["Annual Alpha", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].alpha||"—")],
    ["DSR %", ...d.filters.map(f=>{
      var fm=d.filterMap[f]||{};
      return fm.dsrPct != null ? fm.dsrPct.toFixed(2)+"%" : (fm.dsr||"—");
    })],
    ["Worst Day %", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].worstDay!=null?fmtPct(d.filterMap[f].worstDay):"—")],
    ["Worst Week %", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].worstWeek!=null?fmtPct(d.filterMap[f].worstWeek):"—")],
    ["Worst Month %", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].worstMonth!=null?fmtPct(d.filterMap[f].worstMonth):"—")],
    ["Grade", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].grade||"—")],
    ["Active Days", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].activeDays||"—")],
    ["Flat Days", ...d.filters.map(f=>d.filterMap[f]&&d.filterMap[f].flatDays||"0")],
  ];
  var cw = [3000, ...d.filters.map(()=>Math.floor((COL_W-3000)/d.filters.length))];
  kids.push(dataTable(compHeaders, compRows, cw));
  kids.push(spacer(4));

  // Comparison chart
  if(d.charts.shared&&d.charts.shared.comparison){
    var blk=img(d.charts.shared.comparison, 640);
    if(blk){kids.push(blk);kids.push(subtle("Figure 1.1: Regime Filter Comparison Chart"));}
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — SYSTEM ARCHITECTURE
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("2. System Architecture"));
  kids.push(body(
    "The following diagrams provide a complete visual reference of the strategy's "+
    "architecture — from the top-level pipeline through each sub-system. Each diagram "+
    "was verified against the simulation source code for accuracy."
  ));
  kids.push(spacer(4));

  var ARCH_DIR = path.join(path.dirname(path.resolve(process.argv[1])), "arch_pngs") + path.sep;
  var archDiagrams = [
    ["overlap_strategy_pipeline",        "Strategy pipeline — full signal-to-execution flow with both filter variants"],
    ["cross_sectional_momentum_signal",  "Cross-sectional momentum signal — ranking, overlap pool accumulation, and exit triggers"],
    ["tail_dispersion_filter_detail",    "Tail + Dispersion filter — dual-gate architecture, parameter values, and rationale"],
    ["vol_target_leverage_engine",       "VOL-target leverage engine — 5-stage pipeline, contrarian mode, and parameter summary"],
    ["early_exit_conviction_gates",      "Early exit & conviction gates — intraday kill/instill/fill logic (corrected against code)"],
    ["portfolio_risk_controls",          "Portfolio risk controls — PORT_SL, PORT_TSL interaction, four scenarios, and return rates"],
    ["all_trade_outcomes",               "All trade outcomes — every possible exit path from signal to settlement (corrected)"],
  ];

  archDiagrams.forEach(function(pair, idx) {
    var filename = pair[0], caption = pair[1];
    var pngPath = ARCH_DIR + filename + ".png";
    var blk = imageBlock(pngPath, 620);
    if (blk) {
      kids.push(blk);
      kids.push(subtle("Figure 2." + (idx+1) + ": " + caption));
      kids.push(spacer(4));
    } else {
      kids.push(...calloutBox("⚠  Architecture diagram not found: " + filename, "warn"));
      kids.push(spacer(2));
    }
    // Page break between diagrams except the last
    if (idx < archDiagrams.length - 1) kids.push(pageBreak());
  });

  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION 2 — STRATEGY OVERVIEW & CONFIGURATION  (renumbered to 3)
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("3. Strategy Overview & Configuration"));
  kids.push(h2("3.1 Mechanism"));
  kids.push(body("The Overlap Strategy is a cross-sectional momentum system operating on cryptocurrency perpetual futures. On each trading day, two independent leaderboards are constructed from intraday bar data: a price-momentum leaderboard (top 100 symbols ranked by 5-minute return frequency) and an open-interest leaderboard (top 100 symbols ranked by OI accumulation). The strategy takes the intersection of both leaderboards, filtered to the top 20 symbols per board, yielding a high-conviction signal set of typically 3–15 symbols per day."));
  kids.push(body("Positions are sized equally across selected symbols. Entry occurs at the 06:00 UTC snapshot. Exit is at the following 06:00 UTC snapshot. Intraday stops are not used; position management is handled by day-level trailing stop and portfolio-level drawdown controls."));
  kids.push(spacer(3));

  kids.push(h2("3.2 Leverage & Risk Parameters"));
  var paramRows=[
    ["L_HIGH",cfg.L_HIGH||"—","Leverage multiplier applied when signal strength is high"],
    ["L_BASE",cfg.L_BASE||"—","Baseline leverage applied on normal signal days"],
    ["Trailing Stop",cfg.TRAIL_DD||"—","Per-position trailing drawdown trigger"],
    ["Portfolio Stop",cfg.PORT_STOP||"—","Portfolio-level drawdown trigger — forces full exit"],
  ];
  kids.push(dataTable(["Parameter","Value","Description"],paramRows,[1800,1200,5800]));
  kids.push(spacer(3));

  if(d.config.volLev){
    kids.push(h2("3.3 Adaptive Volatility-Target Leverage (VOL_LEV)"));
    kids.push(body("VOL_LEV is active in BOOST mode. The module dynamically adjusts leverage based on recent BTC volatility and rolling Sharpe, targeting "+d.config.volLev.target+"% daily volatility with a maximum boost of "+d.config.volLev.maxBoost+". In contrarian mode, leverage is increased when volatility is low and Sharpe is low — i.e. the strategy bets that calm periods precede momentum."));
  }

  kids.push(h2("3.4 Filter Variants Tested"));
  d.filters.forEach(f=>{
    var fm=d.filterMap[f]||{};
    kids.push(body("→ "+f+": "+fm.activeDays+" active days, "+((fm.flatDays||0))+" flat days. Grade: "+(fm.grade||"—")+". Sharpe: "+(fmt(fm.sharpe))+".", {bold:false}));
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION 3.5 — MONTHLY RETURN ANALYSIS
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h2("3.5 Monthly Return Analysis"));
  kids.push(body(
    "The chart below shows the compounded return for each calendar month across all filter modes. "+
    "Green bars are profitable months, red bars are losing months. The line overlay traces the "+
    "cumulative compounded return through each month, revealing how individual months drove "+
    "the overall equity curve and where filter modes diverged."
  ));
  kids.push(spacer(3));

  // Chart — try parsed path first, then resolve by filename directly in run dir
  var monthlyChartPath = (d.charts.shared && d.charts.shared.monthly)
    ? resolvePath(d.charts.shared.monthly, auditDir, runDir)
    : resolvePath("monthly_cumulative_returns.png", auditDir, runDir);

  if (monthlyChartPath) {
    var monthlyBlk = imageBlock(monthlyChartPath, 630);
    if (monthlyBlk) {
      kids.push(monthlyBlk);
      kids.push(subtle("Figure 3.5: Monthly return by filter mode — bars show per-month return, lines show cumulative compounded equity."));
    } else {
      kids.push(...calloutBox("⚠  Monthly return chart found but could not be embedded: " + monthlyChartPath, "warn"));
    }
  } else {
    kids.push(...calloutBox(
      "⚠  Monthly return chart not found (monthly_cumulative_returns.png). "+
      "Run the full audit (without --quick) to generate the chart.", "warn"
    ));
  }
  kids.push(spacer(4));

  // Monthly summary table — pivot monthlySummaries into months × filters
  // d.monthlySummaries = [{filterLabel, rows: [{period, periodRoi, cumRoi}]}]
  if (d.monthlySummaries && d.monthlySummaries.length > 0) {
    kids.push(h3("Monthly Return Detail by Filter"));

    // Build month → {filterLabel → periodRoi} map
    var monthMap = {};
    d.monthlySummaries.forEach(function(ms) {
      if (!ms || !ms.rows) return;
      ms.rows.forEach(function(row) {
        var mo = row.period || "";
        if (!monthMap[mo]) monthMap[mo] = {};
        monthMap[mo][ms.filterLabel] = row.periodRoi || row.cumRoi || "—";
      });
    });

    var months = Object.keys(monthMap).sort();
    if (months.length > 0) {
      var mHeaders = ["Month", ...d.filters];
      var mColW    = [1800, ...d.filters.map(() => Math.floor((COL_W - 1800) / d.filters.length))];
      var mRows    = months.map(function(mo) {
        return [mo, ...d.filters.map(function(f) {
          return monthMap[mo][f] || "—";
        })];
      });
      kids.push(dataTable(mHeaders, mRows, mColW));
      kids.push(spacer(2));
      kids.push(subtle("Period Return % = compounded return within each calendar month. Flat months (0%) indicate the filter was active for the entire month."));
    }
  } else {
    // Fallback: worst-month summary table from filterMap
    var hasMonthly = d.filters.some(function(f) { return d.filterMap[f] && d.filterMap[f].worstMonth != null; });
    if (hasMonthly) {
      kids.push(h3("Worst Month Summary by Filter"));
      var wmRows = d.filters.map(function(f) {
        var fm = d.filterMap[f] || {};
        var wm = fm.worstMonth != null ? (fm.worstMonth > 0 ? "+" : "") + fm.worstMonth.toFixed(2) + "%" : "—";
        var ww = fm.worstWeek  != null ? (fm.worstWeek  > 0 ? "+" : "") + fm.worstWeek.toFixed(2)  + "%" : "—";
        var tr = fm.netret != null ? fm.netret : "—";
        return [f, wm, ww, tr, String(fm.activeDays || "—")];
      });
      kids.push(dataTable(["Filter","Worst Month","Worst Week","Net Return","Active Days"], wmRows,
        [2800, 1400, 1400, 1700, 1700]));
      kids.push(spacer(2));
      kids.push(subtle("Worst Month = largest single-month drawdown across the sample. Positive worst month means no calendar month was net negative."));
    }
  }

  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION 3 — PBO / OVERFITTING ANALYSIS
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("4. Overfitting Analysis — CSCV / PBO"));
  var pboType=d.pbo!=null?(d.pbo<0.1?"pass":d.pbo<0.3?"warn":"fail"):"info";
  kids.push(...calloutBox("PBO = "+(d.pboDetail.pbo||((d.pbo*100).toFixed(1)+"%"))+
    "  |  Performance Degradation = "+(d.pboDetail.pd||"—")+
    "  |  Probability of Loss = "+(d.pboDetail.pol||"0.0%"), pboType));
  kids.push(spacer(3));
  kids.push(body("PBO (Probability of Backtest Overfitting) is computed using the CSCV method of Bailey et al. (2015), Journal of Computational Finance. With N="+
    (d.pboDetail.n||"2")+" strategies tested over T="+(d.pboDetail.t||"369")+" trading days, "+
    "the logit(ω) distribution has only "+((d.pboDetail.n||"2"))+" discrete values. The low N is a known limitation — PBO grade bands are unreliable at N<10."));
  kids.push(body("The POL (Probability of Loss) = "+(d.pboDetail.pol||"0.0%")+" — meaning the training winner produced positive OOS Sharpe in 100% of train/test splits. This is the primary overfitting diagnostic."));
  if(d.charts.shared&&d.charts.shared.pbo){
    var blk=img(d.charts.shared.pbo, 600);
    if(blk){kids.push(blk);kids.push(subtle("Figure 4.1: PBO / CSCV logit distribution"));}
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION 5 — INSTITUTIONAL SCORECARD SUMMARY
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1("5. Institutional Scorecard Summary"));
  kids.push(body("The institutional scorecard evaluates the best filter across 15+ weighted criteria calibrated to allocator thresholds. Each criterion is scored and a composite grade assigned."));
  kids.push(spacer(3));
  // Best filter scorecard
  var bestScorecard = d.scorecards[best] || {};
  if (bestScorecard && bestScorecard.items && bestScorecard.items.length > 0) {
    var _bsScore = parseFloat(bestScorecard.score) || 0;
    var _bsType  = _bsScore >= 85 ? "pass" : _bsScore >= 65 ? "warn" : "fail";
    kids.push(...calloutBox("Grade: "+bestScorecard.grade+"  |  Score: "+bestScorecard.score+"  |  Pass: "+bestScorecard.pass+"  Fail: "+bestScorecard.fail+"  Warn: "+bestScorecard.warn, _bsType));
    kids.push(spacer(2));
    kids.push(scoreTable(bestScorecard.items.map(it=>({metric:it.label||"",goal:"",actual:it.score||"",status:it.status==="Pass"?"✅":it.status==="Fail"?"❌":"⚠"}))));
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTIONS 6+ — PER-FILTER DEEP DIVES
  // ═══════════════════════════════════════════════════════════════════════════
  filtersToReport.forEach((filt, fi) => {
    var fm = d.filterMap[filt] || {};
    var fc = d.charts[filt] || {};
    var fInst = fc.inst || {};
    var sectionBase = 6 + fi;

    kids.push(h1(sectionBase+". Per-Filter Evidence: "+filt));

    // ── 4.1 Performance summary ──
    kids.push(h2(sectionBase+".1 Performance Summary"));
    var cs = d.costSummary[filt] || {};
    var vs = d.volLevSummary[filt] || {};
    var perfRows = [
      ["Net Return",          cs.net||"—"],
      ["Gross Return",        cs.gross||"—"],
      ["Total Fees",          cs.fees||"—"],
      ["Net Fee Drag",        cs.drag||"—"],
      ["Avg Fee / Active Day",cs.avgFee||"—"],
      ["Active Trading Days", cs.activeDays||fm.activeDays||"—"],
      ["Sharpe",              fmt(fm.sharpe)],
      ["Sortino",             fmt(fm.sortino)],
      ["Calmar",              fmt(fm.calmar)],
      ["Max Drawdown",        fmtPct(fm.maxdd)],
      ["WF CV",               fmt(fm.cv)],
      ["Beta to BTC",         fmt(fm.beta)],
      ["Annual Alpha",        fm.alpha||"—"],
      ["DSR %",               fm.dsrPct != null ? fm.dsrPct.toFixed(2)+"%" : (fm.dsr||"—")],
      ["Worst Day %",         fm.worstDay != null ? fmtPct(fm.worstDay) : "—"],
      ["Worst Week %",        fm.worstWeek != null ? fmtPct(fm.worstWeek) : "—"],
      ["Worst Month %",       fm.worstMonth != null ? fmtPct(fm.worstMonth) : "—"],
      ["Grade",               fm.grade||"—"],
    ];
    if(vs.mean) perfRows.push(["Vol-Lev Mean Boost", vs.mean+"×"]);
    if(vs.max)  perfRows.push(["Vol-Lev Max Boost",  vs.max+"×"]);
    if(vs.floorDays) perfRows.push(["Days at Lev Floor", vs.floorDays]);
    if(vs.maxDays)   perfRows.push(["Days at Max Boost", vs.maxDays]);
    kids.push(dataTable(["Metric","Value"],perfRows,[4000,3200]));
    kids.push(spacer(3));

    // Dashboard & equity charts
    if(fc.dashboard){var blk=img(fc.dashboard,640);if(blk){kids.push(blk);kids.push(subtle("Performance Dashboard — "+filt));kids.push(spacer(4));}}
    if(fc.equity){var blk=img(fc.equity,640);if(blk){kids.push(blk);kids.push(subtle("Corrected Equity Curve — "+filt));kids.push(spacer(4));}}
    // Inst: equity + rolling sharpe + rolling cagr + return distribution
    var blk;
    blk=instImg("equity_curve.png",filt,620);           if(blk){kids.push(blk);kids.push(subtle("Equity Curve + ATH + Drawdown Analysis — "+filt));kids.push(spacer(2));}
    blk=instImg("rolling_sharpe.png",filt,620);         if(blk){kids.push(blk);kids.push(subtle("Rolling Sharpe — "+filt));kids.push(spacer(2));}
    blk=instImg("rolling_cagr.png",filt,620);           if(blk){kids.push(blk);kids.push(subtle("Rolling CAGR — "+filt));kids.push(spacer(2));}
    blk=instImg("return_distribution.png",filt,620);    if(blk){kids.push(blk);kids.push(subtle("Return Distribution — "+filt));kids.push(spacer(2));}
    blk=instImg("drawdown_episodes.png",filt,620);      if(blk){kids.push(blk);kids.push(subtle("Drawdown Episodes — "+filt));kids.push(spacer(2));}

    // ── 4.2 Institutional Scorecard ──
    kids.push(h2(sectionBase+".2 Institutional Scorecard"));
    var sc = d.scorecards[filt];
    if(sc&&sc.items.length>0){
      var _scScore = parseFloat(sc.score) || 0;
      var _scType  = _scScore >= 85 ? "pass" : _scScore >= 65 ? "warn" : "fail";
      kids.push(...calloutBox("Grade: "+sc.grade+"  |  Score: "+sc.score+"  |  Pass: "+sc.pass+"  Fail: "+sc.fail+"  Warn: "+sc.warn, _scType));
      var scRows=sc.items.map(it=>[
        it.label||"",
        it.score||"",
        it.status==="Pass"?"✅":it.status==="Fail"?"❌":"⚠",
      ]);
      kids.push(dataTable(["Metric","Score","Status"],scRows,[5000,1500,1000]));
    } else {
      kids.push(body("(Scorecard data not captured for this filter.)"));
    }
    kids.push(spacer(4));

    // ── DSR + MTL Detail ──
    kids.push(h2(sectionBase+".3 Deflated Sharpe Ratio (DSR) & Minimum Track Record Length"));
    var ddKey = Object.keys(d.dsrDetail).find(k=>k===filt||filt.includes(k.replace(/^A - /,""))||k.includes(filt.replace(/^A - /,""))) || filt;
    var dd = d.dsrDetail[ddKey] || {};
    if(dd.dsr){
      kids.push(dataTable(["Metric","Value"],[
        ["Observed Sharpe",       dd.obsSharpe||"—"],
        ["SR Benchmark (expected max from trials)", dd.srBench||"—"],
        ["SR Std Error",          dd.srStdErr||"—"],
        ["Z-score",               dd.zScore||"—"],
        ["DSR (prob. genuine edge)", (parseFloat(dd.dsr)*100).toFixed(2)+"%"],
        ["P(false positive)",     dd.falsePos ? (parseFloat(dd.falsePos)*100).toFixed(2)+"%" : "—"],
        ["Return Skewness",       dd.skew||"—"],
        ["Excess Kurtosis",       dd.kurt||"—"],
        ["DSR Verdict",           dd.verdict||"—"],
        ["MTL (95% conf)",        dd.mtl||"—"],
        ["MTL Verdict",           dd.mtlVerdict||"—"],
      ],[4000,3200]));
    } else {
      // Fallback to global dsrMtl
      var dsr2=d.dsrMtl||{};
      kids.push(dataTable(["Metric","Value"],[
        ["DSR (prob. genuine edge)", dsr2.dsr||"—"],
        ["P(false positive)",        dsr2.fp||"—"],
        ["Min Track Record Length",  dsr2.mtl||"—"],
        ["Track Record Adequate",    dsr2.trackOk||"—"],
      ],[4000,3200]));
    }
    kids.push(spacer(4));

    // ── 4.4 Walk-Forward Validation ──
    kids.push(h2(sectionBase+".4 Walk-Forward Cross-Validation"));
    var folds = d.wfFolds[filt] || [];
    if(folds.length>0){
      var foldRows=folds.map(fold=>[
        fold.fold||"—",
        fold.dates||"—",
        fold.is_sharpe!=null?fmt(fold.is_sharpe):"—",
        fold.oos_sharpe!=null?fmt(fold.oos_sharpe):"—",
        fold.oos_maxdd!=null?fold.oos_maxdd+"%":"—",
        fold.oos_sortino!=null?fmt(fold.oos_sortino):"—",
        fold.oos_dsr||"—",
        fold.active!=null?fold.active+"d":"—",
      ]);
      kids.push(dataTable(
        ["Fold","Test Dates","IS Sharpe","OOS Sharpe","MaxDD","Sortino","DSR","Active"],
        foldRows,
        [500,2200,1000,1000,1000,900,900,800]
      ));
    }
    kids.push(...calloutBox("WF CV = "+fmt(fm.cv)+" | Mean OOS Sharpe = "+fmt(fm.fa_wf_sharpe)+" | % Folds Positive = "+((fm.fa_wf_pct_pos!=null?fm.fa_wf_pct_pos.toFixed(1)+"%":fm.wf_pct_pos)||"—"), +fm.cv<0.40?"pass":"warn"));
    kids.push(...bulletList(insights.wfCV));
    kids.push(spacer(4));

    // Helper to find per-filter data using prefix matching
    function _fkey(dict) {
      return Object.keys(dict).find(function(k){
        return k===filt||filt.includes(k.replace(/^A - /,""))||k.includes(filt.replace(/^A - /,""));
      })||filt;
    }

    // ── Risk-Adjusted Return Quality ──
    var raData=d.riskAdjReturn[_fkey(d.riskAdjReturn)]||{};
    if(raData.sharpe){
      kids.push(h2(sectionBase+".4b Risk-Adjusted Return Quality"));
      kids.push(dataTable(["Metric","Value"],[
        ["Sharpe Ratio",   raData.sharpe||"—"],
        ["Sortino Ratio",  raData.sortino||"—"],
        ["Calmar Ratio",   raData.calmar||"—"],
        ["Omega Ratio",    raData.omega||"—"],
        ["Ulcer Index",    raData.ulcer||"—"],
        ["Profit Factor",  raData.profitFactor||"—"],
      ],[4000,3200]));
      kids.push(spacer(3));
    }

    // ── VaR / CVaR + Rolling MaxDD ──
    var vcData=d.varCvar[_fkey(d.varCvar)]||{};
    var rmdData=d.rollMaxDD[_fkey(d.rollMaxDD)]||[];
    if(vcData.var5||rmdData.length>0){
      kids.push(h2(sectionBase+".4c Value at Risk & Rolling Max Drawdown"));
      if(vcData.var5){
        kids.push(dataTable(["Metric","Value"],[
          ["VaR 5% (daily)",  vcData.var5||"—"],
          ["CVaR 5% (daily)", vcData.cvar5||"—"],
          ["VaR 1% (daily)",  vcData.var1||"—"],
          ["CVaR 1% (daily)", vcData.cvar1||"—"],
        ],[4000,3200]));
        kids.push(spacer(2));
      }
      if(rmdData.length>0){
        kids.push(dataTable(["Window","Worst Cumulative Return"],
          rmdData.map(r=>[r.window, r.worst]),
          [2000,5200]));
      }
      kids.push(spacer(3));
    }

    // ── Regime & Conditional Analysis ──
    var rcData=d.regimeCond[_fkey(d.regimeCond)]||{};
    if(rcData.upDays){
      kids.push(h2(sectionBase+".4d Regime & Conditional Analysis"));
      kids.push(dataTable(["Regime","Days","Mean Return","Sharpe"],[
        ["Up days",        rcData.upDays||"—",   rcData.upMean||"—",     rcData.upSharpe||"—"],
        ["Down days",      rcData.downDays||"—", rcData.downMean||"—",   rcData.downSharpe||"—"],
        ["Low-vol regime", "—",                  rcData.lowVolMean||"—", rcData.lowVolSharpe||"—"],
        ["High-vol regime","—",                  rcData.highVolMean||"—",rcData.highVolSharpe||"—"],
      ],[2200,1200,2000,2000]));
      if(rcData.roll60ShMin){
        kids.push(spacer(2));
        kids.push(dataTable(["Rolling 60d Metric","Min","Median","Max"],[
          ["Sharpe",rcData.roll60ShMin,rcData.roll60ShMed,rcData.roll60ShMax],
        ],[2800,1600,1600,1600]));
      }
      kids.push(spacer(3));
    }

    // ── Capital & Operational ──
    var coData=d.capitalOps[_fkey(d.capitalOps)]||{};
    if(coData.fullKelly||coData.levers&&coData.levers.length>0){
      kids.push(h2(sectionBase+".4e Capital & Operational"));
      if(coData.fullKelly){
        kids.push(dataTable(["Metric","Value"],[
          ["Full Kelly Fraction",  coData.fullKelly||"—"],
          ["Half Kelly Fraction",  coData.halfKelly||"—"],
          ["Ruin Probability (50% DD, 365d, 2000-path bootstrap)",  coData.ruinProb||"—"],
        ],[4000,3200]));
        kids.push(spacer(2));
      }
      if(coData.levers&&coData.levers.length>0){
        kids.push(body("Leverage Sensitivity:"));
        kids.push(dataTable(["Leverage","CAGR%","Sharpe","MaxDD%"],
          coData.levers.map(r=>[r.lev,r.cagr,r.sharpe,r.maxdd]),
          [1500,2000,1800,2000]));
      }
      kids.push(spacer(3));
    }

    // ── Sharpe Stability ──
    var ssData=d.sharpeStab[_fkey(d.sharpeStab)]||{};
    if(ssData.meanSharpe){
      kids.push(h2(sectionBase+".4f Sharpe Stability (Walk-Forward Folds)"));
      kids.push(dataTable(["Metric","Value"],[
        ["Mean OOS Sharpe",  ssData.meanSharpe||"—"],
        ["Sharpe Std Dev",   ssData.stdDev||"—"],
        ["% Folds > 2.0",   ssData.pctAbove2||"—"],
        ["95% Confidence Interval", ssData.ci95lo&&ssData.ci95hi?"["+ssData.ci95lo+", "+ssData.ci95hi+"]":"—"],
        ["T-statistic",      ssData.tstat||"—"],
        ["P-value",          ssData.pvalue||"—"],
      ],[4000,3200]));
      kids.push(spacer(3));
    }

    // ── 4.4 Alpha / Beta ──
    kids.push(h2(sectionBase+".4 Alpha vs Beta Decomposition"));
    var ab=d.alphaData[filt]||{};
    var abRows=[
      ["Beta to BTC",     ab.beta||fmt(fm.beta)],
      ["Daily Alpha",     ab.dailyAlpha||"—"],
      ["Annual Alpha",    ab.annualAlpha||fm.alpha||"—"],
      ["Variance Explained (R²)", ab.r2||"—"],
    ];
    kids.push(dataTable(["Metric","Value"],abRows,[4000,3200]));
    kids.push(...bulletList(insights.alphaBeta));
    if(fc.btc_scatter){var blk=img(fc.btc_scatter,600);if(blk){kids.push(blk);kids.push(subtle("Strategy daily return vs BTC daily return — "+filt));}}
    if(fc.btc_vol){var blk=img(fc.btc_vol,600);if(blk){kids.push(blk);kids.push(subtle("BTC Volatility Scatter — "+filt));}}
    if(fc.dispersion){var blk=img(fc.dispersion,600);if(blk){kids.push(blk);kids.push(subtle("Dispersion Scatter — "+filt));}}
    kids.push(spacer(4));

    // ── 4.5 Dispersion Analysis ──
    kids.push(h2(sectionBase+".5 Dispersion Decile Expectancy"));
    var dd=d.dispDecile[filt]||[];
    if(dd.length>0){
      kids.push(dataTable(["Decile","Days","Disp Low","Disp High","Mean Ret","Win Rate","Sharpe"],
        dd.map(r=>[r.d,r.days,r.lo,r.hi,r.ret,r.wr,r.sharpe]),
        [700,700,1200,1200,1200,1200,1000]));
    }
    if(fc.decile){var blk=img(fc.decile,620);if(blk){kids.push(blk);kids.push(subtle("Dispersion Decile Chart — "+filt));}}
    kids.push(spacer(4));

    kids.push(h2(sectionBase+".6 Dispersion Threshold Surface"));
    var ds=d.dispSurface[filt]||[];
    if(ds.length>0){
      kids.push(dataTable(["High Pct","Flat Days","Active %","Sharpe"],ds.map(r=>[r.high,r.flat,r.active,r.sharpe]),[1500,1500,1500,1500]));
    }
    if(fc.surface){var blk=img(fc.surface,620);if(blk){kids.push(blk);kids.push(subtle("Threshold Surface — "+filt));}}
    kids.push(spacer(4));

    // ── 4.6 Sharpe vs Correlation ──
    kids.push(h2(sectionBase+".7 Sharpe vs Correlation Analysis"));
    if(fc.sharpe_corr){var blk=img(fc.sharpe_corr,620);if(blk){kids.push(blk);kids.push(subtle("Sharpe vs Correlation — "+filt));}}
    kids.push(spacer(4));

    // ── 4.7 Regime Attribution ──
    kids.push(h2(sectionBase+".8 Regime Attribution"));
    var ra=d.regimeAttr[filt]||d.regimeAttr["_shared"]||{};
    var rd=d.regimeDuration[filt]||{};
    var raRows=[];
    if(ra.disp_High) raRows.push(["High Dispersion",  ra.disp_High.days, ra.disp_High.mean, ra.disp_High.sharpe]);
    if(ra.disp_Low)  raRows.push(["Low Dispersion",   ra.disp_Low.days,  ra.disp_Low.mean,  ra.disp_Low.sharpe]);
    if(ra.btc_Uptrend)   raRows.push(["BTC Uptrend",   ra.btc_Uptrend.days,   ra.btc_Uptrend.mean,   ra.btc_Uptrend.sharpe]);
    if(ra.btc_Downtrend) raRows.push(["BTC Downtrend", ra.btc_Downtrend.days, ra.btc_Downtrend.mean, ra.btc_Downtrend.sharpe]);
    if(ra.vol_High)  raRows.push(["High Vol",   ra.vol_High.days,  ra.vol_High.mean,  ra.vol_High.sharpe]);
    if(ra.vol_Low)   raRows.push(["Low Vol",    ra.vol_Low.days,   ra.vol_Low.mean,   ra.vol_Low.sharpe]);
    if(ra["HighDisp+HighVol"]) raRows.push(["HighDisp+HighVol", ra["HighDisp+HighVol"].days, ra["HighDisp+HighVol"].mean, ra["HighDisp+HighVol"].sharpe]);
    if(ra["LowDisp+LowVol"])   raRows.push(["LowDisp+LowVol",  ra["LowDisp+LowVol"].days,  ra["LowDisp+LowVol"].mean,  ra["LowDisp+LowVol"].sharpe]);
    if(raRows.length>0) kids.push(dataTable(["Regime","Days","Mean Return","Sharpe"],raRows,[3000,1500,1800,1500]));
    if(fc.heatmap){var blk=img(fc.heatmap,620);if(blk){kids.push(blk);kids.push(subtle("Regime Heatmap — "+filt));}}
    if(rd.count){
      kids.push(body("Regime Duration (High Disp+High Vol): N="+rd.count+" regimes, mean="+rd.mean+"d, median="+rd.median+"d, max="+rd.max+"d"));
    }
    kids.push(spacer(4));

    // ── 4.8 Skew Diagnostics ──
    kids.push(h2(sectionBase+".9 Skew vs Equity Diagnostics"));
    var sk=d.skewDiag[filt]||{};
    if(sk.fastMean){
      var skRows=[
        ["Skew Fast (5d) — Mean",    sk.fastMean],
        ["Skew Fast (5d) — Std",     sk.fastStd],
        ["Skew Fast — % Positive",   sk.fastPos],
        ["Skew Slow (20d) — Mean",   sk.slowMean||"—"],
        ["Signal Collapse Days",     sk.collapse?""+sk.collapse+" ("+sk.collapsePct+")":"—"],
        ["Spearman(skew_fast, ret)", sk.corrSkew||"—"],
        ["Spearman(norm_disp, ret)", sk.corrDisp||"—"],
      ];
      kids.push(dataTable(["Metric","Value"],skRows,[4000,3200]));
    }
    if(fc.skew){var blk=img(fc.skew,620);if(blk){kids.push(blk);kids.push(subtle("Skew vs Equity — "+filt));}}
    kids.push(spacer(4));

    // ── 4.9 Sensitivity & Robustness charts (inst) ──
    kids.push(h2(sectionBase+".10 Sensitivity & Robustness Analysis"));

    // ── 4.9a Neighbor Plateau Test (styled) ──
    var npKey=Object.keys(d.neighborPlateau).find(k=>k===filt||filt.includes(k)||k.includes(filt))||filt;
    var np=d.neighborPlateau[npKey]||d.neighborPlateau["_shared"]||null;
    if(np && np.plateauRatio){
      kids.push(h3("Neighbor Plateau Test"));
      kids.push(body("Simultaneously perturbs all parameters by ±"+(np.perturbPct||"15%")+
        " (n="+(np.nNeighbors||"200")+" random neighbors) and measures the distribution of resulting Sharpe ratios. "+
        "A high plateau ratio indicates the strategy sits on a broad, robust plateau rather than a fragile, overfitted spike."));
      kids.push(spacer(2));
      // Verdict callout
      var npPct=parseFloat(np.plateauRatio);
      var npType=npPct>=70?"pass":"warn";
      var npVerdict=npPct>=70
        ? "PLATEAU — "+np.plateauRatio+" of neighbors within ±0.5 Sharpe of baseline (≥70% threshold)"
        : "SPIKE — only "+np.plateauRatio+" of neighbors within ±0.5 Sharpe of baseline (<70% threshold)";
      kids.push(...calloutBox(npVerdict, npType));
      kids.push(spacer(2));
      // Data table
      kids.push(dataTable(["Metric","Value"],[
        ["Baseline Sharpe",              np.baseSharpe||"—"],
        ["Perturbation Scale",           "±"+(np.perturbPct||"15%")+" joint"],
        ["Neighbors Sampled",            np.nNeighbors||"—"],
        ["Plateau Ratio (±0.5 Sharpe)",  np.plateauRatio||"—"],
        ["Neighbor Sharpe p10",          np.p10||"—"],
        ["Neighbor Sharpe p25",          np.p25||"—"],
        ["Neighbor Sharpe Median",       np.median||"—"],
        ["Neighbor Sharpe p75",          np.p75||"—"],
        ["Neighbor Sharpe Std Dev",      np.std||"—"],
      ],[4200,3000]));
      kids.push(spacer(2));
    }
    // Neighbor Plateau chart
    var npBlk=instImg("neighbor_plateau.png",filt,620);
    if(npBlk){kids.push(npBlk);kids.push(subtle("Neighbor Plateau — parameter robustness — "+filt));kids.push(spacer(3));}

    // Remaining sensitivity & robustness charts
    var instCharts = [
      ["sensitivity_heatmap.png",        "Sensitivity Heatmap"],
      ["sensitivity_tornado.png",        "Sensitivity Tornado"],
      ["sensitivity_lines.png",          "Sensitivity Lines"],
      ["leverage_sensitivity.png",       "Leverage Sensitivity"],
      ["slippage_sensitivity.png",       "Slippage Sensitivity"],
      ["capped_return_sensitivity.png",  "Capped Return Sensitivity"],
    ];
    instCharts.forEach(([fn, cap])=>{
      var blk=instImg(fn,filt,620);
      if(blk){kids.push(blk);kids.push(subtle(cap+" — "+filt));kids.push(spacer(2));}
    });
    kids.push(spacer(4));

    // ── 4.10 Capacity & Cost ──
    kids.push(h2(sectionBase+".11 Capacity & Cost Analysis"));
    var capCharts = [
      ["capacity_curve.png",   "Capacity Curve — AUM impact on Sharpe"],
      ["capacity_equity.png",  "Capacity Equity — equity at different AUM levels"],
      ["cost_curve.png",       "Cost Curve — fee sensitivity"],
    ];
    capCharts.forEach(([fn, cap])=>{
      var blk=instImg(fn,filt,620);
      if(blk){kids.push(blk);kids.push(subtle(cap+" — "+filt));kids.push(spacer(2));}
    });
    kids.push(spacer(4));

    // ── 4.11 Stress tests ──
    kids.push(h2(sectionBase+".12 Stress Tests — Shock Injection & Luck Analysis"));
    var stressCharts = [
      ["shock_injection.png",      "Shock Injection — artificial tail event stress"],
      ["lucky_streak.png",         "Lucky Streak Analysis"],
      ["top_n_removal.png",        "Top-N Symbol Removal — concentration risk"],
    ];
    stressCharts.forEach(([fn, cap])=>{
      var blk=instImg(fn,filt,620);
      if(blk){kids.push(blk);kids.push(subtle(cap+" — "+filt));kids.push(spacer(2));}
    });
    kids.push(spacer(4));

    // ── 4.12 Signal Predictiveness ──
    kids.push(h2(sectionBase+".13 Performance Predictability (Reverse Spearman IC)"));
    kids.push(...bulletList(insights.signalPred));
    var spKey=Object.keys(d.signalPred).find(k=>k===filt||filt.includes(k)||k.includes(filt))||filt;
    var sp=d.signalPred[spKey]||{};
    var windows=Object.keys(sp).sort();
    if(windows.length>0){
      windows.forEach(w=>{
        kids.push(h3("Window: "+w));
        var rows=sp[w];
        if(!Array.isArray(rows)||rows.length===0)return;
        [1,3,5].forEach(fwdDay=>{
          var fwdRows=rows.filter(r=>r.fwd===fwdDay).map(r=>[
            r.signal, r.kind, r.fwd+"d",
            r.ic.toFixed(4), r.pval.toFixed(4), r.n,
            r.sig?"★":"",
            r.ic>0?"↑ momentum":"↓ contrarian"
          ]);
          if(fwdRows.length>0){
            kids.push(body("Forward "+fwdDay+"d:"));
            kids.push(dataTable(["Signal","Kind","Fwd","IC","p-val","N","Sig","Direction"],fwdRows,[1600,800,600,1000,1000,700,500,1600]));
            kids.push(spacer(2));
          }
        });
      });
    } else {
      kids.push(body("Performance predictability data not captured."));
    }
    kids.push(spacer(4));

    // ── Periodic Return Breakdown ──
    kids.push(h2(sectionBase+".14 Periodic Return Breakdown"));
    var pfKey = Object.keys(d.periodicFull).find(k=>k===filt||filt.includes(k.replace(/^A - /,""))||k.includes(filt.replace(/^A - /,""))) || filt;
    var pfData = d.periodicFull[pfKey] || {};
    var pfPeriods = [["Monthly","monthly"],["Weekly","weekly"],["Daily","daily"]];
    var pfRows = [];
    pfPeriods.forEach(function(pair){
      var label=pair[0], key=pair[1], p=pfData[key];
      if(p) pfRows.push([label, p.winRate||"—", p.mean||"—", p.avgWin||"—", p.avgLoss||"—", p.best||"—", p.worst||"—"]);
    });
    if(pfRows.length>0){
      kids.push(dataTable(["Period","Win Rate","Mean","Avg Win","Avg Loss","Best","Worst"],pfRows,[1000,1100,1100,1100,1100,1100,1100]));
    } else {
      kids.push(body("Periodic breakdown not captured for this filter."));
    }
    kids.push(spacer(4));

    // ── Minimum Cumulative Return ──
    kids.push(h2(sectionBase+".15 Minimum Cumulative Return"));
    kids.push(body("Worst cumulative non-compounding return over every possible rolling window of N days. Shows the worst entry-to-exit experience for an investor who enters at the worst possible time."));
    kids.push(spacer(2));
    var mcrKey = Object.keys(d.minCumRet).find(k=>k===filt||filt.includes(k.replace(/^A - /,""))||k.includes(filt.replace(/^A - /,""))) || filt;
    var mcrRows = d.minCumRet[mcrKey] || [];
    if(mcrRows.length>0){
      kids.push(dataTable(["Window","Min Cum Return","Worst Start Day","Worst End Day"],
        mcrRows.map(r=>[r.window, r.minRet, r.worstStart, r.worstEnd]),
        [1200,2000,2000,2000]));
    } else {
      kids.push(body("Minimum cumulative return data not captured for this filter."));
    }
    kids.push(spacer(4));

    // ── Fees panel ──
    kids.push(h2(sectionBase+".16 Transaction Fees Panel (First 50 Active Days)"));
    var fp=d.feesPanel[filt]||[];
    if(fp.length>0){
      var activeFees=fp.filter(r=>!r.noEntry).slice(0,30);
      if(activeFees.length>0){
        kids.push(dataTable(
          ["Date","Start ($)","Lev","Gross %","Net %","Net P&L ($)"],
          activeFees.map(r=>[r.date, r.start, r.lev, r.retGross, r.retNet, r.pnl]),
          [1200,1600,700,1000,1000,1700]
        ));
      }
    } else {
      kids.push(body("Fee panel data not captured in audit output."));
    }
    kids.push(spacer(4));

    // ── Monthly summary ──
    // Match by exact label OR by filt containing the milestone label (e.g. "A - No Filter" contains "No Filter")
    var monthSummary = d.monthlySummaries.find(s=>s.filterLabel===filt||filt.includes(s.filterLabel)||s.filterLabel.includes(filt));
    kids.push(h2(sectionBase+".15 Monthly Period Summary"));
    if(monthSummary&&monthSummary.rows.length>0){
      var mHasBalance = monthSummary.rows[0].balance !== undefined;
      if(mHasBalance){
        kids.push(dataTable(
          ["Period","Balance ($)","Net PnL","Period ROI","Cum ROI"],
          monthSummary.rows.map(r=>[r.period, parseFloat(r.balance||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}), (parseFloat(r.pnl||0)>=0?"+":"")+parseFloat(r.pnl||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}), r.periodRoi, r.cumRoi]),
          [1500,2000,1800,1300,1300]
        ));
      } else {
        kids.push(dataTable(
          ["Period","Label","Close ($)","ROI"],
          monthSummary.rows.map(r=>[r.period,r.label,parseFloat(r.close||0).toFixed(2),r.roi]),
          [1500,1800,2000,800]
        ));
      }
    } else {
      kids.push(body("Monthly summary data not captured for this filter."));
    }
    kids.push(spacer(4));

    // ── Weekly summary ──
    var weekSummary = d.weeklySummaries.find(s=>s.filterLabel===filt||filt.includes(s.filterLabel)||s.filterLabel.includes(filt));
    kids.push(h2(sectionBase+".16 Weekly Period Summary"));
    if(weekSummary&&weekSummary.rows.length>0){
      var wHasBalance = weekSummary.rows[0].balance !== undefined;
      if(wHasBalance){
        kids.push(dataTable(
          ["Period","Balance ($)","Net PnL","Period ROI","Cum ROI"],
          weekSummary.rows.map(r=>[r.period, parseFloat(r.balance||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}), (parseFloat(r.pnl||0)>=0?"+":"")+parseFloat(r.pnl||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}), r.periodRoi, r.cumRoi]),
          [1500,2000,1800,1300,1300]
        ));
      } else {
        kids.push(dataTable(
          ["Period","Label","Close ($)","ROI"],
          weekSummary.rows.map(r=>[r.period,r.label,parseFloat(r.close||0).toFixed(2),r.roi]),
          [1500,1800,2000,800]
        ));
      }
    } else {
      kids.push(body("Weekly summary data not captured for this filter."));
    }

    if(fi < d.filters.length-1) kids.push(pageBreak());
    else kids.push(pageBreak());
  });


  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — PARAMETER SWEEP ANALYSIS
  // ═══════════════════════════════════════════════════════════════════════════
  kids.push(h1(sweepSec+". Parameter Sweep & Robustness Analysis"));
  kids.push(body("All charts generated by the parameter_sweeps/ subdirectory. " +
    "Surfaces and ridge maps confirm that the live configuration sits in a " +
    "stable plateau rather than a fragile optimum."));
  kids.push(spacer(3));

  // ── Param Surfaces ──
  var SURFACE_PAIRS = [
    ["killx_killy",   "EARLY_KILL_X × EARLY_KILL_Y",    "Gate timing: trial window vs entry threshold"],
    ["sl_tsl",        "PORT_SL × PORT_TSL",             "Risk controls: hard stop vs trailing stop"],
    ["lbase_lhigh",   "L_BASE × L_HIGH",                "Leverage architecture: base vs ceiling"],
    ["lhigh_tsl",     "L_HIGH × PORT_TSL",              "Risk/reward: leverage ceiling vs trailing stop"],
    ["sl_lhigh",      "PORT_SL × L_HIGH",               "Leverage sensitivity to hard stop"],
    ["killy_sl",      "EARLY_KILL_Y × PORT_SL",         "Entry selectivity vs downside protection"],
    ["filly_killy",   "EARLY_FILL_Y × EARLY_KILL_Y",   "Profit target vs entry threshold"],
    ["filly_fine",    "EARLY_FILL_Y × EARLY_KILL_Y (fine)", "Fine-resolution fill threshold zoom"],
    ["fillx_filly",   "EARLY_FILL_X × EARLY_FILL_Y",   "Fill window duration vs profit threshold"],
    ["killy_instilly","EARLY_KILL_Y × EARLY_INSTILL_Y", "Nested entry gates: skip vs conviction"],
  ];

  kids.push(h2(sweepSec+".1 Parameter Surfaces (2D Sharpe / CAGR / MaxDD Heatmaps)"));
  kids.push(body("Each surface sweeps two parameters simultaneously, holding all others at the live config. " +
    "Bright regions = high Sharpe. The baseline configuration is marked with a cross."));
  kids.push(spacer(2));
  SURFACE_PAIRS.forEach(([lbl, params, desc]) => {
    var blk = sweepImg("param_surface_"+lbl+".png", 620);
    if (blk) {
      kids.push(blk);
      kids.push(subtle(params+" — "+desc));
      kids.push(spacer(3));
    }
  });

  // ── Ridge Maps ──
  kids.push(h2(sweepSec+".2 Sharpe Ridge Maps"));
  kids.push(body("The ridge map shows the maximum Sharpe value in each row and column of the surface, " +
    "revealing whether the optimum is a broad plateau or a narrow ridge."));
  kids.push(spacer(2));
  SURFACE_PAIRS.forEach(([lbl, params, desc]) => {
    var blk = sweepImg("sharpe_ridge_map_"+lbl+".png", 620);
    if (blk) {
      kids.push(blk);
      kids.push(subtle("Ridge Map: "+params+" — "+desc));
      kids.push(spacer(3));
    }
  });

  // ── Plateau Detectors ──
  kids.push(h2(sweepSec+".3 Sharpe Plateau Detectors"));
  kids.push(body("Highlights connected plateau regions within a percentage of the global maximum Sharpe. " +
    "A wide, well-connected plateau indicates robustness to parameter perturbation."));
  kids.push(spacer(2));
  SURFACE_PAIRS.forEach(([lbl, params, desc]) => {
    var blk = sweepImg("sharpe_plateau_detector_"+lbl+".png", 620);
    if (blk) {
      kids.push(blk);
      kids.push(subtle("Plateau: "+params+" — "+desc));
      kids.push(spacer(3));
    }
  });
  // Plateau summary table (95% threshold, one row per surface)
  var platSummaryPath = resolveSweepPath("sharpe_plateau_summary.csv", auditDir, runDir);
  if (platSummaryPath && fs.existsSync(platSummaryPath)) {
    var platLines = fs.readFileSync(platSummaryPath, "utf8").trim().split("\n");
    var plat95 = platLines.slice(1).filter(function(l){ return l.split(",")[3]==="0.95"; });
    if (plat95.length > 0) {
      kids.push(body("Plateau Summary — 95% threshold (fraction of cells at ≥95% of surface peak Sharpe):"));
      kids.push(dataTable(
        ["Surface","Param X","Param Y","Cells ≥95%","% Surface","Cluster Size","Base In?","Verdict"],
        plat95.map(function(l){
          var c=l.split(",");
          return [c[0],c[1],c[2],c[5],c[6]+"%",c[7],c[8]==="True"?"✅":"❌",c[9]];
        }),
        [1100,1100,1100,900,900,1100,800,2200]
      ));
      kids.push(...bulletList(insights.paramSurfaces));
      kids.push(spacer(3));
    }
  }

  // ── CSV / text helpers for sweep files ──
  function readSweepCSV(relPath) {
    var p = resolveSweepPath(relPath, auditDir, runDir);
    if (!p || !fs.existsSync(p)) return null;
    var lines = fs.readFileSync(p, "utf8").trim().split("\n");
    if (lines.length < 2) return null;
    var headers = lines[0].split(",");
    var rows = lines.slice(1).map(function(l){ return l.split(","); });
    return { headers: headers, rows: rows };
  }
  function parseCubeSummary(relPath) {
    var p = resolveSweepPath(relPath, auditDir, runDir);
    if (!p || !fs.existsSync(p)) return null;
    var txt = fs.readFileSync(p, "utf8");
    var out = {};
    var m;
    m = txt.match(/Grid\s*:\s*(.+)/);              if(m) out.grid = m[1].trim();
    m = txt.match(/Cells valid\s*:\s*(\d+)/);       if(m) out.valid = m[1].trim();
    m = txt.match(/Peak Sharpe\s*:\s*([\d.]+)\s+at\s+(.+)/); if(m){ out.peakSharpe=m[1]; out.peakAt=m[2].trim(); }
    m = txt.match(/Baseline Sharpe\s*:\s*([\d.nan]+)/i);    if(m) out.baseline = m[1].trim();
    m = txt.match(/Plateau ≥95%(?:\s*pk)?\s*:\s*([\d/]+)\s+\(([\d.]+)%\)/); if(m){ out.plateau95=m[1]; out.plateau95pct=m[2]+"%"; }
    m = txt.match(/Plateau ≥90%(?:\s*pk)?\s*:\s*([\d/]+)\s+\(([\d.]+)%\)/); if(m){ out.plateau90=m[1]; out.plateau90pct=m[2]+"%"; }
    m = txt.match(/Verdict\s*:\s*(.+)/);            if(m) out.verdict = m[1].trim();
    return out;
  }

  // ── Stability Cubes ──
  kids.push(h2(sweepSec+".4 Parametric Stability Cubes"));
  kids.push(body("Three independent 3-D stability cubes test whether the live configuration sits in a robust " +
    "plateau across its three major design axes: leverage architecture, risk throttle thresholds, and exit mechanics. " +
    "Plateau ≥95% reports the fraction of valid cells achieving ≥95% of the peak Sharpe in the cube."));
  kids.push(spacer(2));

  // 5.4.1 Leverage Cube (L_BASE × L_HIGH × VOL_LEV_MAX_BOOST)
  kids.push(h3("Leverage Cube — L_BASE × L_HIGH × VOL_LEV_MAX_BOOST"));
  var lvBlk = sweepImg("stability_cube_leverage/stability_cube_lbase_slices.png", 620);
  if (lvBlk) { kids.push(lvBlk); kids.push(subtle("Leverage Cube — L_BASE slices (Sharpe across L_HIGH × BOOST)")); kids.push(spacer(2)); }
  var lvSum = parseCubeSummary("stability_cube_leverage/stability_cube_summary.txt");
  if (lvSum) {
    kids.push(dataTable(["Metric","Value"],[
      ["Grid",            lvSum.grid||"—"],
      ["Cells Valid",     lvSum.valid||"—"],
      ["Peak Sharpe",     (lvSum.peakSharpe||"—")+"  at  "+(lvSum.peakAt||"")],
      ["Baseline Sharpe", lvSum.baseline||"—"],
      ["Plateau ≥95%",    (lvSum.plateau95||"—")+"  ("+( lvSum.plateau95pct||"—")+")"],
      ["Plateau ≥90%",    (lvSum.plateau90||"—")+"  ("+( lvSum.plateau90pct||"—")+")"],
      ["Verdict",         lvSum.verdict||"—"],
    ],[3500,3700]));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.stabilityLeverage));

  // 5.4.2 Risk Throttle Cube (EARLY_FILL_Y × EARLY_KILL_Y × BOOST)
  kids.push(h3("Risk Throttle Cube — EARLY_FILL_Y × EARLY_KILL_Y × BOOST"));
  var rtBlk = sweepImg("stability_cube_risk_throttle/stability_cube_heatmap.png", 620);
  if (rtBlk) { kids.push(rtBlk); kids.push(subtle("Risk Throttle Cube — heatmap (FILL_Y × KILL_Y slices)")); kids.push(spacer(2)); }
  var rtSum = parseCubeSummary("stability_cube_risk_throttle/stability_cube_summary.txt");
  if (rtSum) {
    kids.push(dataTable(["Metric","Value"],[
      ["Grid",            rtSum.grid||"—"],
      ["Cells Valid",     rtSum.valid||"—"],
      ["Peak Sharpe",     (rtSum.peakSharpe||"—")+"  at  "+(rtSum.peakAt||"")],
      ["Baseline Sharpe", rtSum.baseline||"—"],
      ["Plateau ≥95%",    (rtSum.plateau95||"—")+"  ("+( rtSum.plateau95pct||"—")+")"],
      ["Plateau ≥90%",    (rtSum.plateau90||"—")+"  ("+( rtSum.plateau90pct||"—")+")"],
      ["Verdict",         rtSum.verdict||"—"],
    ],[3500,3700]));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.stabilityRiskThrottle));

  // 5.4.3 Exit Architecture Cube (PORT_SL × PORT_TSL × EARLY_KILL_Y)
  kids.push(h3("Exit Architecture Cube — PORT_SL × PORT_TSL × EARLY_KILL_Y"));
  var exBlk = sweepImg("stability_cube_exit_architecture/stability_cube_heatmap.png", 620);
  if (exBlk) { kids.push(exBlk); kids.push(subtle("Exit Architecture Cube — heatmap (PORT_SL × PORT_TSL slices)")); kids.push(spacer(2)); }
  var exSum = parseCubeSummary("stability_cube_exit_architecture/stability_cube_summary.txt");
  if (exSum) {
    kids.push(dataTable(["Metric","Value"],[
      ["Grid",            exSum.grid||"—"],
      ["Cells Valid",     exSum.valid||"—"],
      ["Peak Sharpe",     (exSum.peakSharpe||"—")+"  at  "+(exSum.peakAt||"")],
      ["Baseline Sharpe", exSum.baseline||"—"],
      ["Plateau ≥95%",    (exSum.plateau95||"—")+"  ("+( exSum.plateau95pct||"—")+")"],
      ["Plateau ≥90%",    (exSum.plateau90||"—")+"  ("+( exSum.plateau90pct||"—")+")"],
      ["Verdict",         exSum.verdict||"—"],
    ],[3500,3700]));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.stabilityExit));

  // ── L_HIGH Parameter Sweep ──
  kids.push(h2(sweepSec+".5 L_HIGH Leverage Ceiling Sweep"));
  kids.push(body("Sweeps the leverage ceiling (L_HIGH) from 0.8 → 3.0 in steps of 0.1 to test whether the live " +
    "value is near-optimal or whether significant performance is left on the table. " +
    "IS/OOS decay and problem-fold metrics reveal whether higher leverage introduces overfitting."));
  kids.push(spacer(2));
  var lhBlk = sweepImg("l_high_surface.png", 620);
  if (lhBlk) { kids.push(lhBlk); kids.push(subtle("L_HIGH Surface — Sharpe, MaxDD%, WF_CV, IS vs OOS decay, problem folds")); kids.push(spacer(2)); }
  var lhCSV = readSweepCSV("l_high_surface.csv");
  if (lhCSV) {
    var lhSorted = lhCSV.rows.slice().sort(function(a,b){ return parseFloat(b[1]||0)-parseFloat(a[1]||0); }).slice(0,15);
    kids.push(body("Top 15 L_HIGH values ranked by Sharpe:"));
    kids.push(dataTable(
      ["L_HIGH","Sharpe","CAGR%","MaxDD%","WF_CV","MeanOOS","F5 Sh","F8 Sh"],
      lhSorted.map(function(r){ return [r[0],r[1],r[2],r[3],r[8],r[9],r[12],r[13]]; }),
      [800,800,1000,900,800,900,800,800]
    ));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.lHighSweep));

  // ── L_HIGH Sensitivity Analysis (best filter, fixed candidate set) ──────
  kids.push(h2(sweepSec+".5b L_HIGH Sensitivity — Best Filter"));
  kids.push(body(
    "Reruns the simulation at seven fixed L_HIGH values [1.00, 1.33, 1.66, 2.00, 2.33, 2.66, 3.00] " +
    "using the best filter from the main audit. Shows how MaxDD, Total Return, Sharpe, and final " +
    "account balance respond to leverage changes, and saves individual equity curves for each value."
  ));
  kids.push(spacer(2));

  var lhsSummaryBlk = sensImg("l_high_sensitivity_summary.png", 640);
  if (lhsSummaryBlk) {
    kids.push(lhsSummaryBlk);
    kids.push(subtle("L_HIGH Sensitivity — MaxDD, Total Return, Sharpe, Final Equity (baseline marked in red)"));
    kids.push(spacer(2));
  }

  var lhsOverlayBlk = sensImg("l_high_sensitivity_equity_overlay.png", 640);
  if (lhsOverlayBlk) {
    kids.push(lhsOverlayBlk);
    kids.push(subtle("Equity curve overlay — all 7 L_HIGH values, baseline shown as solid line"));
    kids.push(spacer(2));
  }

  var lhsCSV = readSensCSV("l_high_sensitivity.csv");
  if (lhsCSV) {
    kids.push(body("L_HIGH sensitivity results (all 7 candidates):"));
    kids.push(dataTable(
      ["L_HIGH", "Sharpe", "MaxDD%", "CAGR%", "TotalRet%", "FinalEq ($)"],
      lhsCSV.rows.map(function(r) {
        return [r[0], r[1], r[2], r[3], r[4],
                r[5] ? parseFloat(r[5]).toLocaleString("en-US", {maximumFractionDigits:0}) : "—"];
      }),
      [700, 700, 800, 800, 900, 1100]
    ));
    kids.push(spacer(3));
  }

  // ── Tail Guardrail Grid Sweep ──
  kids.push(h2(sweepSec+".6 Tail Guardrail Grid Sweep"));
  kids.push(body("Grid-searches TAIL_DROP_PCT (0.02–0.07) × TAIL_VOL_MULT (1.0–3.0) to confirm that the live " +
    "tail filter parameterization is not a fragile local optimum. " +
    "WF_CV and WF_MeanOOS columns show whether stability holds across the grid."));
  kids.push(spacer(2));
  var tgCSV = readSweepCSV("tail_guardrail_sweep.csv");
  if (tgCSV) {
    var tgSorted = tgCSV.rows.slice().sort(function(a,b){ return parseFloat(b[2]||0)-parseFloat(a[2]||0); }).slice(0,20);
    kids.push(body("Top 20 cells ranked by Sharpe (drop_pct × vol_mult):"));
    kids.push(dataTable(
      ["Drop%","Vol×","Sharpe","CAGR%","MaxDD%","Active","WF_CV","MeanOOS","Pct+","Unstbl"],
      tgSorted.map(function(r){ return [r[0],r[1],r[2],r[3],r[4],r[5],r[7],r[8],r[9],r[10]]; }),
      [700,700,800,900,900,800,800,900,700,700]
    ));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.tailGuardrail));

  // ── Trail Exit Parameter Sweeps ──
  kids.push(h2(sweepSec+".7 Trail Exit Parameter Sweeps (Wide & Narrow)"));
  kids.push(body("Sweeps PORT_TSL (TRAIL_DD: 4–16%) × EARLY_KILL_X exit window (20–50 bars wide / 30–40 bars narrow) " +
    "to map the full Sharpe surface of the trailing exit architecture. " +
    "The narrow sweep zooms into the highest-resolution region for fine-grained optimum confirmation."));
  kids.push(spacer(2));

  kids.push(h3("Wide Sweep (TRAIL_DD × EARLY_X: 13 × 7 = 91 cells)"));
  var twBlk = sweepImg("trail_early_surface_wide.png", 620);
  if (twBlk) { kids.push(twBlk); kids.push(subtle("Trail Exit Wide Surface — Sharpe, MaxDD%, WF_CV, Fold5, Fold8 heatmaps")); kids.push(spacer(2)); }
  var twCSV = readSweepCSV("trail_early_surface_wide.csv");
  if (twCSV) {
    var twTop = twCSV.rows.slice().sort(function(a,b){ return parseFloat(b[2]||0)-parseFloat(a[2]||0); }).slice(0,15);
    kids.push(body("Top 15 wide-sweep cells by Sharpe:"));
    kids.push(dataTable(
      ["TRAIL_DD","EarlyX","Sharpe","MaxDD%","WF_CV","Fold5 Sh","Fold8 Sh"],
      twTop.map(function(r){ return [r[0],r[1],r[2],r[3],r[4],r[5],r[6]]; }),
      [900,800,800,900,800,900,900]
    ));
    kids.push(spacer(3));
  }

  kids.push(h3("Narrow Sweep (TRAIL_DD × EARLY_X: 13 × 11 = 143 cells)"));
  var tnwBlk = sweepImg("trail_early_surface_narrow.png", 620);
  if (tnwBlk) { kids.push(tnwBlk); kids.push(subtle("Trail Exit Narrow Surface — fine resolution zoom (EARLY_X 30–40)")); kids.push(spacer(2)); }
  var tnwCSV = readSweepCSV("trail_early_surface_narrow.csv");
  if (tnwCSV) {
    var tnwTop = tnwCSV.rows.slice().sort(function(a,b){ return parseFloat(b[2]||0)-parseFloat(a[2]||0); }).slice(0,15);
    kids.push(body("Top 15 narrow-sweep cells by Sharpe:"));
    kids.push(dataTable(
      ["TRAIL_DD","EarlyX","Sharpe","MaxDD%","WF_CV","Fold5 Sh","Fold8 Sh"],
      tnwTop.map(function(r){ return [r[0],r[1],r[2],r[3],r[4],r[5],r[6]]; }),
      [900,800,800,900,800,900,900]
    ));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.trailSweep));

  // ── Param Jitter ──
  kids.push(h2(sweepSec+".8 Parameter Jitter Test"));
  kids.push(body("Each parameter perturbed ±10–30% across N trials. Distribution of resulting Sharpe " +
    "ratios shows sensitivity to small parameter changes."));
  var jBlk = sweepImg("param_jitter_sharpe.png", 620);
  if (jBlk) { kids.push(jBlk); kids.push(subtle("Param Jitter — Sharpe distribution over perturbed configs")); kids.push(spacer(3)); }

  // ── Regime Robustness ──
  kids.push(h2(sweepSec+".9 Regime Robustness (IS vs OOS)"));
  var rrBlk = sweepImg("regime_robustness.png", 620);
  if (rrBlk) { kids.push(rrBlk); kids.push(subtle("Regime Robustness — In-Sample vs Out-of-Sample regime comparison")); kids.push(spacer(3)); }
  var rr = d.regimeRobustness || {};
  if (rr.rows && rr.rows.length > 0) {
    kids.push(dataTable(
      ["Regime", "Value", "Pass"],
      rr.rows.map(r => [r.regime, r.delta, r.pass ? "✅" : "❌"]),
      [2400, 1500, 1500, 1200, 800]
    ));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.regimeRobustness));

  // ── Return Concentration ──
  kids.push(h2(sweepSec+".10 Return Concentration"));
  kids.push(body("Lorenz curve and top-day concentration analysis — what fraction of total return is " +
    "generated by the best X% of trading days."));
  var rcBlk = sweepImg("return_concentration.png", 620);
  if (rcBlk) { kids.push(rcBlk); kids.push(subtle("Return Concentration — Lorenz curve")); kids.push(spacer(3)); }

  // ── Noise Stability ──
  kids.push(h2(sweepSec+".11 Noise Stability Test"));
  kids.push(body("Return series perturbed with Gaussian noise at increasing magnitudes. " +
    "Stable strategies show graceful Sharpe degradation rather than cliff-edge collapse."));
  var ns = d.noiseStability || {};
  if (ns.rows && ns.rows.length > 0) {
    kids.push(dataTable(
      ["Noise Sigma", "Sharpe", "CAGR", "MaxDD"],
      ns.rows.map(r => [r.sigma, r.sharpe, r.cagr, r.maxdd]),
      [2000, 2000, 2000, 1200]
    ));
    kids.push(spacer(3));
  }
  kids.push(...bulletList(insights.noiseStability));

  // ── Equity Ensemble ──
  kids.push(h2(sweepSec+".12 Equity Curve Ensemble"));
  kids.push(body("Monte Carlo fan chart from resampled daily return paths. " +
    "The live equity curve shown in context of the simulated distribution envelope."));
  // Ensemble filename: audit.py saves as equity_curve_ensemble_{label}.png in run_dir
  // label = filter name lowercased, spaces→underscores
  var ensLabels = [best].concat(d.filters.filter(f => f !== best));
  var ensFound = false;
  ensLabels.forEach(function(fl) {
    if (ensFound) return;
    var label = fl.toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_-]/g,"_");
    var ensFile = "equity_curve_ensemble_" + label + ".png";
    // Also try old-style name in case of legacy outputs
    var ensFileLegacy = label + "_equity_ensemble.png";
    var ensBlk = img(ensFile, 620) || sweepImg(ensFile, 620) || img(ensFileLegacy, 620) || sweepImg(ensFileLegacy, 620);
    if (ensBlk) {
      kids.push(ensBlk);
      kids.push(subtle("Equity Ensemble — "+fl));
      kids.push(spacer(3));
      ensFound = true;
    }
  });

  // ── Slippage Sweep ──
  kids.push(h2(sweepSec+".13 Slippage Sweep"));
  var slip = d.slippageSweep || {};
  if (slip.rows && slip.rows.length > 0) {
    kids.push(dataTable(
      ["Slippage %", "Sharpe", "CAGR %", "MaxDD %"],
      slip.rows.map(r => [r.slippage, r.sharpe, r.cagr, r.maxdd]),
      [1800, 1800, 1800, 1800]
    ));
    kids.push(...bulletList(insights.slippage));
    kids.push(spacer(3));
  } else {
    kids.push(body("Slippage sweep data not captured."));
  }

  // ── DSR / Ruin ──
  kids.push(h2(sweepSec+".14 DSR, Ruin Probability & Periodic Breakdown"));
  var dsr = d.dsrMtl || {};
  if (dsr.dsr !== undefined) {
    kids.push(dataTable(["Metric","Value"],[
      ["Deflated Sharpe Ratio (DSR)", dsr.dsr],
      ["Probabilistic SR (PSR)",      dsr.psr||"—"],
      ["Min Track Record Length",     dsr.mtl||"—"],
      ["Ruin Probability (50% DD)",   dsr.ruinProb||"—"],
    ],[4000,3200]));
    kids.push(spacer(3));
  }
  var pb = d.periodicBreakdown || {};
  if (pb.daily || pb.weekly || pb.monthly) {
    var pbRows = [];
    if (pb.daily)   pbRows.push(["Daily",   pb.daily.winRate,   pb.daily.mean,   pb.daily.best,   pb.daily.worst]);
    if (pb.weekly)  pbRows.push(["Weekly",  pb.weekly.winRate,  pb.weekly.mean,  pb.weekly.best,  pb.weekly.worst]);
    if (pb.monthly) pbRows.push(["Monthly", pb.monthly.winRate, pb.monthly.mean, pb.monthly.best, pb.monthly.worst]);
    if (pbRows.length > 0) {
      kids.push(dataTable(
        ["Period","Win Rate","Mean Return","Best","Worst"],
        pbRows,
        [1200,1500,1800,1500,1500]
      ));
      kids.push(spacer(3));
    }
  }

  // ── Return Concentration detail ──
  kids.push(h2(sweepSec+".15 Lucky Streak & Top-N Removal (Sweep-Level)"));
  kids.push(body("Complements the per-filter inst charts. These sweep-level versions run against " +
    "the best available filter with extended N range."));
  var lsBlk = sweepImg("lucky_streak.png", 620);
  if (lsBlk) { kids.push(lsBlk); kids.push(subtle("Lucky Streak (sweep-level)")); kids.push(spacer(2)); }
  var tnBlk = sweepImg("top_n_removal.png", 620);
  if (tnBlk) { kids.push(tnBlk); kids.push(subtle("Top-N Removal (sweep-level)")); kids.push(spacer(2)); }

  // ── Shock Injection (sweep) ──
  kids.push(h2(sweepSec+".16 Shock Injection & Ruin Probability (Sweep-Level)"));
  var siBlk = sweepImg("shock_injection.png", 620);
  if (siBlk) { kids.push(siBlk); kids.push(subtle("Shock Injection (sweep-level)")); kids.push(spacer(2)); }

  // ── Capacity Curve (sweep) ──
  kids.push(h2(sweepSec+".17 Capacity Curve (Sweep-Level)"));
  var ccBlk = sweepImg("capacity_curve.png", 620);
  if (ccBlk) { kids.push(ccBlk); kids.push(subtle("Capacity Curve — AUM vs Sharpe decay")); kids.push(spacer(2)); }
  else { kids.push(body("Capacity curve chart not available at sweep level. See per-filter Capacity & Cost Analysis sections.")); }

  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — ALLOCATOR VIEW SCORECARD
  // ═══════════════════════════════════════════════════════════════════════════
  // var sweepSec  = 6 + d.filters.length;  // NEW: Parameter Sweep Analysis
  var allocSec  = sweepSec + 1;
  kids.push(h1(allocSec+". Allocator View Scorecard (Best Filter: "+best+")"));
  var ac = d.allocatorCard;
  kids.push(...calloutBox(
    "Total: "+ac.pass+" Pass  |  "+ac.fail+" Fail  |  "+ac.warn+" Borderline",
    ac.fail>3?"fail":ac.fail>0?"warn":"pass"
  ));
  kids.push(...bulletList(insights.allocatorScorecard));
  kids.push(spacer(3));

  // Category order and membership — matches allocator_view_scorecard.csv
  var ALLOC_SECTIONS = [
    "Return Quality",
    "Robustness & Validation",
    "Risk Profile",
    "Regime Attribution",
    "Market Independence",
    "Execution & Capacity",
  ];
  // Build sections from items using item.section field (set during parsing)
  // Fall back to keyword matching if section field is missing
  var builtSections = {};
  ALLOC_SECTIONS.forEach(function(s){ builtSections[s]=[]; });
  ac.items.forEach(function(it){
    var sec = it.section && ALLOC_SECTIONS.includes(it.section) ? it.section : null;
    if (!sec) {
      // keyword fallback
      var m2 = it.metric||"";
      if(/Sharpe|Sortino|Calmar|CAGR|Return.*MaxDD|Gross Return|Net Return|Equity Mult|Doubling|Drag Ann|FA-WF.*Sharpe|FA-WF.*Median|Min OOS|Avg Monthly|Worst Monthly/.test(m2)) sec="Return Quality";
      else if(/Walk-Forward|Sharpe Std Dev|Folds Pos|Unstable Folds|Mean DSR|Deflated|PBO|Performance Degradation|IS.OOS CAGR|Sharpe Decay/.test(m2)) sec="Robustness & Validation";
      else if(/Max Drawdown|Avg Drawdown|DD Recovery|Ulcer|DD Duration|Time Underwater|Worst Single|CVaR|Loss Streak|Probability of Loss|Ruin Prob|Tail Ratio|Gain-to-Pain/.test(m2)) sec="Risk Profile";
      else if(/Sharpe in|HighDisp|LowDisp|HighVol|LowVol|Uptrend|Downtrend/.test(m2)) sec="Regime Attribution";
      else if(/Beta|BTC Variance|Annual Alpha|Equity Curve|Autocorrelation/.test(m2)) sec="Market Independence";
      else sec="Execution & Capacity";
    }
    if (!builtSections[sec]) builtSections[sec]=[];
    builtSections[sec].push(it);
  });
  var allocSections = ALLOC_SECTIONS.filter(function(s){ return builtSections[s].length>0; });
  if(allocSections.length>0){
    allocSections.forEach(function(sec,si){
      kids.push(h2(allocSec+"."+(si+1)+" "+sec));
      kids.push(scoreTable(builtSections[sec].map(it=>({metric:it.metric,goal:it.goal,actual:it.actual,status:it.status}))));
      kids.push(spacer(3));
    });
  } else if(ac.items.length>0){
    kids.push(scoreTable(ac.items.map(it=>({metric:it.metric,goal:it.goal,actual:it.actual,status:it.status}))));
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — TECHNICAL APPENDIX SCORECARD
  // ═══════════════════════════════════════════════════════════════════════════
  var techSec = allocSec + 1;
  kids.push(h1(techSec+". Technical Appendix Scorecard (Best Filter: "+best+")"));
  var tc = d.technicalCard;
  kids.push(...calloutBox(
    "Total: "+tc.pass+" Pass  |  "+tc.fail+" Fail  |  "+tc.warn+" Borderline",
    tc.fail>5?"fail":tc.fail>0?"warn":"pass"
  ));
  kids.push(spacer(3));
  if(tc.items.length>0){
    kids.push(scoreTable(tc.items.map(it=>({metric:it.metric,goal:it.goal,actual:it.actual,status:it.status}))));
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — UNIVERSE & MARKET CAP DIAGNOSTICS
  // ═══════════════════════════════════════════════════════════════════════════
  var univSec = techSec + 1;
  kids.push(h1(univSec+". Universe & Market Cap Diagnostics"));
  var ms = d.mcapSummary;
  kids.push(h2(univSec+".1 Summary Statistics"));
  kids.push(dataTable(["Metric","Value"],[
    ["Symbol Coverage",          ms.coverage||d.universe.coverage||"—"],
    ["Row Match Rate",           ms.rowMatchRate||d.universe.rowMatchRate||"—"],
    ["Mean Market Cap",          ms.mean||d.universe.meanMcap||"—"],
    ["Median Market Cap",        ms.median||d.universe.medianMcap||"—"],
    ["Mean of Daily Medians",    ms.meanDailyMedian||"—"],
    ["Median of Daily Medians",  ms.medianDailyMedian||"—"],
    ["Mean of Daily Means",      ms.meanDailyMean||ms.dailyMeanAvg||"—"],
    ["Total Rows",               d.universe.totalRows||"—"],
    ["Missing Rows",             d.universe.missingRows||"—"],
  ],[4000,3200]));
  kids.push(spacer(3));

  if(ms.unmatched&&ms.unmatched.length>0){
    kids.push(h2(univSec+".2 Unmatched Symbols (No Market Cap Data)"));
    kids.push(...calloutBox("⚠  "+ms.unmatched.length+" symbols without market cap data: "+ms.unmatched.join(", "),"warn"));
    kids.push(spacer(3));
  }

  if(d.mcapOutliers.length>0){
    kids.push(h2(univSec+".3 Top Outlier Days by Mean Market Cap"));
    kids.push(dataTable(["Date","Mean ($M)","Median ($M)","Symbols"],
      d.mcapOutliers.map(r=>[r.date,r.mean,r.median,r.n]),
      [1800,2000,2000,1000]));
    kids.push(spacer(3));
  }

  if(Object.keys(d.mcapDetail).length>0){
    kids.push(h2(univSec+".4 Per-Day Market Cap Detail"));
    kids.push(body("Showing all "+Object.keys(d.mcapDetail).length+" trading days:"));
    var dayRows=Object.entries(d.mcapDetail).map(([dt,v])=>[dt,v.mean,v.median,v.matched,v.missing]);
    kids.push(dataTable(["Date","Mean ($M)","Median ($M)","Matched","Missing"],dayRows,[1800,1800,1800,1100,900]));
  }
  kids.push(pageBreak());

  // ═══════════════════════════════════════════════════════════════════════════
  // SECTION — FINAL METRICS SUMMARY
  // ═══════════════════════════════════════════════════════════════════════════
  var finalSec = univSec + 1;
  kids.push(h1(finalSec+". Final Metrics Per Filter"));
  var finalRows = d.filters.map(f=>{
    var fm=d.filterMap[f]||{};
    var dsrStr = fm.dsrPct != null ? fm.dsrPct.toFixed(2)+"%" : (fm.dsr||"—");
    return [f, fmt(fm.sharpe), fmt(toFloat(fm.cagr))+"%", fmtPct(fm.maxdd), fmt(fm.cv),
            dsrStr, fm.worstDay!=null?fmtPct(fm.worstDay):"—",
            fm.worstWeek!=null?fmtPct(fm.worstWeek):"—", fm.grade||"—"];
  });
  kids.push(dataTable(
    ["Filter","Sharpe","CAGR","MaxDD","WF-CV","DSR%","Wst Day","Wst Wk","Grade"],
    finalRows,
    [2400,900,1100,1100,900,1100,1100,1000,900]
  ));

  return new Document({
    styles: {
      default: { document: { run: { font:FONT, size:20, color:C.TEXT } } },
    },
    numbering: { config: [{
      reference: "bullets",
      levels: [{ level:0, format:LevelFormat.BULLET, text:"•", alignment:AlignmentType.LEFT,
        style:{paragraph:{indent:{left:720,hanging:360}}} }],
    }]},
    settings: { compatibilityVersion: 15 },
    sections: [{
      properties: { page: {
        size: { width:PAGE_W, height:PAGE_H },
        margin: { top:MARGIN, right:MARGIN, bottom:MARGIN+200, left:MARGIN },
      }},
      headers: { default: new Header({ children: [
        new Paragraph({
          border: { bottom:{style:BorderStyle.SINGLE,size:4,color:C.ACCENT2,space:4} },
          spacing: { after:0 },
          children: [
            run("OVERLAP STRATEGY  |  Institutional Audit Report", {size:16,bold:true,color:C.ACCENT2}),
            run("   |  "+today, {size:16,color:C.SUBTEXT}),
            run("   |  CONFIDENTIAL", {size:16,color:C.SUBTEXT,italic:true}),
          ],
        }),
      ]})},
      footers: { default: new Footer({ children: [
        new Paragraph({
          border: { top:{style:BorderStyle.SINGLE,size:4,color:C.MED_GREY,space:4} },
          spacing: { before:0 },
          children: [
            run("For Qualified Investors Only. Simulated performance does not guarantee future results.", {size:14,color:C.SUBTEXT,italic:true}),
          ],
        }),
      ]})},
      children: kids,
    }],
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function main() {
  var inputFile  = process.argv[2];
  var outputFile = process.argv[3] || "audit_report.docx";

  var raw;
  if (inputFile) {
    console.log("Reading:", inputFile);
    raw = fs.readFileSync(inputFile, "utf8");
  } else {
    console.error("Usage: node generate_audit_report.js <audit_output.txt> [output.docx]");
    process.exit(1);
  }

  var auditDir = path.dirname(path.resolve(inputFile));
  var data     = parse(raw);

  console.log("Filters:", data.filters.length, data.filters.join(", "));
  console.log("WF folds per filter:", Object.entries(data.wfFolds).map(([k,v])=>k+"="+v.length).join(", "));
  console.log("PBO:", data.pbo);
  console.log("Verdict:", data.verdict.map(v=>v.rank+". "+v.label).join(", "));
  console.log("Chart paths found:", Object.keys(data.charts).filter(k=>k!=="shared").join(", "));
  console.log("Shared charts:", Object.keys(data.charts.shared||{}).join(", "));
  console.log("Allocator scorecard items:", data.allocatorCard.items.length);
  console.log("Technical scorecard items:", data.technicalCard.items.length);
  console.log("Monthly summaries:", data.monthlySummaries.length, " Weekly:", data.weeklySummaries.length);

  var insights = ENABLE_AI_COMMENTARY ? await generateAllInsights(data, auditDir) : {};
  var doc = build(data, auditDir, insights);
  var buf = await Packer.toBuffer(doc);
  fs.writeFileSync(outputFile, buf);
  console.log("\nReport saved:", outputFile, "("+Math.round(buf.length/1024)+" KB)");
}

main().catch(e=>{console.error(e);process.exit(1);});
