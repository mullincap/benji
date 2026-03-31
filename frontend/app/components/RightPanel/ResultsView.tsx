'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

import MetricCard from '../ui/MetricCard';
import FilterTable from '../ui/FilterTable';

interface ResultsViewProps {
  results: Record<string, unknown> | null;
  jobId?: string | null;
  startingCapital?: number | null;
  params?: Record<string, unknown> | null;
}

function fmtMetric(v: unknown, isInt = false): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return isInt ? String(Math.round(v)) : v.toFixed(3);
  return String(v);
}

function fmtCurrency(v: number): string {
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(v);
}

function fmtPercent2(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return `${v.toFixed(2)}%`;
  const n = Number(v);
  if (Number.isFinite(n)) return `${n.toFixed(2)}%`;
  return String(v);
}

function fmtSignedPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return 'N/A';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtDateLong(d: Date): string {
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function fmtDateShortYear(d: Date): string {
  const month = d.toLocaleDateString(undefined, { month: 'short' });
  const day = String(d.getDate()).padStart(2, '0');
  const yy = String(d.getFullYear()).slice(-2);
  return `${month} ${day} '${yy}`;
}

function asPct(v: unknown): number | null {
  const n = asNum(v);
  return n === null ? null : n;
}

function metricColor(key: string, value: unknown): string {
  if (typeof value !== 'number') return 'var(--t2)';
  const heuristics: Record<string, (v: number) => string> = {
    sharpe: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    cagr: (v) => (v > 0 ? 'var(--green)' : v > -5 ? 'var(--amber)' : 'var(--red)'),
    max_dd: (v) => (v > -20 ? 'var(--green)' : v > -30 ? 'var(--amber)' : 'var(--red)'),
    active: (v) => (v > 90 ? 'var(--green)' : v > 30 ? 'var(--amber)' : 'var(--red)'),
    tot_ret: (v) => (v > 0 ? 'var(--green)' : v > -10 ? 'var(--amber)' : 'var(--red)'),
    grade: (v) => (v >= 80 ? 'var(--green)' : v >= 65 ? 'var(--amber)' : 'var(--red)'),
    sortino: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar_ratio: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    omega: (v) => (v > 1.5 ? 'var(--green)' : v > 1 ? 'var(--amber)' : 'var(--red)'),
    ulcer_index: (v) => (v < 5 ? 'var(--green)' : v < 15 ? 'var(--amber)' : 'var(--red)'),
    fa_oos_sharpe: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    dsr_pct: (v) => (v > 95 ? 'var(--green)' : v > 80 ? 'var(--amber)' : 'var(--red)'),
    cv: (v) => (v < 0.25 ? 'var(--green)' : v < 0.5 ? 'var(--amber)' : 'var(--red)'),
    flat_days: (v) => (v < 30 ? 'var(--green)' : v < 60 ? 'var(--amber)' : 'var(--red)'),
  };
  return heuristics[key]?.(value) ?? 'var(--t0)';
}

type XValue = number | string | Date;
type Point = { x: XValue; y: number } | number;

type ChartPoint = { px: number; py: number; y: number; x?: XValue };

function parseDateLike(v: XValue | undefined): Date | null {
  if (v === undefined || v === null) return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v;
  if (typeof v === 'string') {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof v === 'number') {
    const ms = v > 1e12 ? v : v > 1e9 ? v * 1000 : null;
    if (!ms) return null;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

function fmtDateLabel(d: Date): string {
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function normalizeFilterLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/\+/g, 'p')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function extractAlerts(text: string): string[] {
  return text
    .split('\n')
    .filter((line) => /warning|error|failed|unavailable|exception/i.test(line))
    .filter((line) => line.trim().length > 0)
    .slice(-24);
}

function normalizeLoose(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function parseHeadingLine(line: string): string | null {
  if (/^FEES PANEL\b/i.test(line)) {
    return 'FEES PANEL';
  }
  if (!line.startsWith('┌─')) return null;
  return line.replace(/^┌─\s*/, '').replace(/\s*─+$/, '').trim();
}

type ParsedSection = { title: string; body: string };

function extractSelectedFilterAdvancedSections(text: string, selectedFilter: string | null): ParsedSection[] {
  if (!text || !selectedFilter) return [];
  const lines = text.split('\n');
  const headings: Array<{ idx: number; title: string }> = [];
  for (let i = 0; i < lines.length; i += 1) {
    const title = parseHeadingLine(lines[i].trim());
    if (title) headings.push({ idx: i, title });
  }
  if (headings.length === 0) return [];

  const filterVariants = [
    selectedFilter,
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
  ].map(normalizeLoose);

  const stressCandidates = headings.filter((h) => h.title.includes('STRESS TEST SUMMARY'));
  const matchingStress = stressCandidates.find((h) => filterVariants.some((fv) => fv.length > 0 && normalizeLoose(h.title).includes(fv)));
  const stressRef = matchingStress ?? stressCandidates[stressCandidates.length - 1];
  if (!stressRef) return [];

  const blockStart = [...headings]
    .reverse()
    .find((h) => h.idx < stressRef.idx && (h.title.includes('FEES PANEL') || h.title.includes('DAILY SERIES AUDIT')))?.idx ?? 0;
  const nextFeesIdx = headings.find((h) => h.idx > stressRef.idx && h.title.includes('FEES PANEL'))?.idx;
  const nextDailyAuditIdx = headings.find((h) => h.idx > stressRef.idx && h.title.includes('DAILY SERIES AUDIT'))?.idx;
  const runSummaryIdx = lines.findIndex((l, i) => i > blockStart && l.includes('RUN SUMMARY'));
  const endCandidates = [nextFeesIdx, nextDailyAuditIdx, runSummaryIdx >= 0 ? runSummaryIdx : undefined]
    .filter((v): v is number => typeof v === 'number' && v > blockStart)
    .sort((a, b) => a - b);
  const blockEnd = endCandidates[0] ?? lines.length;

  const blockHeadings = headings.filter((h) => h.idx >= blockStart && h.idx < blockEnd);
  const sections: ParsedSection[] = [];
  for (let i = 0; i < blockHeadings.length; i += 1) {
    const cur = blockHeadings[i];
    const next = blockHeadings[i + 1];
    const start = cur.idx + 1;
    const end = next ? next.idx : blockEnd;
    let sectionLines = lines.slice(start, end);
    if (cur.title.includes('BOTTOM LINE')) {
      const cutoff = sectionLines.findIndex((line) => {
        const t = line.trim();
        return (
          t.startsWith('════════')
          || t.startsWith('[equity overwrite]')
          || t.startsWith('SIGNAL PREDICTIVENESS')
          || t.startsWith('RUN INPUTS SUMMARY')
          || t.startsWith('OUTPUT FILES')
          || /^\d{4}-\d{2}-\d{2}/.test(t)
        );
      });
      if (cutoff >= 0) {
        sectionLines = sectionLines.slice(0, cutoff);
      }
    }
    const body = sectionLines.join('\n').trim();
    if (!body) continue;
    sections.push({ title: cur.title, body });
  }
  return sections;
}

function extractFullReportSections(text: string): ParsedSection[] {
  if (!text) return [];
  const lines = text.split('\n');
  const headings: Array<{ idx: number; title: string }> = [];
  for (let i = 0; i < lines.length; i += 1) {
    const title = parseHeadingLine(lines[i].trim());
    if (title) headings.push({ idx: i, title });
  }
  if (headings.length === 0) {
    return [{ title: 'FULL REPORT', body: text }];
  }
  const sections: ParsedSection[] = [];
  for (let i = 0; i < headings.length; i += 1) {
    const cur = headings[i];
    const next = headings[i + 1];
    const start = cur.idx + 1;
    const end = next ? next.idx : lines.length;
    const body = lines.slice(start, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: cur.title, body });
  }
  return sections;
}

function filterSectionsForSelectedFilter(sections: ParsedSection[], selectedFilter: string | null): ParsedSection[] {
  if (!selectedFilter) return sections;
  const variants = [
    selectedFilter,
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
    selectedFilter.replace(/^A\s*-\s*/i, ''),
  ]
    .map(normalizeLoose)
    .filter((v) => v.length > 0);
  const filtered = sections.filter((section) => {
    const hay = normalizeLoose(`${section.title}\n${section.body}`);
    return variants.some((v) => hay.includes(v));
  });
  return filtered.length > 0 ? filtered : sections;
}

function isDividerLine(line: string): boolean {
  const t = line.trim();
  return /^={20,}$/.test(t) || /^═{20,}$/.test(t);
}

function parseSignalFilterName(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  return parts.length >= 2 ? parts[1] : '';
}

function parseMilestoneFilterName(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  return parts.length >= 2 ? parts[1] : '';
}

function parseFilterFromDelimitedTitle(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) return parts[1];
  const paren = titleLine.match(/\(([^)]+)\)/);
  return paren ? paren[1] : '';
}

function canonicalizeFilterLabel(s: string): string {
  const t = normalizeLoose(s);
  if (!t) return '';
  if (t.includes('nofilter')) return 'no_filter';
  if (t.includes('calendarfilter')) return 'calendar';
  if (t.includes('tailguardrail')) return 'tail_guardrail';
  if (t.includes('tail') && t.includes('disp') && t.includes('vol')) return 'tail_disp_vol';
  if (t.includes('tail') && t.includes('vol') && t.includes('or')) return 'tail_vol_or';
  if (t.includes('tail') && t.includes('vol') && t.includes('and')) return 'tail_vol_and';
  if (t.includes('tail') && t.includes('blofin')) return 'tail_blofin';
  if (t.includes('tail') && (t.includes('dispersion') || t.includes('disp'))) return 'tail_dispersion';
  if (t === 'dispersion' || (t.includes('dispersion') && !t.includes('tail'))) return 'dispersion';
  if (t.includes('volatility') && !t.includes('tail')) return 'volatility';
  return t;
}

function matchesSelectedFilter(filterName: string, selectedFilter: string | null): boolean {
  if (!selectedFilter) return true;
  const canonA = canonicalizeFilterLabel(filterName);
  const canonB = canonicalizeFilterLabel(selectedFilter);
  if (canonA && canonB) return canonA === canonB;
  const variants = [
    selectedFilter,
    selectedFilter.replace(/^A\s*-\s*/i, ''),
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
  ].map(normalizeLoose);
  const hay = normalizeLoose(filterName);
  return variants.some((v) => v.length > 0 && (hay.includes(v) || v.includes(hay)));
}

function isSpecialSectionTitleLine(line: string): boolean {
  const t = line.trim();
  return (
    t.includes('WEEKLY MILESTONES')
    || t.includes('MONTHLY MILESTONES')
    || t.includes('SIGNAL PREDICTIVENESS')
    || t.includes('ALLOCATOR VIEW SCORECARD')
    || t.includes('TECHNICAL APPENDIX SCORECARD')
    || /^TAIL GUARDRAIL GRID SWEEP\b/i.test(t)
    || /^PARAMETER SWEEP\b/i.test(t)
    || /^L_HIGH SURFACE - RANKED BY SHARPE\b/i.test(t)
    || /^PARAMETER SURFACE MAP\b/i.test(t)
    || /^SLIPPAGE IMPACT SWEEP\b/i.test(t)
    || /^NOISE PERTURBATION STABILITY TEST\b/i.test(t)
    || /^PARAM JITTER \/ SHARPE STABILITY TEST\b/i.test(t)
    || /^RETURN CONCENTRATION ANALYSIS\b/i.test(t)
    || /^PERIODIC RETURN BREAKDOWN\b/i.test(t)
    || /^MINIMUM CUMULATIVE RETURN\b/i.test(t)
    || /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i.test(t)
    || /^SHOCK INJECTION TEST\s+\|\s+Filter:/i.test(t)
    || /^RUIN PROBABILITY\s+\|\s+Filter:/i.test(t)
    || /^LIQUIDITY CAPACITY CURVE\s+\|\s+Filter:/i.test(t)
    || /^PARAMETRIC STABILITY CUBE\b/i.test(t)
    || /^RISK THROTTLE STABILITY CUBE(?! SUMMARY)\b/i.test(t)
    || /^EXIT ARCHITECTURE STABILITY CUBE(?! SUMMARY)\b/i.test(t)
    || /^REGIME ROBUSTNESS TEST\s+\|\s+Filter:/i.test(t)
    || /^REGIME CONSISTENCY SUMMARY\b/i.test(t)
    || /^MARKET CAP DIAGNOSTIC\b/i.test(t)
    || /^MARKET CAP UNIVERSE SUMMARY\b/i.test(t)
  );
}

function findDividerBoundedEnd(lines: string[], start: number): number {
  for (let i = start + 1; i < lines.length; i += 1) {
    if (isDividerLine(lines[i])) {
      const after = (lines[i + 1] ?? '').trim();
      if (isSpecialSectionTitleLine(after) || /^OUTPUT FILES$/i.test(after) || /^RUN INPUTS SUMMARY$/i.test(after)) {
        return i;
      }
    }
  }
  return lines.length;
}

function extractSpecialFullReportSections(text: string, selectedFilter: string | null): ParsedSection[] {
  if (!text) return [];
  const lines = text.split('\n');
  const sections: ParsedSection[] = [];

  const signalStarts: number[] = [];
  for (let i = 0; i < lines.length; i += 1) {
    if (lines[i].includes('SIGNAL PREDICTIVENESS')) {
      signalStarts.push(i);
    }
  }
  for (let i = 0; i < signalStarts.length; i += 1) {
    const start = signalStarts[i];
    const nextSignal = signalStarts[i + 1] ?? lines.length;
    const line = lines[start].trim().replace(/\s{2,}/g, ' ');
    const filterName = parseSignalFilterName(line);
    if (!matchesSelectedFilter(filterName, selectedFilter)) continue;
    const end = Math.min(
      nextSignal,
      lines.findIndex((l, idx) => idx > start && l.includes('ALLOCATOR VIEW SCORECARD')) > -1
        ? lines.findIndex((l, idx) => idx > start && l.includes('ALLOCATOR VIEW SCORECARD'))
        : lines.length,
    );
    const body = lines.slice(start + 1, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: line, body });
  }

  const scorecardTitles = ['ALLOCATOR VIEW SCORECARD', 'TECHNICAL APPENDIX SCORECARD'];
  for (const scorecardTitle of scorecardTitles) {
    const start = lines.findIndex((l) => l.includes(scorecardTitle));
    if (start < 0) continue;
    let end = lines.length;
    for (let i = start + 1; i < lines.length; i += 1) {
      const t = lines[i].trim();
      if (t.includes('ALLOCATOR VIEW SCORECARD') || t.includes('TECHNICAL APPENDIX SCORECARD')) {
        end = i;
        break;
      }
      if (isDividerLine(t)) {
        const next = (lines[i + 1] ?? '').trim();
        if (/^OUTPUT FILES$/i.test(next) || /^RUN INPUTS SUMMARY$/i.test(next)) {
          end = i;
          break;
        }
      }
    }
    const body = lines.slice(start + 1, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: scorecardTitle, body });
  }

  const milestoneTitles = ['WEEKLY MILESTONES', 'MONTHLY MILESTONES'];
  for (const milestoneTitle of milestoneTitles) {
    const starts: number[] = [];
    for (let i = 0; i < lines.length; i += 1) {
      if (lines[i].includes(milestoneTitle)) starts.push(i);
    }
    let matchedForSelected = 0;
    for (const start of starts) {
      const titleLine = lines[start].trim().replace(/\s{2,}/g, ' ');
      const filterName = parseMilestoneFilterName(titleLine);
      const isMatch = matchesSelectedFilter(filterName, selectedFilter);
      if (!isMatch) continue;
      matchedForSelected += 1;
      const end = findDividerBoundedEnd(lines, start);
      const body = lines.slice(start + 1, end).join('\n').trim();
      if (!body) continue;
      sections.push({ title: titleLine, body });
    }
    // Do not fallback to all filters here; milestones should follow selected/best filter only.
    void matchedForSelected;
  }

  const sweepTitleMatchers = [
    /^TAIL GUARDRAIL GRID SWEEP\b/i,
    /^PARAMETER SWEEP\b/i,
    /^L_HIGH SURFACE - RANKED BY SHARPE\b/i,
    /^PARAMETER SURFACE MAP\b/i,
    /^SLIPPAGE IMPACT SWEEP\b/i,
    /^NOISE PERTURBATION STABILITY TEST\b/i,
    /^PARAM JITTER \/ SHARPE STABILITY TEST\b/i,
    /^RETURN CONCENTRATION ANALYSIS\b/i,
    /^PERIODIC RETURN BREAKDOWN\b/i,
    /^MINIMUM CUMULATIVE RETURN\b/i,
    /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i,
    /^SHOCK INJECTION TEST\s+\|\s+Filter:/i,
    /^RUIN PROBABILITY\s+\|\s+Filter:/i,
    /^LIQUIDITY CAPACITY CURVE\s+\|\s+Filter:/i,
    /^PARAMETRIC STABILITY CUBE\b/i,
    /^RISK THROTTLE STABILITY CUBE(?! SUMMARY)\b/i,
    /^EXIT ARCHITECTURE STABILITY CUBE(?! SUMMARY)\b/i,
    /^REGIME ROBUSTNESS TEST\s+\|\s+Filter:/i,
    /^REGIME CONSISTENCY SUMMARY\b/i,
    /^MARKET CAP DIAGNOSTIC\b/i,
    /^MARKET CAP UNIVERSE SUMMARY\b/i,
  ];
  const byFamilyMatched = new Map<number, ParsedSection[]>();
  const byFamilyAll = new Map<number, ParsedSection[]>();
  for (let i = 0; i < lines.length; i += 1) {
    const lineRaw = lines[i];
    const line = lineRaw.trim();
    const familyIdx = sweepTitleMatchers.findIndex((rx) => rx.test(line));
    if (familyIdx < 0) continue;
    const titleLine = line.replace(/\s{2,}/g, ' ');
    const end = findDividerBoundedEnd(lines, i);
    const body = lines.slice(i + 1, end).join('\n').trim();
    if (!body) continue;
    const parsed: ParsedSection = { title: titleLine, body };
    const parenFilter = line.match(/\(([^)]+?)\s+filter\)/i)?.[1] ?? '';
    const inlineFilter = line.match(/Filter:\s*([^|]+)$/i)?.[1]?.trim() ?? '';
    const filterName = parenFilter || inlineFilter || parseFilterFromDelimitedTitle(line);
    if (!filterName) {
      sections.push(parsed);
      continue;
    }
    const allForFamily = byFamilyAll.get(familyIdx) ?? [];
    allForFamily.push(parsed);
    byFamilyAll.set(familyIdx, allForFamily);
    if (!matchesSelectedFilter(filterName, selectedFilter)) continue;
    const matchedForFamily = byFamilyMatched.get(familyIdx) ?? [];
    matchedForFamily.push(parsed);
    byFamilyMatched.set(familyIdx, matchedForFamily);
  }
  for (const [familyIdx, allForFamily] of byFamilyAll.entries()) {
    const matchedForFamily = byFamilyMatched.get(familyIdx);
    const chosen = matchedForFamily && matchedForFamily.length > 0 ? matchedForFamily : allForFamily;
    sections.push(...chosen);
  }

  // Single-line section for equity ensemble output when only a chart path is emitted.
  for (const raw of lines) {
    const t = raw.trim();
    if (!/^Equity ensemble chart saved:/i.test(t)) continue;
    sections.push({
      title: 'EQUITY ENSEMBLE',
      body: t,
    });
  }
  const sharpeRidgeLines = lines
    .map((raw) => raw.trim())
    .filter((t) => /^Sharpe ridge map saved\s*:/i.test(t));
  if (sharpeRidgeLines.length > 0) {
    sections.push({
      title: 'SHARPE RIDGE MAP',
      body: sharpeRidgeLines.join('\n'),
    });
  }
  const sharpePlateauLines = lines
    .map((raw) => raw.trim())
    .filter((t) => /^Sharpe plateau detector\s*:/i.test(t));
  if (sharpePlateauLines.length > 0) {
    sections.push({
      title: 'SHARPE PLATEAU DETECTOR',
      body: sharpePlateauLines.join('\n'),
    });
  }

  return sections;
}

function buildFullReportSections(text: string, selectedFilter: string | null): ParsedSection[] {
  const all = extractFullReportSections(text);
  const stress = extractSelectedFilterAdvancedSections(text, selectedFilter);
  const signalAndScorecardsFromHeadings = filterSectionsForSelectedFilter(all, selectedFilter).filter((s) => {
    const t = s.title.toUpperCase();
    return (
      t.includes('SIGNAL PREDICTIVENESS')
      || t.includes('ALLOCATOR VIEW SCORECARD')
      || t.includes('TECHNICAL APPENDIX SCORECARD')
    );
  });
  const signalAndScorecardsFromText = extractSpecialFullReportSections(text, selectedFilter);
  const merged = [...stress, ...signalAndScorecardsFromHeadings, ...signalAndScorecardsFromText];
  const seen = new Set<string>();
  const deduped: ParsedSection[] = [];
  for (const s of merged) {
    const key = `${s.title}\n${s.body}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(s);
  }
  const folded: ParsedSection[] = [];
  for (const s of deduped) {
    const t = s.title.toUpperCase();
    const prev = folded[folded.length - 1];
    if (
      prev
      && t.startsWith('RISK THROTTLE STABILITY CUBE SUMMARY')
      && prev.title.toUpperCase().startsWith('RISK THROTTLE STABILITY CUBE')
    ) {
      prev.body = `${prev.body}\n\n${s.title}\n${s.body}`.trim();
      continue;
    }
    if (
      prev
      && t.startsWith('EXIT ARCHITECTURE STABILITY CUBE SUMMARY')
      && prev.title.toUpperCase().startsWith('EXIT ARCHITECTURE STABILITY CUBE')
    ) {
      prev.body = `${prev.body}\n\n${s.title}\n${s.body}`.trim();
      continue;
    }
    folded.push(s);
  }
  if (folded.length > 0) return folded;
  return filterSectionsForSelectedFilter(all, selectedFilter);
}

function syntheticDateAt(index: number, total: number): Date {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - (total - 1 - index));
  return d;
}

function makeTickIndices(total: number): number[] {
  if (total <= 1) return [0];
  const raw = [0, Math.floor((total - 1) * 0.25), Math.floor((total - 1) * 0.5), Math.floor((total - 1) * 0.75), total - 1];
  return Array.from(new Set(raw)).sort((a, b) => a - b);
}

function buildChartPoints(data: Point[], width: number, height: number, pad = 4): ChartPoint[] {
  if (!data || data.length === 0) return [];
  const values = data.map((d) => (typeof d === 'number' ? d : d.y));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return values.map((v, i) => {
    const x = pad + (i / (values.length - 1)) * (width - pad * 2);
    const y = height - pad - ((v - min) / range) * (height - pad * 2);
    return {
      px: x,
      py: y,
      y: v,
      x: typeof data[i] === 'number' ? undefined : (data[i] as { x: XValue; y: number }).x,
    };
  });
}

function pointsToPolyline(points: ChartPoint[]): string {
  return points.map((p) => `${p.px.toFixed(1)},${p.py.toFixed(1)}`).join(' ');
}

function CurveCard({
  title,
  data,
  color,
  gradientId,
  height = 160,
  fillAbove = false,
  valueFormatter,
  showTitle = true,
}: {
  title: string;
  data: Point[] | null | undefined;
  color: string;
  gradientId: string;
  height?: number;
  fillAbove?: boolean;
  valueFormatter?: (v: number) => string;
  showTitle?: boolean;
}) {
  const W = 480;
  const H = height;
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const points = useMemo(() => (data && data.length > 1 ? buildChartPoints(data, W, H) : []), [data, H]);
  const polyline = points.length > 1 ? pointsToPolyline(points) : null;
  const hoverPoint = hoverIdx !== null ? points[hoverIdx] : null;
  const tickIndices = useMemo(() => makeTickIndices(points.length), [points.length]);

  // Build area path from polyline
  let areaPath = '';
  if (polyline) {
    const pts = polyline.split(' ');
    const first = pts[0];
    const last = pts[pts.length - 1];
    const lastX = last.split(',')[0];
    const firstX = first.split(',')[0];
    if (fillAbove) {
      areaPath = `M ${first} ${pts.slice(1).map((p) => `L ${p}`).join(' ')} L ${lastX},0 L ${firstX},0 Z`;
    } else {
      areaPath = `M ${first} ${pts.slice(1).map((p) => `L ${p}`).join(' ')} L ${lastX},${H} L ${firstX},${H} Z`;
    }
  }

  return (
    <div
      style={{
        background: 'var(--bg2)',
        border: '1px solid var(--line)',
        borderRadius: 3,
        padding: 12,
        flex: 1,
        minWidth: 0,
        position: 'relative',
      }}
    >
      {showTitle && (
        <div
          style={{
            fontSize: 9,
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            color: 'var(--t3)',
            fontWeight: 700,
            marginBottom: 8,
          }}
        >
          {title}
        </div>
      )}
      {polyline ? (
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', height: H, display: 'block' }}
          preserveAspectRatio="none"
          onMouseMove={(e) => {
            if (!svgRef.current || points.length === 0) return;
            const rect = svgRef.current.getBoundingClientRect();
            const x = ((e.clientX - rect.left) / rect.width) * W;
            let bestIdx = 0;
            let bestDist = Math.abs(points[0].px - x);
            for (let i = 1; i < points.length; i += 1) {
              const dist = Math.abs(points[i].px - x);
              if (dist < bestDist) {
                bestDist = dist;
                bestIdx = i;
              }
            }
            setHoverIdx(bestIdx);
          }}
          onMouseLeave={() => setHoverIdx(null)}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.20" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
          </defs>
          {areaPath && <path d={areaPath} fill={`url(#${gradientId})`} />}
          <polyline
            points={polyline}
            fill="none"
            stroke={color}
            strokeWidth="1.5"
            vectorEffect="non-scaling-stroke"
          />
          {hoverPoint && (
            <>
              <line
                x1={hoverPoint.px}
                y1={0}
                x2={hoverPoint.px}
                y2={H}
                stroke="var(--line2)"
                strokeWidth="1"
                strokeDasharray="2 2"
              />
              <circle
                cx={hoverPoint.px}
                cy={hoverPoint.py}
                r={3}
                fill={color}
                stroke="var(--bg2)"
                strokeWidth="1"
              />
            </>
          )}
        </svg>
      ) : (
        <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ fontSize: 10, color: 'var(--t3)' }}>No chart data</span>
        </div>
      )}
      {hoverPoint && (
        <div
          style={{
            position: 'absolute',
            left: `${Math.min(92, Math.max(8, (hoverPoint.px / W) * 100))}%`,
            top: 30,
            transform: 'translateX(-50%)',
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            color: 'var(--t0)',
            fontSize: 9,
            padding: '4px 6px',
            borderRadius: 2,
            pointerEvents: 'none',
            whiteSpace: 'nowrap',
          }}
        >
          x: {fmtDateLabel(parseDateLike(hoverPoint.x) ?? syntheticDateAt(hoverIdx ?? 0, points.length || 1))}  y: {(valueFormatter ?? ((v) => v.toFixed(3)))(hoverPoint.y)}
        </div>
      )}
      {points.length > 1 && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginTop: 6,
            color: 'var(--t3)',
            fontSize: 9,
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          {tickIndices.map((idx) => {
            const p = points[idx];
            const d = parseDateLike(p?.x) ?? syntheticDateAt(idx, points.length);
            return <span key={idx}>{fmtDateLabel(d)}</span>;
          })}
        </div>
      )}
    </div>
  );
}

type FilterRow = Record<string, unknown> & {
  filter?: string;
  not_run?: boolean;
  sharpe?: number | null;
  cagr?: number | null;
  max_dd?: number | null;
  active?: number | null;
  wf_cv?: number | null;
  cv?: number | null;
  dsr_pct?: number | null;
  tot_ret?: number | null;
  grade?: string | null;
  grade_score?: number | null;
  equity_curve?: Point[];
  drawdown_curve?: Point[];
};
type ReportTab = 'summary' | 'stress_tests' | 'raw_output' | 'tear_sheet' | 'full_report';

function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

export default function ResultsView({ results, jobId, startingCapital, params }: ResultsViewProps) {
  const m = useMemo(() => ((results?.metrics ?? {}) as Record<string, unknown>), [results]);
  const equityCurve = (m.equity_curve ?? results?.equity_curve) as Point[] | null | undefined;
  const drawdownCurve = (m.drawdown_curve ?? results?.drawdown_curve) as Point[] | null | undefined;
  const filterComparison = (m.filter_comparison ?? results?.filter_comparison) as FilterRow[] | null | undefined;
  const perFilterMetrics = (m.filters ?? []) as FilterRow[];
  const hints = (m.hints ?? results?.hints) as string[] | null | undefined;
  const filterComparisonWithExpected: FilterRow[] = [...(filterComparison ?? [])];
  if (params) {
    const expected: Array<{ key: string; label: string; aliases: string[] }> = [
      { key: 'run_filter_none', label: 'A - No Filter', aliases: ['A - No Filter'] },
      { key: 'run_filter_calendar', label: 'A - Calendar Filter', aliases: ['A - Calendar Filter'] },
      { key: 'run_filter_tail', label: 'A - Tail Guardrail', aliases: ['A - Tail Guardrail'] },
      { key: 'run_filter_dispersion', label: 'A - Dispersion', aliases: ['A - Dispersion'] },
      { key: 'run_filter_tail_disp', label: 'A - Tail + Dispersion', aliases: ['A - Tail + Dispersion', 'A - Tail p Dispersion'] },
      { key: 'run_filter_vol', label: 'A - Volatility', aliases: ['A - Volatility'] },
      { key: 'run_filter_tail_disp_vol', label: 'A - Tail + Disp + Vol', aliases: ['A - Tail + Disp + Vol', 'A - Tail p Disp p Vol'] },
      { key: 'run_filter_tail_or_vol', label: 'A - Tail + Vol (OR)', aliases: ['A - Tail + Vol (OR)', 'A - Tail p Vol (OR'] },
      { key: 'run_filter_tail_and_vol', label: 'A - Tail + Vol (AND)', aliases: ['A - Tail + Vol (AND)', 'A - Tail p Vol (AND'] },
      { key: 'run_filter_tail_blofin', label: 'A - Tail + Blofin', aliases: ['A - Tail + Blofin', 'A - Tail p Blofin'] },
    ];
    const existing = new Set(
      filterComparisonWithExpected.map((r) => normalizeFilterLabel(String(r.filter ?? '').trim())),
    );
    for (const entry of expected) {
      if (!params[entry.key]) continue;
      const hasAlias = entry.aliases.some((alias) => existing.has(normalizeFilterLabel(alias)));
      if (!hasAlias) {
        filterComparisonWithExpected.push({ filter: entry.label, not_run: true });
      }
    }
  }
  const dispersionRequested = !!params?.run_filter_dispersion || !!params?.run_filter_tail_disp || !!params?.run_filter_tail_disp_vol;
  const dispersionMissing =
    filterComparisonWithExpected.some((r) => r.filter === 'A - Dispersion' && r.not_run)
    || filterComparisonWithExpected.some((r) => r.filter === 'A - Tail + Dispersion' && r.not_run)
    || filterComparisonWithExpected.some((r) => r.filter === 'A - Tail + Disp + Vol' && r.not_run);
  const showDispersionVpnHint = dispersionRequested && dispersionMissing;
  const runStartingCapital =
    typeof results?.starting_capital === 'number'
      ? results.starting_capital
      : typeof startingCapital === 'number'
        ? startingCapital
        : 100000;

  const mergedFilters: FilterRow[] = (() => {
    const byLabel = new Map<string, FilterRow>();
    for (const pf of perFilterMetrics) {
      const key = normalizeFilterLabel(String(pf.filter ?? ''));
      if (key) byLabel.set(key, pf);
    }
    return filterComparisonWithExpected.map((row) => {
      const key = normalizeFilterLabel(String(row.filter ?? ''));
      const pf = byLabel.get(key);
      if (!pf) return row;
      return { ...row, ...pf, filter: String(pf.filter ?? row.filter ?? '') };
    });
  })();

  const [manualSelectedFilter, setManualSelectedFilter] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<ReportTab>('summary');
  const [auditOutput, setAuditOutput] = useState<string>('');
  const [outputLoading, setOutputLoading] = useState(false);
  const [outputError, setOutputError] = useState<string | null>(null);
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>('idle');
  const [tearTemplate, setTearTemplate] = useState('');
  const defaultSelectedFilter = (() => {
    if (mergedFilters.length === 0) return null;
    const candidates = mergedFilters.filter((r) => !r.not_run);
    const ranked = [...(candidates.length > 0 ? candidates : mergedFilters)].sort(
      (a, b) => (asNum(b.sharpe) ?? Number.NEGATIVE_INFINITY) - (asNum(a.sharpe) ?? Number.NEGATIVE_INFINITY),
    );
    return String(ranked[0]?.filter ?? mergedFilters[0]?.filter ?? '');
  })();

  const selectedFilter =
    manualSelectedFilter
      && mergedFilters.some((r) => normalizeFilterLabel(String(r.filter ?? '')) === normalizeFilterLabel(manualSelectedFilter))
      ? manualSelectedFilter
      : defaultSelectedFilter;
  const selectedRow =
    selectedFilter
      ? mergedFilters.find((r) => normalizeFilterLabel(String(r.filter ?? '')) === normalizeFilterLabel(selectedFilter)) ?? null
      : null;
  const alertLines = useMemo(() => extractAlerts(auditOutput), [auditOutput]);
  const advancedSections = useMemo(
    () => extractSelectedFilterAdvancedSections(auditOutput, selectedFilter),
    [auditOutput, selectedFilter],
  );
  const fullReportSections = useMemo(() => {
    return buildFullReportSections(auditOutput, selectedFilter);
  }, [auditOutput, selectedFilter]);

  const selectedEquityCurve = ((selectedRow?.equity_curve as Point[] | undefined) ?? equityCurve) as Point[] | null | undefined;
  const selectedDrawdownCurve = ((selectedRow?.drawdown_curve as Point[] | undefined) ?? drawdownCurve) as Point[] | null | undefined;
  const selectedTotalDays = selectedEquityCurve?.length ?? 0;
  const selectedActiveDays = asNum(selectedRow?.active);
  const scorecard = useMemo(
    () => ((results?.scorecard ?? []) as Array<{ label?: string; status?: string; value?: unknown }>),
    [results],
  );
  const activeDaysMain = (
    selectedActiveDays !== null && Number.isFinite(selectedActiveDays)
  ) ? String(Math.round(selectedActiveDays)) : fmtMetric(selectedRow?.active, true);
  const activeDaysSuffix = (
    selectedActiveDays !== null
    && Number.isFinite(selectedActiveDays)
    && selectedTotalDays > 0
  ) ? `/${selectedTotalDays} days` : undefined;
  const selectedActivePct = (
    selectedActiveDays !== null
    && Number.isFinite(selectedActiveDays)
    && selectedTotalDays > 0
  ) ? (selectedActiveDays / selectedTotalDays) * 100 : null;
  const equityCurveDollars = selectedEquityCurve?.map((p) => (
    typeof p === 'number'
      ? p * runStartingCapital
      : { ...p, y: p.y * runStartingCapital }
  ));

  useEffect(() => {
    let mounted = true;
    fetch('/tear_sheet12.html')
      .then((r) => (r.ok ? r.text() : ''))
      .then((txt) => {
        if (mounted) setTearTemplate(txt);
      })
      .catch(() => {
        if (mounted) setTearTemplate('');
      });
    return () => {
      mounted = false;
    };
  }, []);

  const tearSheetHtml = useMemo(() => {
    if (!tearTemplate) return '';
    const eq = (equityCurveDollars ?? []).map((p, idx) => {
      if (typeof p === 'number') return { d: syntheticDateAt(idx, (equityCurveDollars ?? []).length), y: p };
      return { d: parseDateLike(p.x) ?? syntheticDateAt(idx, (equityCurveDollars ?? []).length), y: p.y };
    }).filter((p) => Number.isFinite(p.y));
    if (eq.length < 2) return tearTemplate;

    const first = eq[0];
    const last = eq[eq.length - 1];
    const days = eq.length;
    const totRet = ((last.y / first.y) - 1) * 100;
    const cagr = asPct(selectedRow?.cagr ?? m.cagr) ?? 0;
    const sharpe = asNum(selectedRow?.sharpe ?? m.sharpe) ?? 0;
    const sortino = asNum(m.sortino) ?? 0;
    const calmar = asNum(m.calmar ?? m.calmar_ratio) ?? 0;
    const maxDd = asPct(selectedRow?.max_dd ?? m.max_drawdown) ?? 0;
    const dsr = asPct(selectedRow?.dsr_pct ?? m.dsr_pct) ?? 0;
    const cv = asNum(selectedRow?.wf_cv ?? selectedRow?.cv ?? m.cv) ?? 0;
    const grade = asNum(selectedRow?.grade_score ?? m.grade_score ?? selectedRow?.grade);
    const activeDays = Math.round(asNum(selectedRow?.active) ?? 0);
    const dailyRets: number[] = [];
    for (let i = 1; i < eq.length; i += 1) {
      dailyRets.push((eq[i].y / eq[i - 1].y) - 1);
    }
    const bestDay = dailyRets.length > 0 ? Math.max(...dailyRets) * 100 : 0;
    const worstDay = dailyRets.length > 0 ? Math.min(...dailyRets) * 100 : 0;
    const avgDaily = dailyRets.length > 0 ? (dailyRets.reduce((a, b) => a + b, 0) / dailyRets.length) * 100 : 0;

    const monthlyMap = new Map<string, { d: Date; first: number; last: number }>();
    for (const p of eq) {
      const key = `${p.d.getFullYear()}-${String(p.d.getMonth() + 1).padStart(2, '0')}`;
      if (!monthlyMap.has(key)) monthlyMap.set(key, { d: new Date(p.d), first: p.y, last: p.y });
      const cur = monthlyMap.get(key)!;
      cur.last = p.y;
      cur.d = new Date(p.d);
    }
    const monthlyRows = Array.from(monthlyMap.values()).map((v) => ({
      label: v.d.toLocaleDateString(undefined, { month: 'short', year: '2-digit' }).replace(' ', " '"),
      pct: ((v.last / v.first) - 1) * 100,
    }));
    const monthlyWinRate = monthlyRows.length > 0 ? (monthlyRows.filter((r) => r.pct > 0).length / monthlyRows.length) * 100 : 0;
    const avgMonthly = monthlyRows.length > 0 ? monthlyRows.reduce((a, b) => a + b.pct, 0) / monthlyRows.length : 0;

    const sampleN = Math.min(58, eq.length);
    const weeklyData = Array.from({ length: sampleN }).map((_, i) => {
      const idx = Math.round((i / Math.max(1, sampleN - 1)) * (eq.length - 1));
      return { date: fmtDateShortYear(eq[idx].d), bal: Math.round(eq[idx].y) };
    });
    const btcWeekly = weeklyData.map((w, i) => Math.round(runStartingCapital * (1 + (((w.bal / runStartingCapital) - 1) * 0.3) * (i / Math.max(1, weeklyData.length - 1)))));

    const passCount = scorecard.filter((s) => s.status === 'pass').length;
    const warnCount = scorecard.filter((s) => s.status === 'warn').length;
    const failCount = scorecard.filter((s) => s.status && s.status !== 'pass' && s.status !== 'warn').length;
    const naCount = Math.max(0, scorecard.length - passCount - warnCount - failCount);
    const passPct = scorecard.length ? (passCount / scorecard.length) * 100 : 0;
    const failPct = scorecard.length ? (failCount / scorecard.length) * 100 : 0;
    const warnPct = scorecard.length ? (warnCount / scorecard.length) * 100 : 0;
    const naPct = Math.max(0, 100 - passPct - failPct - warnPct);
    let html = tearTemplate;
    html = html.replace(/const weeklyData = \[[\s\S]*?\];/, `const weeklyData = ${JSON.stringify(weeklyData)};`);
    html = html.replace(/const btcWeekly = \[[\s\S]*?\];/, `const btcWeekly = ${JSON.stringify(btcWeekly)};`);
    html = html.replace(/<!-- CONFIG STRIP -->[\s\S]*?<!-- FOOTER -->/, '<!-- FOOTER -->');

    const injected = `
<script>
(() => {
  const setText = (sel, txt) => { const el = document.querySelector(sel); if (el) el.textContent = txt; };
  setText('.strategy-name', ${JSON.stringify(selectedFilter ?? 'Selected Filter')});
  setText('.header-sub', ${JSON.stringify(`${fmtDateLong(first.d)} - ${fmtDateLong(last.d)} · ${days} Calendar Days (${(days / 365).toFixed(2)} yrs) · ${new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(runStartingCapital)} Starting Capital`)});
  setText('.as-of-stamp span', ${JSON.stringify(fmtDateLong(last.d))});
  setText('.run-id', ${JSON.stringify(`RUN: ${jobId ?? 'N/A'}`)});
  const hero = document.querySelectorAll('.hero-card');
  const heroVals = [
    ${JSON.stringify(fmtSignedPct(avgMonthly, 2))},
    ${JSON.stringify(fmtPercent2(cagr))},
    ${JSON.stringify(fmtMetric(sharpe))},
    ${JSON.stringify(fmtMetric(sortino))},
    ${JSON.stringify(fmtPercent2(maxDd))},
    ${JSON.stringify(fmtMetric(calmar))}
  ];
  hero.forEach((h, i) => {
    const v = h.querySelector('.hero-value');
    if (v && heroVals[i]) v.textContent = heroVals[i];
  });
  setText('.chart-title-left', ${JSON.stringify(`Equity Curve - ${selectedFilter ?? 'Selected Filter'}`)});
  const cs = document.querySelectorAll('.chart-stats span');
  if (cs[0]) cs[0].textContent = ${JSON.stringify(new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(runStartingCapital))};
  if (cs[1]) cs[1].textContent = ${JSON.stringify(new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(last.y))};
  if (cs[2]) cs[2].textContent = ${JSON.stringify(fmtMetric(sharpe))};
  const rows = document.querySelectorAll('.stat-row');
  rows.forEach((r) => {
    const key = (r.querySelector('.stat-key')?.textContent || '').trim().toLowerCase();
    const val = r.querySelector('.stat-val');
    if (!val) return;
    if (key.includes('cumulative net return')) val.textContent = ${JSON.stringify(fmtSignedPct(totRet, 2))};
    else if (key.includes('cagr (annualised)')) val.textContent = ${JSON.stringify(fmtPercent2(cagr))};
    else if (key.includes('final equity')) val.textContent = ${JSON.stringify(new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(last.y))};
    else if (key.includes('best single day')) val.textContent = ${JSON.stringify(fmtSignedPct(bestDay, 2))};
    else if (key.includes('worst single day')) val.textContent = ${JSON.stringify(fmtSignedPct(worstDay, 2))};
    else if (key.includes('avg daily return')) val.textContent = ${JSON.stringify(fmtSignedPct(avgDaily, 2))};
    else if (key.includes('avg monthly return')) val.textContent = ${JSON.stringify(fmtSignedPct(avgMonthly, 2))};
    else if (key === 'sharpe ratio') val.textContent = ${JSON.stringify(fmtMetric(sharpe))};
    else if (key === 'sortino ratio') val.textContent = ${JSON.stringify(fmtMetric(sortino))};
    else if (key === 'calmar ratio') val.textContent = ${JSON.stringify(fmtMetric(calmar))};
    else if (key.includes('max drawdown')) val.textContent = ${JSON.stringify(fmtPercent2(maxDd))};
    else if (key.includes('active days')) val.textContent = ${JSON.stringify(`${activeDays} / ${days} (${days > 0 ? ((activeDays / days) * 100).toFixed(2) : '0.00'}%)`)};
    else if (key.includes('deflated sharpe ratio')) val.textContent = ${JSON.stringify(fmtPercent2(dsr))};
    else if (key.includes('walk-forward cv')) val.textContent = ${JSON.stringify(fmtMetric(cv))};
  });
  const moGrid = document.querySelector('.monthly-grid');
  if (moGrid) {
    moGrid.innerHTML = ${JSON.stringify(
      monthlyRows.slice(-14).map((m) => {
        const cls = m.pct >= 0 ? 'mo-pos' : 'mo-neg';
        return `<div class="mo-cell ${cls}"><div class="mo-label">${m.label}</div><div class="mo-val">${m.pct >= 0 ? '+' : ''}${m.pct.toFixed(2)}%</div></div>`;
      }).join('')
    )};
  }
  const moHdr = document.querySelector('.monthly-sec .sec-title span:last-child');
  if (moHdr) {
    moHdr.textContent = ${JSON.stringify(`${monthlyRows.filter((r) => r.pct > 0).length} positive | ${monthlyRows.filter((r) => r.pct <= 0).length} negative | ${monthlyWinRate.toFixed(1)}% win rate`)};
    moHdr.style.whiteSpace = 'nowrap';
  }
  const sb = document.querySelectorAll('.scorecard-bar .sb-track > div');
  if (sb[0]) sb[0].style.width = ${JSON.stringify(`${passPct.toFixed(1)}%`)};
  if (sb[1]) sb[1].style.width = ${JSON.stringify(`${failPct.toFixed(1)}%`)};
  if (sb[2]) sb[2].style.width = ${JSON.stringify(`${warnPct.toFixed(1)}%`)};
  if (sb[3]) sb[3].style.width = ${JSON.stringify(`${naPct.toFixed(1)}%`)};
  const sbNum = document.querySelectorAll('.scorecard-bar .sb-num span:last-child');
  if (sbNum[0]) sbNum[0].textContent = ${JSON.stringify(`${passCount} Pass`)};
  if (sbNum[1]) sbNum[1].textContent = ${JSON.stringify(`${failCount} Fail`)};
  if (sbNum[2]) sbNum[2].textContent = ${JSON.stringify(`${warnCount} Borderline`)};
  if (sbNum[3]) sbNum[3].textContent = ${JSON.stringify(`${naCount} N/A`)};
  if (${String(grade !== null)}) {
    const badge = document.querySelector('.header-badges');
    if (badge) {
      const el = document.createElement('span');
      el.className = 'badge badge-gold';
      el.textContent = 'Grade ${grade !== null ? Math.round(grade) : ''}';
      badge.appendChild(el);
    }
  }
})();
</script>`;

    html = html.replace('</body>', `${injected}</body>`);
    return html;
  }, [tearTemplate, equityCurveDollars, selectedRow, m, scorecard, selectedFilter, runStartingCapital, jobId]);

  const metricCards = selectedRow
    ? [
      { label: 'Sharpe', key: 'sharpe', value: fmtMetric(selectedRow.sharpe), colorValue: selectedRow.sharpe },
      { label: 'CAGR %', key: 'cagr', value: fmtPercent2(selectedRow.cagr), colorValue: selectedRow.cagr },
      { label: 'Max DD %', key: 'max_dd', value: fmtPercent2(selectedRow.max_dd), colorValue: selectedRow.max_dd },
      {
        label: 'Active Days',
        key: 'active',
        value: activeDaysMain,
        unit: activeDaysSuffix,
        unitColor: 'var(--t1)',
        secondary: selectedActivePct !== null ? `(${selectedActivePct.toFixed(2)}%)` : undefined,
        colorValue: selectedRow.active,
      },
      { label: 'WF-CV', key: 'cv', value: fmtMetric((selectedRow.wf_cv ?? selectedRow.cv) as unknown), colorValue: (selectedRow.wf_cv ?? selectedRow.cv) },
      { label: 'DSR %', key: 'dsr_pct', value: fmtPercent2(selectedRow.dsr_pct), colorValue: selectedRow.dsr_pct },
      { label: 'Sum of Daily Return %', key: 'tot_ret', value: fmtPercent2(selectedRow.tot_ret), colorValue: selectedRow.tot_ret },
      { label: 'Grade', key: 'grade', value: selectedRow.grade_score != null ? String(selectedRow.grade_score) : String(selectedRow.grade ?? 'N/A'), colorValue: selectedRow.grade_score },
    ]
    : [
      { label: 'Sortino', key: 'sortino', value: fmtMetric(m.sortino), colorValue: m.sortino },
      { label: 'Calmar', key: 'calmar', value: fmtMetric(m.calmar ?? m.calmar_ratio), colorValue: m.calmar ?? m.calmar_ratio },
      { label: 'Omega', key: 'omega', value: fmtMetric(m.omega), colorValue: m.omega },
      { label: 'Ulcer Index', key: 'ulcer_index', value: fmtMetric(m.ulcer_index), colorValue: m.ulcer_index },
      { label: 'FA-OOS Sharpe', key: 'fa_oos_sharpe', value: fmtMetric(m.fa_oos_sharpe), colorValue: m.fa_oos_sharpe },
      { label: 'DSR %', key: 'dsr_pct', value: fmtPercent2(m.dsr_pct), colorValue: m.dsr_pct },
      { label: 'WF-CV', key: 'cv', value: fmtMetric(m.cv), colorValue: m.cv },
      { label: 'Flat Days', key: 'flat_days', value: fmtMetric(m.flat_days, true), colorValue: m.flat_days },
    ];
  if (!results) {
    return (
      <div style={{ padding: 16, color: 'var(--t3)', fontSize: 10 }}>
        No results available.
      </div>
    );
  }

  async function ensureAuditOutputLoaded() {
    if (!jobId || auditOutput || outputLoading) return;
    setOutputLoading(true);
    setOutputError(null);
    try {
      const res = await fetch(`http://localhost:8000/api/jobs/${jobId}/output`);
      if (!res.ok) throw new Error(`GET /api/jobs/${jobId}/output failed: ${res.status}`);
      const data = (await res.json()) as { text?: string };
      setAuditOutput(typeof data.text === 'string' ? data.text : '');
    } catch (err) {
      setOutputError(String(err));
    } finally {
      setOutputLoading(false);
    }
  }

  async function copyRawOutput() {
    try {
      await navigator.clipboard.writeText(auditOutput || '');
      setCopyState('copied');
      setTimeout(() => setCopyState('idle'), 1400);
    } catch {
      setCopyState('error');
      setTimeout(() => setCopyState('idle'), 1800);
    }
  }

  return (
    <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          position: 'sticky',
          top: 0,
          zIndex: 20,
          background: 'var(--bg0)',
          paddingTop: 8,
          paddingBottom: 8,
          borderBottom: '1px solid var(--line)',
        }}
      >
        <div
          style={{
            fontSize: 10,
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: 'var(--t2)',
            fontWeight: 700,
            whiteSpace: 'nowrap',
          }}
        >
          Audit id: {jobId ?? 'N/A'}
        </div>
        <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
          {([
            ['summary', 'SUMMARY'],
            ['tear_sheet', 'Tear Sheet'],
            ['full_report', 'Full Report'],
            ['raw_output', 'Raw Output'],
          ] as Array<[ReportTab, string]>).map(([tab, label]) => (
            <button
              key={tab}
              onClick={async () => {
                setActiveTab(tab);
                if (tab !== 'summary') await ensureAuditOutputLoaded();
              }}
              style={{
                height: 28,
                padding: '0 10px',
                borderRadius: 3,
                border: `1px solid ${activeTab === tab ? 'var(--green)' : 'var(--line2)'}`,
                background: activeTab === tab ? 'var(--green-dim)' : 'var(--bg1)',
                color: activeTab === tab ? 'var(--green)' : 'var(--t2)',
                fontSize: 9,
                letterSpacing: '0.1em',
                fontWeight: 700,
                cursor: 'pointer',
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {activeTab === 'summary' && (
        <>
          {/* Metric cards 4×2 grid */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(4, 1fr)',
              gap: 8,
            }}
          >
            {metricCards.map(({ label, key, value, colorValue, secondary, unit, unitColor }) => (
              <MetricCard
                key={key}
                label={label}
                value={value}
                unit={unit}
                unitColor={unitColor}
                secondary={secondary}
                color={metricColor(key, colorValue)}
              />
            ))}
          </div>

          {/* Full-width stacked charts */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <details open style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}>
              <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
                Equity Curve ($)
              </summary>
              <div style={{ marginTop: 8 }}>
                <CurveCard
                  title="Equity Curve ($)"
                  data={equityCurveDollars}
                  color="#00c896"
                  gradientId="equity-gradient"
                  height={480}
                  valueFormatter={fmtCurrency}
                  showTitle={false}
                />
              </div>
            </details>

            <details open style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}>
              <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
                Drawdown Curve
              </summary>
              <div style={{ marginTop: 8 }}>
                <CurveCard
                  title="Drawdown Curve"
                  data={selectedDrawdownCurve}
                  color="#ff4d4d"
                  gradientId="drawdown-gradient"
                  fillAbove
                  showTitle={false}
                />
              </div>
            </details>
          </div>

          <details open style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}>
            <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
              Filter Comparison
            </summary>
            <div style={{ marginTop: 8 }}>
              {(hints && hints.length > 0) && (
                <div
                  style={{
                    marginBottom: 10,
                    fontSize: 10,
                    color: 'var(--amber)',
                    background: 'rgba(255, 186, 77, 0.09)',
                    border: '1px solid rgba(255, 186, 77, 0.35)',
                    borderRadius: 3,
                    padding: '7px 9px',
                    lineHeight: 1.45,
                  }}
                >
                  {hints.map((h, i) => <div key={i}>{h}</div>)}
                </div>
              )}
              {showDispersionVpnHint && (!hints || hints.length === 0) && (
                <div
                  style={{
                    marginBottom: 10,
                    fontSize: 10,
                    color: 'var(--amber)',
                    background: 'rgba(255, 186, 77, 0.09)',
                    border: '1px solid rgba(255, 186, 77, 0.35)',
                    borderRadius: 3,
                    padding: '7px 9px',
                    lineHeight: 1.45,
                  }}
                >
                  Dispersion filters were requested but did not run. If Binance API access is geo-blocked, turn on VPN and re-run.
                </div>
              )}
              <FilterTable
                rows={mergedFilters}
                selectedFilter={selectedFilter}
                onSelectFilter={(filter) => setManualSelectedFilter(filter)}
              />
            </div>
          </details>
        </>
      )}

      {activeTab === 'stress_tests' && (
        <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, display: 'flex', flexDirection: 'column', gap: 10 }}>
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading audit output...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && advancedSections.length > 0 && (
            <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10 }}>
              <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>
                Risk Audit Tests • {selectedFilter ?? 'Selected Filter'}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {advancedSections.map((section) => (
                  <details key={section.title} style={{ border: '1px solid var(--line)', borderRadius: 3, padding: '6px 8px', background: 'var(--bg1)' }}>
                    <summary style={{ cursor: 'pointer', fontSize: 10, color: 'var(--t1)', fontWeight: 600 }}>
                      {section.title}
                    </summary>
                    <pre
                      style={{
                        margin: '8px 0 0 0',
                        whiteSpace: 'pre-wrap',
                        fontSize: 10,
                        lineHeight: 1.45,
                        color: 'var(--t2)',
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                      }}
                    >
                      {section.body}
                    </pre>
                  </details>
                ))}
              </div>
            </div>
          )}
          {!outputLoading && !outputError && advancedSections.length === 0 && (
            <div style={{ fontSize: 10, color: 'var(--t3)' }}>No tear sheet sections detected for the selected filter.</div>
          )}
          {!outputLoading && !outputError && alertLines.length > 0 && (
            <div style={{ border: '1px solid rgba(255, 186, 77, 0.35)', borderRadius: 3, padding: 10, background: 'rgba(255, 186, 77, 0.06)' }}>
              <div style={{ fontSize: 9, color: 'var(--amber)', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>
                Alerts & Warnings
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4, fontSize: 10, color: 'var(--amber)' }}>
                {alertLines.map((line, idx) => <div key={`${idx}-${line}`}>{line}</div>)}
              </div>
            </div>
          )}
        </div>
      )}

      {activeTab === 'raw_output' && (
        <div
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--line)',
            borderRadius: 3,
            padding: 12,
            height: 'calc(100vh - 170px)',
            display: 'flex',
            flexDirection: 'column',
            overflowY: 'auto',
          }}
        >
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading full report...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && (
            <>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginBottom: 8,
                  position: 'sticky',
                  top: 0,
                  zIndex: 2,
                  background: 'var(--bg2)',
                  paddingBottom: 8,
                  borderBottom: '1px solid var(--line)',
                }}
              >
                <div
                  style={{
                    fontSize: 9,
                    color: 'var(--t3)',
                    textTransform: 'uppercase',
                    letterSpacing: '0.1em',
                    fontWeight: 700,
                  }}
                >
                  Raw Output
                </div>
                <button
                  onClick={copyRawOutput}
                  style={{
                    height: 26,
                    padding: '0 10px',
                    borderRadius: 3,
                    border: '1px solid var(--line2)',
                    background: 'var(--bg1)',
                    color: copyState === 'copied' ? 'var(--green)' : copyState === 'error' ? 'var(--red)' : 'var(--t1)',
                    fontSize: 9,
                    letterSpacing: '0.08em',
                    fontWeight: 700,
                    cursor: 'pointer',
                  }}
                >
                  {copyState === 'copied' ? 'COPIED' : copyState === 'error' ? 'COPY FAILED' : 'COPY ALL'}
                </button>
              </div>
              <pre
                style={{
                  margin: 0,
                  whiteSpace: 'pre-wrap',
                  fontSize: 10,
                  lineHeight: 1.45,
                  color: 'var(--t2)',
                  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                  flex: 1,
                  minHeight: 0,
                  overflowY: 'auto',
                }}
              >
                {auditOutput || 'No full report text available for this audit.'}
              </pre>
            </>
          )}
        </div>
      )}

      {activeTab === 'tear_sheet' && (
        <div
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--line)',
            borderRadius: 3,
            overflow: 'hidden',
            height: 'calc(100vh - 170px)',
          }}
        >
          <iframe
            title="Tear Sheet"
            srcDoc={tearSheetHtml || '<!doctype html><html><body style="background:#07090f;color:#7a8aaa;font-family:monospace;padding:16px">Loading tear sheet template...</body></html>'}
            style={{
              width: '100%',
              height: '100%',
              border: 'none',
              display: 'block',
              background: '#07090f',
            }}
          />
        </div>
      )}

      {activeTab === 'full_report' && (
        <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, display: 'flex', flexDirection: 'column', gap: 10 }}>
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading full report sections...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10 }}>
                <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
                  Full Report • {fullReportSections.length} sections
                </div>
                <button
                  onClick={copyRawOutput}
                  style={{
                    height: 24,
                    padding: '0 8px',
                    borderRadius: 3,
                    border: '1px solid var(--line2)',
                    background: 'var(--bg1)',
                    color: copyState === 'copied' ? 'var(--green)' : copyState === 'error' ? 'var(--red)' : 'var(--t1)',
                    fontSize: 9,
                    letterSpacing: '0.08em',
                    fontWeight: 700,
                    cursor: 'pointer',
                  }}
                >
                  {copyState === 'copied' ? 'COPIED' : copyState === 'error' ? 'COPY FAILED' : 'COPY ALL'}
                </button>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 'calc(100vh - 220px)', overflowY: 'auto', paddingRight: 2 }}>
                {fullReportSections.length === 0 && (
                  <div style={{ fontSize: 10, color: 'var(--t3)' }}>No full report sections detected.</div>
                )}
                {fullReportSections.map((section, idx) => (
                  <details key={`${idx}-${section.title}`} style={{ border: '1px solid var(--line)', borderRadius: 3, padding: '6px 8px', background: 'var(--bg1)' }}>
                    <summary style={{ cursor: 'pointer', fontSize: 10, color: 'var(--t1)', fontWeight: 600 }}>
                      {section.title}
                    </summary>
                    <pre
                      style={{
                        margin: '8px 0 0 0',
                        whiteSpace: 'pre-wrap',
                        fontSize: 10,
                        lineHeight: 1.45,
                        color: 'var(--t2)',
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                      }}
                    >
                      {section.body}
                    </pre>
                  </details>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
