#!/usr/bin/env python3
"""Terminal TUI Mission Control for Event-Driven Telemetry Mesh.

Renders an ANSI real-time console dashboard for headless and SSH environments.
"""

import argparse
import asyncio
from datetime import datetime
import os
import sys
import time
from typing import Any, Dict, List
import httpx


# ANSI Color Codes
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
BLUE = "\033[34m"
BG_DARK = "\033[40m"
CLEAR_SCREEN = "\033[2J\033[H"


def format_header(health_data: Dict[str, Any]) -> str:
    services = health_data.get("services", {})
    r_ok = services.get("redis_broker") == "connected"
    t_ok = services.get("timescaledb") == "connected"
    o_ok = services.get("ollama_inference") == "ready"

    r_badge = f"{GREEN}● REDIS{RESET}" if r_ok else f"{RED}○ REDIS{RESET}"
    t_badge = f"{GREEN}● TIMESCALE{RESET}" if t_ok else f"{YELLOW}◐ IN-MEMORY{RESET}"
    o_badge = f"{GREEN}● OLLAMA{RESET}" if o_ok else f"{YELLOW}○ OLLAMA (HEURISTIC){RESET}"
    ws_count = health_data.get("active_websockets", 0)

    lines = [
        f"{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════════════════════════════{RESET}",
        f"{BOLD} EVENT-DRIVEN TELEMETRY MESH // TERMINAL MISSION CONTROL{RESET}  |  {r_badge}  {t_badge}  {o_badge}  | WS: {CYAN}{ws_count}{RESET}",
        f"{BOLD}{CYAN}══════════════════════════════════════════════════════════════════════════════════════════════════{RESET}",
    ]
    return "\n".join(lines)


def format_rig_table(records: List[Dict[str, Any]]) -> str:
    # Deduplicate latest by rig
    latest_by_rig: Dict[str, Dict[str, Any]] = {}
    for r in records:
        latest_by_rig[r["rig_id"]] = r

    header = (
        f"{BOLD}{'RIG ID':<14} {'WELL ID':<12} {'DEPTH (ft)':<12} {'WOB (klbf)':<12} "
        f"{'RPM':<8} {'TORQ (kft)':<12} {'SPP (psi)':<12} {'FLOW (gpm)':<14} {'ROP (ft/h)':<10}{RESET}"
    )
    separator = f"{DIM}{'-' * 98}{RESET}"

    rows = [header, separator]

    if not latest_by_rig:
        rows.append(f"{DIM}  Waiting for rig telemetry packets...{RESET}")
    else:
        for rig_id, r in latest_by_rig.items():
            flow_str = f"{r.get('flow_in', 0):.0f} / {r.get('flow_out', 0):.0f}"
            row = (
                f"{GREEN}{r.get('rig_id', ''):<14}{RESET} "
                f"{r.get('well_id', ''):<12} "
                f"{CYAN}{r.get('depth_md', 0):>9.1f} ft{RESET} "
                f"{r.get('wob', 0):>10.1f}   "
                f"{r.get('rpm', 0):>6.0f}  "
                f"{r.get('torque', 0):>10.1f}   "
                f"{MAGENTA}{r.get('spp', 0):>9.0f} psi{RESET} "
                f"{flow_str:>13}  "
                f"{r.get('rop', 0):>8.1f}"
            )
            rows.append(row)

    return "\n".join(rows)


def format_metrics(metrics_data: Dict[str, Any], dlq_data: Dict[str, Any]) -> str:
    published = metrics_data.get("published_total", 0)
    processed = metrics_data.get("processed_total", 0)
    anomalies = metrics_data.get("anomalies_total", 0)
    reclaimed = metrics_data.get("reclaimed_total", 0)
    stream_stats = metrics_data.get("stream_stats", {})
    queue_pending = stream_stats.get("pending", 0)
    dlq_count = dlq_data.get("stats", {}).get("length", 0)

    line = (
        f"{BOLD}PIPELINE METRICS:{RESET} "
        f"Published: {GREEN}{published:,}{RESET} | "
        f"Processed: {CYAN}{processed:,}{RESET} | "
        f"Queue Lag: {YELLOW if queue_pending > 0 else GREEN}{queue_pending}{RESET} | "
        f"DLQ Count: {RED if dlq_count > 0 else GREEN}{dlq_count}{RESET} | "
        f"Reclaimed: {reclaimed} | "
        f"Anomalies: {RED if anomalies > 0 else GREEN}{anomalies}{RESET}"
    )
    return line


def format_ai_panel(anomalies: List[Dict[str, Any]]) -> str:
    lines = [
        f"{BOLD}{MAGENTA}── OLLAMA AI REAL-TIME REASONING FEED ─────────────────────────────────────────────────────────────{RESET}"
    ]

    if not anomalies:
        lines.append(f"{DIM}  No active drilling anomalies detected. Continuous physics & ML monitoring active.{RESET}")
    else:
        # Show last 2 anomalies
        for a in reversed(anomalies[-2:]):
            sev = a.get("severity", "MEDIUM")
            sev_color = RED if sev == "CRITICAL" else (YELLOW if sev == "HIGH" else CYAN)
            diag = a.get("llm_diagnosis") or {}
            root_cause = diag.get("root_cause_analysis", "Threshold deviation detected.")
            rec = diag.get("drilling_recommendation", "Monitor operational limits.")
            time_str = a.get("timestamp", "")[:19].replace("T", " ")

            lines.append(
                f" {sev_color}[{sev}]{RESET} {BOLD}{a.get('rig_id')} - {a.get('anomaly_type')}{RESET} "
                f"({a.get('rule_triggered')}) at {DIM}{time_str}{RESET}"
            )
            lines.append(f"   {CYAN}AI Root Cause:{RESET} {root_cause[:180]}...")
            lines.append(f"   {GREEN}Action Plan:{RESET}   {rec.replace(chr(10), ' | ')[:160]}...")
            lines.append("")

    return "\n".join(lines)


async def run_dashboard(url: str, interval: float, once: bool) -> None:
    async with httpx.AsyncClient(base_url=url, timeout=3.0) as client:
        while True:
            try:
                # Fetch state in parallel
                h_task = client.get("/api/v1/health")
                t_task = client.get("/api/v1/telemetry/recent?limit=25")
                m_task = client.get("/api/v1/metrics")
                a_task = client.get("/api/v1/anomalies?limit=5")
                d_task = client.get("/api/v1/dlq")

                resps = await asyncio.gather(h_task, t_task, m_task, a_task, d_task, return_exceptions=True)

                h_data = resps[0].json() if not isinstance(resps[0], Exception) and resps[0].status_code == 200 else {}
                t_data = resps[1].json() if not isinstance(resps[1], Exception) and resps[1].status_code == 200 else []
                m_data = resps[2].json() if not isinstance(resps[2], Exception) and resps[2].status_code == 200 else {}
                a_data = resps[3].json() if not isinstance(resps[3], Exception) and resps[3].status_code == 200 else []
                d_data = resps[4].json() if not isinstance(resps[4], Exception) and resps[4].status_code == 200 else {}

                output = [
                    CLEAR_SCREEN,
                    format_header(h_data),
                    "",
                    format_rig_table(t_data),
                    "",
                    format_metrics(m_data, d_data),
                    "",
                    format_ai_panel(a_data),
                    f"{DIM}Press Ctrl+C to exit. Refreshing every {interval}s...{RESET}",
                ]

                sys.stdout.write("\n".join(output) + "\n")
                sys.stdout.flush()

            except Exception as e:
                sys.stdout.write(f"\r{RED}Connection error to {url}: {e}{RESET}\n")

            if once:
                break

            await asyncio.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Terminal TUI Mission Control for Telemetry Mesh")
    parser.add_argument("--url", default="http://localhost:8000", help="Telemetry Mesh API Base URL")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds")
    parser.add_argument("--once", action="store_true", help="Print dashboard state once and exit")
    args = parser.parse_args()

    try:
        asyncio.run(run_dashboard(args.url, args.interval, args.once))
    except KeyboardInterrupt:
        sys.stdout.write(f"\n{YELLOW}Terminal TUI exited.{RESET}\n")


if __name__ == "__main__":
    main()
