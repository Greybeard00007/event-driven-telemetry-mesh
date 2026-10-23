// Real-Time Dashboard Controller for Event-Driven Telemetry Mesh

let ws = null;
let mechanicsChart = null;
let hydraulicsChart = null;
const maxPoints = 35;

// Data state
const chartData = {
    labels: [],
    wob: [],
    rpm: [],
    torque: [],
    spp: [],
    flowIn: [],
    flowOut: [],
};

let selectedRig = "RIG-TITAN";
let totalAnomaliesCount = 0;
let eventsInLastWindow = 0;
let lastRateCheckTime = Date.now();

// Initialize Dashboard
document.addEventListener("DOMContentLoaded", () => {
    initCharts();
    initWebSocket();
    setupEventHandlers();
    startMetricsPolling();
});

// Setup Chart.js Charts
function initCharts() {
    const ctxMechanics = document.getElementById("chart-mechanics").getContext("2d");
    const ctxHydraulics = document.getElementById("chart-hydraulics").getContext("2d");

    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 0 },
        scales: {
            x: {
                grid: { color: "#1e293b" },
                ticks: { color: "#64748b", maxRotation: 0, font: { size: 10 } }
            },
            y: {
                grid: { color: "#1e293b" },
                ticks: { color: "#94a3b8", font: { size: 10 } }
            },
            y1: {
                position: "right",
                grid: { drawOnChartArea: false },
                ticks: { color: "#94a3b8", font: { size: 10 } }
            }
        },
        plugins: {
            legend: {
                labels: { color: "#cbd5e1", boxWidth: 12, font: { size: 11 } }
            }
        }
    };

    mechanicsChart = new Chart(ctxMechanics, {
        type: "line",
        data: {
            labels: chartData.labels,
            datasets: [
                {
                    label: "WOB (klbf)",
                    data: chartData.wob,
                    borderColor: "#38bdf8",
                    backgroundColor: "rgba(56, 189, 248, 0.1)",
                    borderWidth: 2,
                    pointRadius: 0,
                    yAxisID: "y",
                },
                {
                    label: "RPM",
                    data: chartData.rpm,
                    borderColor: "#22c55e",
                    borderWidth: 2,
                    pointRadius: 0,
                    yAxisID: "y1",
                },
                {
                    label: "Torque (kft-lbf)",
                    data: chartData.torque,
                    borderColor: "#f59e0b",
                    borderWidth: 1.8,
                    borderDash: [4, 4],
                    pointRadius: 0,
                    yAxisID: "y",
                }
            ]
        },
        options: commonOptions,
    });

    hydraulicsChart = new Chart(ctxHydraulics, {
        type: "line",
        data: {
            labels: chartData.labels,
            datasets: [
                {
                    label: "SPP (psi)",
                    data: chartData.spp,
                    borderColor: "#a855f7",
                    borderWidth: 2,
                    pointRadius: 0,
                    yAxisID: "y",
                },
                {
                    label: "Flow In (gpm)",
                    data: chartData.flowIn,
                    borderColor: "#06b6d4",
                    borderWidth: 1.8,
                    pointRadius: 0,
                    yAxisID: "y1",
                },
                {
                    label: "Flow Out (gpm)",
                    data: chartData.flowOut,
                    borderColor: "#ec4899",
                    borderWidth: 1.8,
                    borderDash: [3, 3],
                    pointRadius: 0,
                    yAxisID: "y1",
                }
            ]
        },
        options: commonOptions,
    });
}

// WebSocket Connection
function initWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws/live`;

    const statusPill = document.getElementById("pill-ws");
    const statusText = document.getElementById("ws-status");

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        statusPill.classList.add("active");
        statusText.innerText = "Live";
    };

    ws.onclose = () => {
        statusPill.classList.remove("active");
        statusText.innerText = "Reconnecting...";
        setTimeout(initWebSocket, 2000);
    };

    ws.onerror = (err) => {
        console.error("WebSocket error:", err);
        ws.close();
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === "telemetry_batch") {
                handleTelemetryBatch(data.records);
            } else if (data.type === "anomaly_alert") {
                handleAnomalyAlert(data.anomaly);
            }
        } catch (e) {
            console.error("Message parse error:", e);
        }
    };
}

// Process Incoming Telemetry
function handleTelemetryBatch(records) {
    if (!records || records.length === 0) return;
    eventsInLastWindow += records.length;

    // Filter by selected rig if not 'ALL'
    let matchingRecords = records;
    if (selectedRig !== "ALL") {
        matchingRecords = records.filter(r => r.rig_id === selectedRig);
    }
    if (matchingRecords.length === 0) return;

    for (const r of matchingRecords) {
        const timeLabel = new Date(r.timestamp).toLocaleTimeString([], { hour12: false, minute: "2-digit", second: "2-digit" });
        
        chartData.labels.push(timeLabel);
        chartData.wob.push(r.wob);
        chartData.rpm.push(r.rpm);
        chartData.torque.push(r.torque);
        chartData.spp.push(r.spp);
        chartData.flowIn.push(r.flow_in);
        chartData.flowOut.push(r.flow_out);

        if (chartData.labels.length > maxPoints) {
            chartData.labels.shift();
            chartData.wob.shift();
            chartData.rpm.shift();
            chartData.torque.shift();
            chartData.spp.shift();
            chartData.flowIn.shift();
            chartData.flowOut.shift();
        }

        // Update mini gauges with latest reading
        updateMiniGauges(r);
    }

    mechanicsChart.update();
    hydraulicsChart.update();
}

function updateMiniGauges(r) {
    document.getElementById("gauge-depth").innerText = `${r.depth_md.toLocaleString()} ft`;
    document.getElementById("gauge-wob").innerText = `${r.wob.toFixed(1)} klbf`;
    document.getElementById("gauge-rpm").innerText = `${Math.round(r.rpm)} RPM`;
    document.getElementById("gauge-spp").innerText = `${Math.round(r.spp)} psi`;
    document.getElementById("gauge-flow").innerText = `${Math.round(r.flow_in)} / ${Math.round(r.flow_out)} gpm`;
}

// Process Anomaly & Ollama LLM Diagnosis
function handleAnomalyAlert(anomaly) {
    totalAnomaliesCount++;
    document.getElementById("kpi-anomalies").innerText = totalAnomaliesCount;

    const container = document.getElementById("ai-alerts-container");
    const emptyState = document.getElementById("empty-alerts");
    if (emptyState) {
        emptyState.remove();
    }

    const card = document.createElement("div");
    const severityClass = anomaly.severity ? anomaly.severity.toLowerCase() : "medium";
    card.className = `anomaly-card ${severityClass}`;

    const diag = anomaly.llm_diagnosis || {};
    const timeStr = new Date(anomaly.timestamp).toLocaleTimeString();
    const confidencePct = diag.confidence_score ? Math.round(diag.confidence_score * 100) : 90;

    card.innerHTML = `
        <div class="anomaly-header">
            <span class="anomaly-badge">[${anomaly.rig_id}] ${anomaly.anomaly_type}</span>
            <span class="anomaly-time">${timeStr} &bull; ${confidencePct}% CONFIDENCE</span>
        </div>
        <div class="anomaly-rule">${anomaly.rule_triggered}</div>
        <div class="diagnosis-box">
            <div class="diag-title">
                <span>AI Root-Cause Diagnosis (${diag.model_name || "Ollama"})</span>
                <span>URGENCY: ${anomaly.severity}</span>
            </div>
            <div class="diag-body">${diag.root_cause_analysis || "Threshold deviation evaluated."}</div>
            <div class="rec-title">Immediate Drilling Crew Action (SOP Grounded)</div>
            <div class="rec-body">${diag.drilling_recommendation || "Maintain active monitoring."}</div>
        </div>
        <div class="ack-area" id="ack-area-${anomaly.anomaly_id}" style="margin-top:6px; display:flex; justify-content:flex-end;">
            <button class="btn-primary" style="padding:4px 10px; font-size:0.75rem; background:#334155;" onclick="acknowledgeAnomaly('${anomaly.anomaly_id}', '${anomaly.rig_id}')">
                &#9998; Acknowledge & Log Mitigation
            </button>
        </div>
    `;

    container.insertBefore(card, container.firstChild);

    // Keep max 25 cards in DOM
    while (container.children.length > 25) {
        container.removeChild(container.lastChild);
    }
}

// Operator Anomaly Signoff
window.acknowledgeAnomaly = async function(anomalyId, rigId) {
    const drillerName = prompt("Enter Driller / Toolpusher Name:", "Driller On Duty");
    if (!drillerName) return;
    const actionTaken = prompt("Action Taken & Mitigation Performed:", "Confirmed parameter status; followed well control procedure.");
    if (!actionTaken) return;

    try {
        const resp = await fetch(`/api/v1/anomalies/${anomalyId}/acknowledge`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                driller_name: drillerName,
                action_taken: actionTaken,
                comments: "Logged via mission-control dashboard",
            }),
        });
        if (resp.ok) {
            const data = await resp.json();
            const ackArea = document.getElementById(`ack-area-${anomalyId}`);
            if (ackArea) {
                ackArea.innerHTML = `<span style="color:#22c55e; font-size:0.75rem; font-family:var(--font-mono);">&#10003; ACKNOWLEDGED by ${drillerName} [Sig: ${data.audit_entry.audit_id}]</span>`;
            }
        }
    } catch (e) {
        alert("Error logging acknowledgment: " + e.message);
    }
};

// Setup Event Handlers
function setupEventHandlers() {
    const rigSelect = document.getElementById("rig-selector");
    rigSelect.addEventListener("change", (e) => {
        selectedRig = e.target.value;
        // Reset chart arrays for crisp display
        chartData.labels.length = 0;
        chartData.wob.length = 0;
        chartData.rpm.length = 0;
        chartData.torque.length = 0;
        chartData.spp.length = 0;
        chartData.flowIn.length = 0;
        chartData.flowOut.length = 0;
        mechanicsChart.update();
        hydraulicsChart.update();
    });

    const btnInject = document.getElementById("btn-inject");
    btnInject.addEventListener("click", async () => {
        const rigId = document.getElementById("inject-rig").value;
        const anomType = document.getElementById("inject-type").value;
        const duration = parseFloat(document.getElementById("inject-duration").value);
        const msgEl = document.getElementById("inject-status-msg");

        msgEl.className = "inject-msg";
        msgEl.innerText = `Injecting ${anomType} into ${rigId}...`;

        try {
            const resp = await fetch("/api/v1/simulator/inject", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    rig_id: rigId,
                    anomaly_type: anomType,
                    duration_seconds: duration,
                }),
            });
            const data = await resp.json();
            if (resp.ok) {
                msgEl.className = "inject-msg success";
                msgEl.innerText = `Successfully injected ${anomType} for ${duration}s. Watch real-time charts & AI feed!`;
            } else {
                msgEl.className = "inject-msg error";
                msgEl.innerText = `Failed: ${data.detail || "Error"}`;
            }
        } catch (err) {
            msgEl.className = "inject-msg error";
            msgEl.innerText = `Network error: ${err.message}`;
        }
    });
}

// Metrics Poller
function startMetricsPolling() {
    setInterval(async () => {
        const now = Date.now();
        const deltaSec = (now - lastRateCheckTime) / 1000;
        const rate = Math.round(eventsInLastWindow / Math.max(deltaSec, 0.1));
        document.getElementById("kpi-rate").innerHTML = `${rate} <small>ev/s</small>`;
        eventsInLastWindow = 0;
        lastRateCheckTime = now;

        try {
            const resp = await fetch("/api/v1/metrics");
            if (resp.ok) {
                const m = await resp.json();
                document.getElementById("kpi-total-published").innerText = `${m.published_total.toLocaleString()} Total Events`;
                if (m.stream_stats && typeof m.stream_stats.pending !== "undefined") {
                    document.getElementById("kpi-queue").innerText = m.stream_stats.pending;
                }
            }

            const dlqResp = await fetch("/api/v1/dlq");
            if (dlqResp.ok) {
                const dlqData = await dlqResp.json();
                const dlqCount = dlqData.stats ? (dlqData.stats.length || 0) : 0;
                const dlqEl = document.getElementById("dlq-count");
                if (dlqEl) dlqEl.innerText = dlqCount;
            }

            const hResp = await fetch("/api/v1/health");
            if (hResp.ok) {
                const h = await hResp.json();
                const brokerPill = document.getElementById("pill-broker");
                const timescalePill = document.getElementById("pill-timescale");
                const ollamaPill = document.getElementById("pill-ollama");

                if (h.services.redis_broker === "connected") brokerPill.classList.add("active");
                if (h.services.timescaledb === "connected") timescalePill.classList.add("active");
                if (h.services.ollama_inference === "ready") ollamaPill.classList.add("active");
            }
        } catch (e) {
            // Quiet poll failure
        }
    }, 2000);
}
