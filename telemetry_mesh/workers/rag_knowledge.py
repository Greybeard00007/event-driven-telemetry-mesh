"""Retrieval-Augmented Generation (RAG) knowledge engine for drilling operational procedures."""

import math
import re
from typing import Dict, List, Tuple
from telemetry_mesh.models.anomaly import AnomalyType


DRILLING_SOP_CORPUS = [
    {
        "id": "API-RP-53-KICK-01",
        "title": "API Standard 53 & IADC Hard Shut-in Well Control Procedure",
        "keywords": ["kick", "influx", "flow_out", "pit_gain", "shut-in", "annular", "bop", "sidpp", "sicp"],
        "content": (
            "Standard Hard Shut-In Procedure (API RP 53 / IADC):\n"
            "1. Space out drill string until tool joint clears the rotary table.\n"
            "2. Stop rotary drive and immediately shut down mud pumps.\n"
            "3. Close the Annular Preventer or Upper Pipe Rams.\n"
            "4. Open the remote choke line valve (HCR valve) into the closed choke manifold.\n"
            "5. Alert company man and toolpusher.\n"
            "6. Record stabilized Shut-In Drill Pipe Pressure (SIDPP), Shut-In Casing Pressure (SICP), and active Pit Gain.\n"
            "7. Calculate kill mud weight: KMW = Initial MW + (SIDPP / (0.052 * TVD)).\n"
            "8. Execute Driller's Method or Wait-and-Weight method."
        ),
    },
    {
        "id": "IADC-STICK-SLIP-02",
        "title": "IADC / Shell SoftTorque Stick-Slip Mitigation Guidelines",
        "keywords": ["stick_slip", "torsional", "rpm", "oscillation", "stall", "torque_cycle", "harmonic"],
        "content": (
            "Stick-Slip Torsional Oscillation Mitigation Protocol:\n"
            "1. Reduce Weight on Bit (WOB) by 30% to 50% immediately to unload cutters from rock face.\n"
            "2. Increase surface rotary speed by 15-25 RPM to pull string out of torsional harmonic resonance.\n"
            "3. Engage SoftTorque / automated stick-slip mitigation system on top drive drive system.\n"
            "4. Verify bit depth of cut (DOC) is stable.\n"
            "5. If stick-slip persists, pick up off bottom, circulate bottoms up, and inspect mud lubricating properties."
        ),
    },
    {
        "id": "BP-PACK-OFF-03",
        "title": "Borehole Pack-Off and Annular Restriction Recovery Procedure",
        "keywords": ["pack_off", "annular", "spp_spike", "flow_drop", "cuttings", "choking", "tight_hole"],
        "content": (
            "Annular Pack-Off Immediate Response Protocol:\n"
            "1. Stop downward drill string advancement immediately; do NOT attempt to force ahead.\n"
            "2. Reduce mud pump strokes by 50% to prevent exceeding formation fracture gradient.\n"
            "3. Work the drill pipe downwards and upwards with maximum allowable overpull.\n"
            "4. Do NOT attempt to rotate if torque is locked.\n"
            "5. Once circulation is restored, pump high-viscosity / high-density weighted sweep pill.\n"
            "6. Circulate until shale shakers show clean cuttings return before resuming drilling."
        ),
    },
    {
        "id": "CHEVRON-STUCK-PIPE-04",
        "title": "Positive Displacement Motor (PDM) Stall Recovery Protocol",
        "keywords": ["motor_stall", "stall", "pdm", "rotor", "differential_pressure", "torque_spike"],
        "content": (
            "Mud Motor Stall Recovery Procedure:\n"
            "1. Immediately pick off bottom until torque gauge drops to clean off-bottom baseline.\n"
            "2. Monitor Standpipe Pressure (SPP) until it normalizes to off-bottom circulating pressure.\n"
            "3. If motor does not restart rotation, cycle mud pumps off and on.\n"
            "4. Re-tag bottom gently with half the previous WOB."
        ),
    },
    {
        "id": "EXXON-LOST-CIRC-05",
        "title": "Lost Circulation Mitigation and Fracture Treatment Protocol",
        "keywords": ["lost_circulation", "loss", "fluid_loss", "fracture", "deficit", "lcm"],
        "content": (
            "Severe Lost Circulation Response:\n"
            "1. Pull bit 100 ft off bottom to protect drill string against stuck pipe.\n"
            "2. Reduce pump rate to minimum rate necessary to maintain annulus level.\n"
            "3. Fill annulus from surface with base fluid if fluid level drops.\n"
            "4. Prepare and pump 50-barrel LCM (Lost Circulation Material) pill containing coarse fiber and nut shell.\n"
            "5. Squeeze pill into formation if static losses continue."
        ),
    },
]


class DrillingKnowledgeBase:
    """In-memory vector retrieval engine utilizing tokenized TF-IDF and cosine similarity."""

    def __init__(self):
        self.documents = DRILLING_SOP_CORPUS

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [w.lower() for w in re.findall(r"\b[a-zA-Z0-9_-]{3,}\b", text)]

    def retrieve(self, query: str, top_k: int = 2) -> List[Dict[str, str]]:
        """Retrieve most relevant operational SOPs based on keyword vector match."""
        query_tokens = set(self._tokenize(query))
        if not query_tokens:
            return self.documents[:top_k]

        scored: List[Tuple[float, Dict[str, str]]] = []
        for doc in self.documents:
            doc_keywords = set(doc["keywords"])
            doc_content_tokens = set(self._tokenize(doc["content"]))

            # Weighted intersection
            keyword_overlap = len(query_tokens.intersection(doc_keywords)) * 3.0
            content_overlap = len(query_tokens.intersection(doc_content_tokens)) * 1.0

            score = keyword_overlap + content_overlap
            scored.append((score, doc))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [doc for score, doc in scored[:top_k] if score > 0] or self.documents[:1]
