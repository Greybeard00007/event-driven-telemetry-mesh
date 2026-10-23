"""Digital Twin Forward Physics Engine for Drilling Mechanics and Hydraulics.

Calculates:
1. Teale's Mechanical Specific Energy (MSE, kpsi)
2. Equivalent Circulating Density (ECD, ppg)
3. Corrected d-Exponent (d_xc) for pre-kick pore pressure overpressure detection
"""

import math
from typing import Dict, Optional
from telemetry_mesh.models.telemetry import RigTelemetry


class DrillingPhysicsEngine:
    """Computes advanced downhole drilling mechanics and hydraulics indicators."""

    def __init__(
        self,
        bit_diameter_in: float = 8.5,       # 8-1/2" standard drill bit
        nominal_mud_weight_ppg: float = 10.5, # 10.5 ppg mud
        normal_pore_pressure_grad_ppg: float = 9.0, # Normal formation pore pressure
    ):
        self.bit_diameter = bit_diameter_in
        self.bit_area = (math.pi / 4.0) * (self.bit_diameter**2)  # square inches
        self.mud_weight = nominal_mud_weight_ppg
        self.normal_pp_grad = normal_pore_pressure_grad_ppg

    def compute_mse(self, wob_klbf: float, rpm: float, torque_kft_lbf: float, rop_ft_hr: float) -> float:
        """Calculate Teale's Mechanical Specific Energy (MSE) in kpsi.

        MSE = (WOB / A_b) + (13.33 * RPM * Torque) / (A_b * ROP)
        WOB in klbf (1000 lbf), Torque in kft-lbf, ROP in ft/hr, Bit Area in sq in.
        """
        if rop_ft_hr <= 0.5:
            # When string is off bottom or stationary, rotary energy is not cutting rock
            return round((wob_klbf * 1000.0) / max(self.bit_area, 1.0) / 1000.0, 2)

        # Convert WOB to lbf, Torque to ft-lbf
        wob_lbf = wob_klbf * 1000.0
        torque_ft_lbf = torque_kft_lbf * 1000.0

        # Axial component (psi)
        axial_psi = wob_lbf / self.bit_area
        # Rotary / torsional component (psi)
        rotary_psi = (13.33 * rpm * torque_ft_lbf) / (self.bit_area * rop_ft_hr)

        total_mse_kpsi = (axial_psi + rotary_psi) / 1000.0
        return round(total_mse_kpsi, 2)

    def compute_ecd(self, flow_in_gpm: float, spp_psi: float, tvd_ft: float) -> float:
        """Calculate Equivalent Circulating Density (ECD) in pounds per gallon (ppg).

        ECD = MW + (Annular Pressure Drop / (0.052 * TVD))
        Annular pressure loss is typically 8% to 15% of total surface standpipe pressure.
        """
        if tvd_ft <= 100.0:
            return round(self.mud_weight, 2)

        # Approximate annular friction pressure loss (~12% of SPP)
        annular_pressure_loss = spp_psi * 0.12
        dynamic_increment = annular_pressure_loss / (0.052 * tvd_ft)
        ecd = self.mud_weight + dynamic_increment
        return round(ecd, 2)

    def compute_d_exponent(
        self,
        rop_ft_hr: float,
        rpm: float,
        wob_klbf: float,
    ) -> Optional[float]:
        """Calculate the corrected d-exponent (d_xc) for pore pressure trend analysis.

        d = log10( ROP / (60 * RPM) ) / log10( 12 * WOB / (10^6 * Bit_Dia) )
        d_xc = d * (Normal_Pore_Pressure_Gradient / Mud_Weight)

        A sudden downward deflection in d_xc indicates transition into an overpressured
        under-compacted shale formation — warning of an imminent kick before pit gain!
        """
        if rop_ft_hr <= 1.0 or rpm <= 5.0 or wob_klbf <= 1.0:
            return None

        wob_lbf = wob_klbf * 1000.0
        numerator_term = rop_ft_hr / (60.0 * rpm)
        denominator_term = (12.0 * wob_lbf) / (1_000_000.0 * self.bit_diameter)

        if numerator_term <= 0 or denominator_term <= 0 or denominator_term == 1.0:
            return None

        try:
            d = math.log10(numerator_term) / math.log10(denominator_term)
            # Correct for mud weight
            d_xc = d * (self.normal_pp_grad / max(self.mud_weight, 8.33))
            return round(d_xc, 3)
        except (ValueError, ZeroDivisionError):
            return None

    def evaluate_telemetry(self, telemetry: RigTelemetry) -> Dict[str, float]:
        """Compute all digital twin physics indicators for a given sensor frame."""
        tvd = telemetry.depth_tvd or (telemetry.depth_md * 0.92)

        mse = self.compute_mse(
            wob_klbf=telemetry.wob,
            rpm=telemetry.rpm,
            torque_kft_lbf=telemetry.torque,
            rop_ft_hr=telemetry.rop,
        )

        ecd = self.compute_ecd(
            flow_in_gpm=telemetry.flow_in,
            spp_psi=telemetry.spp,
            tvd_ft=tvd,
        )

        d_xc = self.compute_d_exponent(
            rop_ft_hr=telemetry.rop,
            rpm=telemetry.rpm,
            wob_klbf=telemetry.wob,
        )

        # Baseline rock unconfined compressive strength comparison (~25-45 kpsi)
        # Drilling efficiency ratio: rock_strength / MSE
        efficiency_pct = min(100.0, max(5.0, (30.0 / max(mse, 1.0)) * 100.0))

        return {
            "mse_kpsi": mse,
            "ecd_ppg": ecd,
            "d_xc": d_xc if d_xc is not None else 1.25,
            "drilling_efficiency_pct": round(efficiency_pct, 1),
            "bit_diameter_in": self.bit_diameter,
            "mud_weight_ppg": self.mud_weight,
        }
