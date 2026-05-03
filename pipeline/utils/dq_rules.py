"""
DQ rules loader — single source of truth for all DQ configuration.

Wraps dq_rules.yaml so both transform.py (detection) and run_all.py (reporting)
read the same config object rather than duplicating YAML lookups.

Design:
- Loaded once on the driver; never serialised to executors.
- Returns safe defaults when the file is missing (dev/test without /data/config).
- DQRules.from_config(config) is the normal entry point.
"""

from __future__ import annotations

import yaml


class DQRules:
    def __init__(self, path: str = None):
        self._r: dict = {}
        if path:
            try:
                with open(path) as f:
                    self._r = yaml.safe_load(f) or {}
            except FileNotFoundError:
                pass

    @classmethod
    def from_config(cls, config: dict) -> DQRules:
        path = config.get("dq", {}).get("rules_path", "/data/config/dq_rules.yaml")
        return cls(path)

    # ── DQ detection helpers ─────────────────────────────────────────────────

    def currency_variants(self) -> list:
        """Canonical ZAR + all variant spellings, uppercased. Used in Spark isin() checks."""
        cfg = self._r.get("currency_normalisation", {})
        canonical = cfg.get("target_value", "ZAR").upper()
        variants = [v.upper() for v in cfg.get("variants", [])]
        # canonical first so isin() includes it for the _is_currency_variant exclusion
        seen, result = set(), []
        for v in [canonical] + variants:
            if v not in seen:
                seen.add(v)
                result.append(v)
        return result

    def null_required_fields(self, table: str) -> list:
        """Required (non-nullable) field names for a table. Empty list = no null checks."""
        return self._r.get("null_checks", {}).get(table, {}).get("fields", [])

    def date_accepted_formats(self) -> list:
        """Human-readable accepted date variant labels from config (informational)."""
        return self._r.get("date_format_checks", {}).get("accepted_variants", [])

    # ── Report generation ────────────────────────────────────────────────────

    def report_issues(self) -> list:
        """
        Ordered list of issue definitions for dq_report.json generation.

        Each entry has:
          issue_type            — string key written to the report
          handling_action       — string value written to the report
          count_keys            — list of "table.key" dot-paths into dq_summary
          records_in_output_count — bool: True → records_in_output == records_affected
        """
        return self._r.get("report_issues", [])

    def get_handling_action(self, section_path: list, default: str = "") -> str:
        """Traverse nested yaml by key list and return handling_action (legacy helper)."""
        node = self._r
        for k in section_path:
            node = node.get(k, {}) if isinstance(node, dict) else {}
        return node.get("handling_action", default) if isinstance(node, dict) else default
