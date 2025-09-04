# src/entrypoints.py
"""
Entry points for scheduled tasks (called by GitHub Actions).
"""

from __future__ import annotations
import os
from pipeline import run_paper_session
from report_eod import build_eod
from report_periodic import build_periodic
from kill_switch import evaluate_and_update
from config import CONFIG


def daily_update():
    """
    Placeholder for any daily refresh tasks (e.g., ingesting new candles).
    Currently a no-op.
    """
    print("daily_update(): OK (placeholder)")
    return True


def eod_task():
    """
    End-of-day task: build the EOD report.
    """
    print("eod_task(): building EOD report...")
    build_eod()
    return True


def periodic_reports_task():
    """
    Task to build aggregated periodic reports (daily/weekly/monthly).
    """
    print("periodic_reports_task(): building periodic reports...")
    build_periodic()
    return True


def after_run_housekeeping():
    """
    Housekeeping tasks after a run:
    - evaluate kill-switch
    - persist state if needed
    """
    print("after_run_housekeeping(): evaluating kill-switch...")
    evaluate_and_update()
    return True
