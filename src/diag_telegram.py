# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
sys.path.append("src")
import telegram as tg

def ping(text="Hello from GitHub"):
    tg._send(text, html=False)
