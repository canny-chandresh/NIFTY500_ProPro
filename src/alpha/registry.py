# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List

@dataclass
class AlphaDef:
    name: str
    module: str      # python module under alpha.factors
    enabled: bool
    fast: bool       # allowed in hourly
    shadow: bool     # computed but not used in training/scoring if True

ALPHAS: List[AlphaDef] = [
    AlphaDef("alpha_gap_decay",   "alpha_gap_decay",   enabled=True,  fast=True,  shadow=True),
    AlphaDef("alpha_pair_flow",   "alpha_pair_flow",   enabled=True,  fast=False, shadow=True),
    AlphaDef("alpha_event_guard", "alpha_event_guard", enabled=True,  fast=False, shadow=True),
]
