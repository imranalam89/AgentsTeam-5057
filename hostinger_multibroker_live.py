"""
Hostinger VPS 24/7 Multi-Broker 3-Asset Live Daemon — Port 5057
=================================================================
Locked 3-Asset Master Strategy Brain (100% Delta 1m + 1s L2 Parity) with Multi-Broker Fan-Out:
  - DELTA Exchange India (Direct API)
  - PICKMYTRADE (Tradovate Webhook Bridge: MGCZ6 / MBTZ6 / METZ6)
  - WEBHOOK (Universal JSON / TradersPost / PineConnector / Custom Template)
  - MT5_API (HTTP REST API + Optional SyndicateBridgeEA.mq5 Poll Queue)
  - TRADOVATE_API (Direct Tradovate REST API)

Per-Account & Per-Pair Sizing:
  - FIXED_LOT (e.g. 0.01 MT5 lot, 1 MGCZ6 contract, custom per pair)
  - RISK_USD (e.g. $5.00 risk per trade auto-scaled to broker point value)

Architecture Guarantees:
  - Isolated Port 5057 (Ports 5050, 5055, and 5056 remain 100% untouched)
  - Distance-Anchored SL / TP / Trail (no cross-broker price basis mismatch)
  - Pure Python 3 Stdlib (0 numpy/pandas dependencies on VPS)
"""

import os
import sys
import time
import math
import heapq
import json
import hmac
import hashlib
import secrets
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from http.cookies import SimpleCookie
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, List, Optional, Tuple
import urllib.request
import urllib.error

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

ENV_PATH = os.path.join(BASE_DIR, ".env")
MULTIBROKER_JOURNAL_PATH = os.path.join(BASE_DIR, "live_journal_multibroker.json")
MULTIBROKER_CONFIG_PATH = os.path.join(BASE_DIR, "multibroker_runtime_config.json")
FROZEN_MODELS_PATH = os.path.join(BASE_DIR, "unified_frozen_models.json")
L2_1M_HISTORY_PATH = os.path.join(BASE_DIR, "l2_1m_history.jsonl")
JOURNAL_VERSION = 5
DELTA_GST_MULT = 1.18

PAIRS = ("XAUTUSD", "BTCUSD", "ETHUSD")
BROKER_TYPES = ("DELTA", "PICKMYTRADE", "WEBHOOK", "MT5_API", "TRADOVATE_API")
SIZING_MODES = ("FIXED_LOT", "RISK_USD")
DELTA_ENTRY_MODES = ("MAKER_CHASER", "MARKET")
IST_OFFSET = timedelta(hours=5, minutes=30)

SPECIALIST_COOLDOWN_SECS: Dict[str, Dict[str, int]] = {
    "XAUTUSD": {"1m": 2700, "5m": 1800, "15m": 2700, "1h": 3600},
    "BTCUSD":  {"1m": 5400, "5m": 3600, "15m": 900,  "1h": 3600},
    "ETHUSD":  {"1m": 1800, "5m": 900,  "15m": 1800, "1h": 3600},
}

PAIR_SPECS: Dict[str, Dict[str, Any]] = {
    "XAUTUSD": {
        "symbol": "XAUTUSD",
        "short": "GOLD",
        "product_id": 131253,
        "lot_size": 0.001,
        "maker_fee": 0.0001,
        "taker_fee": 0.0002,
        "default_spread": 0.15,
        "max_spread": 0.65,
        "sl_min": 3.00,
        "sl_max": 9.35,
        "post_loss_cd_secs": 1800,
    },
    "BTCUSD": {
        "symbol": "BTCUSD",
        "short": "BTC",
        "product_id": 27,
        "lot_size": 0.001,
        "maker_fee": 0.0002,
        "taker_fee": 0.0005,
        "default_spread": 4.50,
        "max_spread": 20.00,
        "sl_min": 130.0,
        "sl_max": 650.0,
        "post_loss_cd_secs": 900,
    },
    "ETHUSD": {
        "symbol": "ETHUSD",
        "short": "ETH",
        "product_id": 3136,
        "lot_size": 0.01,
        "maker_fee": 0.0002,
        "taker_fee": 0.0005,
        "default_spread": 0.35,
        "max_spread": 0.75,
        "sl_min": 11.0,
        "sl_max": 34.0,
        "post_loss_cd_secs": 0,
    },
}

DEFAULT_PAIR_CONFIGS_BY_BROKER: Dict[str, Dict[str, Dict[str, Any]]] = {
    "DELTA": {
        "XAUTUSD": {"enabled": True, "symbol": "XAUTUSD", "lots": 100.0, "point_value": 0.001, "tick_size": 0.01},
        "BTCUSD":  {"enabled": True, "symbol": "BTCUSD",  "lots": 10.0,  "point_value": 0.001, "tick_size": 0.50},
        "ETHUSD":  {"enabled": True, "symbol": "ETHUSD",  "lots": 10.0,  "point_value": 0.01,  "tick_size": 0.05},
    },
    "PICKMYTRADE": {
        "XAUTUSD": {"enabled": True, "symbol": "MGC1!", "lots": 1.0, "point_value": 10.0, "tick_size": 0.10},
        "BTCUSD":  {"enabled": True, "symbol": "MBT1!", "lots": 1.0, "point_value": 0.10, "tick_size": 5.00},
        "ETHUSD":  {"enabled": True, "symbol": "MET1!", "lots": 1.0, "point_value": 0.10, "tick_size": 0.50},
    },
    "TRADOVATE_API": {
        "XAUTUSD": {"enabled": True, "symbol": "MGCZ6", "lots": 1.0, "point_value": 10.0, "tick_size": 0.10},
        "BTCUSD":  {"enabled": True, "symbol": "MBTZ6", "lots": 1.0, "point_value": 0.10, "tick_size": 5.00},
        "ETHUSD":  {"enabled": True, "symbol": "METZ6", "lots": 1.0, "point_value": 0.10, "tick_size": 0.50},
    },
    "MT5_API": {
        "XAUTUSD": {"enabled": True, "symbol": "GOLD",   "lots": 0.01, "point_value": 100.0, "tick_size": 0.01},
        "BTCUSD":  {"enabled": True, "symbol": "BTCUSD", "lots": 0.01, "point_value": 1.0,   "tick_size": 0.01},
        "ETHUSD":  {"enabled": True, "symbol": "ETHUSD", "lots": 0.01, "point_value": 1.0,   "tick_size": 0.01},
    },
    "WEBHOOK": {
        "XAUTUSD": {"enabled": True, "symbol": "MGCZ6",  "lots": 1.0, "point_value": 10.0, "tick_size": 0.10},
        "BTCUSD":  {"enabled": True, "symbol": "BTCUSD", "lots": 1.0, "point_value": 0.10, "tick_size": 0.50},
        "ETHUSD":  {"enabled": True, "symbol": "ETHUSD", "lots": 1.0, "point_value": 0.10, "tick_size": 0.05},
    },
}


def normalize_pair_configs(
    raw_pc: Any,
    broker_type: str,
    enabled_pairs_fallback: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    btype = str(broker_type or "DELTA").upper()
    if btype not in DEFAULT_PAIR_CONFIGS_BY_BROKER:
        btype = "DELTA"
    defaults = DEFAULT_PAIR_CONFIGS_BY_BROKER[btype]
    raw_dict = raw_pc if isinstance(raw_pc, dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for sym in PAIRS:
        base = dict(defaults[sym])
        item = raw_dict.get(sym) if isinstance(raw_dict.get(sym), dict) else {}
        if "enabled" in item:
            en = bool(item["enabled"])
        elif isinstance(enabled_pairs_fallback, list):
            en = sym in enabled_pairs_fallback
        else:
            en = bool(base["enabled"])
        b_sym = str(item.get("symbol") or base["symbol"]).strip() or base["symbol"]
        try:
            lots_v = float(item.get("lots", base["lots"]))
        except Exception:
            lots_v = float(base["lots"])
        if lots_v <= 0:
            lots_v = float(base["lots"])
        try:
            pv_v = float(item.get("point_value", base["point_value"]))
        except Exception:
            pv_v = float(base["point_value"])
        if pv_v <= 0:
            pv_v = float(base["point_value"])
        try:
            tk_v = float(item.get("tick_size", base["tick_size"]))
        except Exception:
            tk_v = float(base["tick_size"])
        if tk_v <= 0:
            tk_v = float(base["tick_size"])
        out[sym] = {
            "enabled": en,
            "symbol": b_sym,
            "lots": round(lots_v, 4),
            "point_value": round(pv_v, 6),
            "tick_size": round(tk_v, 6),
        }
    return out


def round_to_tick(price: float, tick_size: float) -> float:
    tk = float(tick_size) if tick_size and tick_size > 0 else 0.01
    steps = round(float(price) / tk)
    val = steps * tk
    dec = 2
    if tk < 0.01:
        dec = 4
    elif tk >= 1.0:
        dec = 2
    return round(val, dec)


def fmt_qty_json(qty: float) -> Any:
    q = float(qty)
    if abs(q - round(q)) < 1e-6:
        return int(round(q))
    return round(q, 2)


def load_frozen_models() -> Dict[str, Any]:
    """Load frozen In-Sample DAVID k-NN memory banks, vol_caps, and routers (pure stdlib JSON)."""
    if not os.path.exists(FROZEN_MODELS_PATH):
        print(f"[FROZEN MODELS] WARNING: {FROZEN_MODELS_PATH} not found; DAVID k-NN running in fallback mode.")
        return {}
    try:
        with open(FROZEN_MODELS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for sym in PAIRS:
            sd = raw.get(sym, {}).get("david")
            if isinstance(sd, dict):
                sd["feat_mean"] = tuple(float(x) for x in sd.get("feat_mean", []))
                sd["feat_std"] = tuple(float(x) for x in sd.get("feat_std", []))
                sd["long_vecs"] = [tuple(float(v) for v in row) for row in sd.get("long_vecs", [])]
                sd["long_outcomes"] = [(float(r[0]), float(r[1])) for r in sd.get("long_outcomes", [])]
                sd["short_vecs"] = [tuple(float(v) for v in row) for row in sd.get("short_vecs", [])]
                sd["short_outcomes"] = [(float(r[0]), float(r[1])) for r in sd.get("short_outcomes", [])]
        print(
            "[FROZEN MODELS] Loaded unified_frozen_models.json ("
            + ", ".join(
                f"{s}: L={len(raw.get(s, {}).get('david', {}).get('long_vecs', []))}/S={len(raw.get(s, {}).get('david', {}).get('short_vecs', []))}"
                for s in PAIRS
            )
            + ")"
        )
        return raw
    except Exception as e:
        print(f"[FROZEN MODELS] Load error: {e}")
        return {}


FROZEN_MODELS: Dict[str, Any] = load_frozen_models()
_DAVID_FALLBACK_WARNED: set = set()


def query_david_memory(symbol: str, raw_vec: List[float], direction: str) -> Tuple[float, float]:
    """Pure-Python replica of DavidExperienceMemory.query_memory using frozen 45-Twin IS vectors."""
    d_mod = FROZEN_MODELS.get(symbol, {}).get("david")
    if not isinstance(d_mod, dict) or not d_mod.get("feat_mean"):
        if symbol not in _DAVID_FALLBACK_WARNED:
            _DAVID_FALLBACK_WARNED.add(symbol)
            print(f"[DAVID {symbol}] Frozen memory missing; returning neutral baseline.")
        return 68.0, 2.4

    mean = d_mod["feat_mean"]
    std = d_mod["feat_std"]
    k = int(d_mod.get("k", 45))
    n_dim = len(mean)
    norm_v = tuple((float(raw_vec[j]) - mean[j]) / std[j] for j in range(n_dim))

    is_long = direction in ("BUY", "LONG")
    vecs = d_mod["long_vecs"] if is_long else d_mod["short_vecs"]
    outs = d_mod["long_outcomes"] if is_long else d_mod["short_outcomes"]
    if not vecs or not outs or len(outs) < k:
        return 0.0, 0.0

    dists = []
    for idx, row in enumerate(vecs):
        d2 = 0.0
        for j in range(n_dim):
            diff = row[j] - norm_v[j]
            d2 += diff * diff
        if d2 < 1e-9:
            d2 = 1e9
        dists.append((d2, idx))

    nearest = heapq.nsmallest(k, dists, key=lambda x: x[0])
    weights = []
    sum_w = 0.0
    for d2, idx in nearest:
        w = 1.0 / (math.sqrt(d2) + 0.35)
        weights.append((w, outs[idx]))
        sum_w += w

    win_w = 0.0
    pos_r = 0.0
    neg_r = 0.0
    for w_raw, (win_val, net_r) in weights:
        w = w_raw / sum_w
        if win_val > 0.5:
            win_w += w
            pos_r += w * (net_r if net_r > 0.1 else 0.1)
        else:
            neg_r += w * abs(net_r if net_r < -0.1 else -0.1)

    return round(win_w * 100.0, 2), round(pos_r / max(neg_r, 1e-4), 2)


BTC_DAY_SESSION_ROUTER: Dict[Tuple[int, str], Tuple[Tuple[str, float], Tuple[str, float]]] = {
    (0, "ASIA"): (("10:OMAR", 21.6), ("POD_MARCUS_SOPHIA", 4.85)),
    (0, "LONDON"): (("L2_COIL240_LIQ_TRAP", 8.8), ("L2_COIL_MACRO_SUPPLY_TRAP", 7.41)),
    (0, "NEW_YORK"): (("L2_COIL240_LIQ_TRAP", 2.27), ("L2_COIL120_JUDAS_TRAP", 1.46)),
    (1, "ASIA"): (("5:MAX", 7.98), ("10:OMAR", 2.75)),
    (1, "LONDON"): (("L2_COIL120_JUDAS_TRAP", 3.27), ("L2_COIL240_LIQ_TRAP", 1.56)),
    (1, "NEW_YORK"): (("10:OMAR", 1.93), ("L2_COIL120_JUDAS_TRAP", 0.28)),
    (2, "ASIA"): (("10:OMAR", 2.28), ("L2_COIL120_JUDAS_TRAP", 2.20)),
    (2, "LONDON"): (("L2_COIL120_JUDAS_TRAP", 1.41), ("5:MAX", 0.39)),
    (2, "NEW_YORK"): (("10:OMAR", 5.88), ("L2_COIL120_JUDAS_TRAP", 1.13)),
    (3, "ASIA"): (("7:DEREK", 3.24), ("L2_COIL120_JUDAS_TRAP", 1.23)),
    (3, "LONDON"): (("10:OMAR", 4.68), ("L2_COIL120_JUDAS_TRAP", 0.35)),
    (3, "NEW_YORK"): (("5:MAX", 16.69), ("L2_COIL120_JUDAS_TRAP", 7.62)),
    (4, "ASIA"): (("5:MAX", 1.67), ("L2_COIL120_JUDAS_TRAP", 0.93)),
    (4, "LONDON"): (("L2_COIL120_JUDAS_TRAP", 6.49), ("5:MAX", 6.20)),
    (4, "NEW_YORK"): (("L2_COIL240_LIQ_TRAP", 8.24), ("L2_COIL120_JUDAS_TRAP", 1.90)),
    (5, "ASIA"): (("L2_COIL120_JUDAS_TRAP", 3.47), ("10:OMAR", 0.86)),
    (5, "LONDON"): (("L2_COIL60_JUDAS_TRAP", 1.69), ("L2_COIL120_JUDAS_TRAP", 0.35)),
    (5, "NEW_YORK"): (("L2_COIL240_LIQ_TRAP", 0.34), ("L2_COIL120_JUDAS_TRAP", 0.0)),
    (6, "ASIA"): (("L2_COIL60_JUDAS_TRAP", 14.66), ("L2_COIL240_LIQ_TRAP", 1.70)),
    (6, "LONDON"): (("L2_DELTA_COIL_ABSORPTION", 1.72), ("L2_COIL120_JUDAS_TRAP", 0.26)),
    (6, "NEW_YORK"): (("L2_COIL120_JUDAS_TRAP", 1.87), ("L2_COIL240_LIQ_TRAP", 1.14)),
}

ETH_DAY_SESSION_ROUTER: Dict[Tuple[int, str], Tuple[Tuple[str, float], Tuple[str, float]]] = {
    (0, "ASIA"): (("L2_ABSORPTION_WALL_BOUNCE", 9.67), ("1:ALEX", 5.41)),
    (0, "LONDON"): (("8:ELENA", 3.90), ("L2_DELTA_SWEEP_TRAP", 2.56)),
    (0, "NEW_YORK"): (("L2_DELTA_SWEEP_TRAP", 2.54), ("L2_ABSORPTION_WALL_BOUNCE", 0.99)),
    (1, "ASIA"): (("2:MAYA", 4.11), ("L2_DELTA_SWEEP_TRAP", 0.60)),
    (1, "LONDON"): (("10:OMAR", 2.22), ("L2_DELTA_SWEEP_TRAP", 1.25)),
    (1, "NEW_YORK"): (("L2_CVD_DIVERGENCE_REV", 3.89), ("L2_ABSORPTION_WALL_BOUNCE", 3.07)),
    (2, "ASIA"): (("L2_CVD_DIVERGENCE_REV", 4.16), ("POD_MARCUS_SOPHIA", 2.82)),
    (2, "LONDON"): (("L2_ABSORPTION_WALL_BOUNCE", 3.14), ("L2_DELTA_SWEEP_TRAP", 0.34)),
    (2, "NEW_YORK"): (("10:OMAR", 20.61), ("L2_ABSORPTION_WALL_BOUNCE", 0.93)),
    (3, "ASIA"): (("L2_CVD_DIVERGENCE_REV", 2.43), ("L2_ABSORPTION_WALL_BOUNCE", 1.97)),
    (3, "LONDON"): (("POD_MARCUS_SOPHIA", 5.06), ("L2_CVD_DIVERGENCE_REV", 1.43)),
    (3, "NEW_YORK"): (("L2_ABSORPTION_WALL_BOUNCE", 23.62), ("10:OMAR", 2.53)),
    (4, "ASIA"): (("L2_CVD_DIVERGENCE_REV", 7.50), ("L2_DELTA_SWEEP_TRAP", 0.23)),
    (4, "LONDON"): (("L2_DELTA_SWEEP_TRAP", 2.12), ("L2_ABSORPTION_WALL_BOUNCE", 1.53)),
    (4, "NEW_YORK"): (("L2_ABSORPTION_WALL_BOUNCE", 2.00), ("L2_DELTA_SWEEP_TRAP", 1.24)),
    (5, "ASIA"): (("8:ELENA", 1.52), ("L2_ABSORPTION_WALL_BOUNCE", 0.52)),
    (5, "LONDON"): (("10:OMAR", 1.79), ("L2_DELTA_SWEEP_TRAP", 1.42)),
    (5, "NEW_YORK"): (("L2_ABSORPTION_WALL_BOUNCE", 2.02), ("L2_DELTA_SWEEP_TRAP", 0.81)),
    (6, "ASIA"): (("10:OMAR", 1.53), ("L2_DELTA_SWEEP_TRAP", 0.65)),
    (6, "LONDON"): (("L2_ABSORPTION_WALL_BOUNCE", 1.16), ("L2_CVD_DIVERGENCE_REV", 1.08)),
    (6, "NEW_YORK"): (("10:OMAR", 3.18), ("L2_DELTA_SWEEP_TRAP", 0.23)),
}

BTC_COIL_CORE_AGENTS = {
    "L2_COIL60_JUDAS_TRAP",
    "L2_COIL_MACRO_SUPPLY_TRAP",
    "L2_COIL240_LIQ_TRAP",
    "L2_DELTA_COIL_ABSORPTION",
}
BTC_COIL_TRAP_AGENTS = BTC_COIL_CORE_AGENTS | {"L2_COIL120_JUDAS_TRAP"}

PLAYBOOK_MAP: Dict[str, Tuple[str, str, int]] = {
    "2:MAYA": ("5:LEO", "LEO_SWEEP_TRAP", 5),
    "6:SOPHIA": ("5:LEO", "LEO_KILLZONE_SWEEP", 4),
    "9:SAM": ("5:LEO", "LEO_LIQUIDITY_TRAP", 4),
    "7:DEREK": ("6:KAI", "KAI_SMC_OTE_TREND", 5),
    "4:NINA": ("6:KAI", "KAI_FVG_SMC", 4),
    "8:ELENA": ("9:ELENA", "ELENA_VWAP_WICK_REV", 5),
    "11:LIAM": ("9:ELENA", "ELENA_VWAP_VSA_POCKET", 5),
    "5:MAX": ("7:NINA", "NINA_RANGE_BOX_FADE", 4),
    "3:MARCUS": ("8:MAX", "MAX_BOX_BREAKOUT", 4),
    "10:OMAR": ("8:MAX", "MAX_DELTA_MOMENTUM", 5),
    "1:ALEX": ("8:MAX", "MAX_EMA_TECH", 3),
    "POD_MARCUS_SOPHIA": ("8:MAX", "MAX_BOX_BREAKOUT", 5),
    "POD_MAYA_SAM": ("5:LEO", "LEO_LIQUIDITY_TRAP", 5),
    "POD_MAYA_LIAM": ("9:ELENA", "ELENA_VWAP_VSA_POCKET", 5),
    "POD_SOPHIA_LIAM": ("9:ELENA", "ELENA_VWAP_VSA_POCKET", 4),
    "POD_DEREK_OMAR": ("6:KAI", "KAI_SMC_OTE_TREND", 5),
    "POD_NINA_MARCUS": ("6:KAI", "KAI_FVG_BOS_RETEST", 4),
    "L2_COIL60_JUDAS_TRAP": ("5:LEO", "BTC_COIL60_JUDAS_TRAP", 6),
    "L2_COIL_MACRO_SUPPLY_TRAP": ("6:KAI", "BTC_COIL_MACRO_SUPPLY_TRAP", 6),
    "L2_COIL240_LIQ_TRAP": ("11:SAM", "BTC_COIL240_LIQ_TRAP", 6),
    "L2_DELTA_COIL_ABSORPTION": ("11:SAM", "BTC_DELTA_COIL_ABSORPTION", 6),
    "L2_COIL120_JUDAS_TRAP": ("5:LEO", "BTC_COIL120_JUDAS_TRAP", 6),
    "L2_DELTA_SWEEP_TRAP": ("5:LEO", "L2_DELTA_SWEEP_TRAP", 6),
    "L2_CVD_DIVERGENCE_REV": ("4:OWEN", "L2_CVD_DIVERGENCE_REV", 6),
    "L2_ABSORPTION_WALL_BOUNCE": ("11:SAM", "L2_ABSORPTION_WALL_BOUNCE", 6),
    "L2_DELTA_DISPLACEMENT_MOM": ("8:MAX", "L2_DELTA_DISPLACEMENT_MOM", 6),
}


def now_ist() -> datetime:
    return datetime.now(timezone.utc) + IST_OFFSET


def fmt_ist_datetime(ts: Optional[float] = None) -> str:
    dt = (datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)) + IST_OFFSET
    return dt.strftime("%Y-%m-%d %H:%M")


def fmt_ist_date(ts: Optional[float] = None) -> str:
    dt = (datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)) + IST_OFFSET
    return dt.strftime("%Y-%m-%d")


def mask_secret(val: str, prefix: str = "dk_") -> str:
    s = str(val or "").strip()
    if not s:
        return "not_configured"
    if len(s) <= 4:
        return f"{prefix}****"
    return f"{prefix}****{s[-4:]}"


def load_env_config() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "PORT": int(os.getenv("MULTIBROKER_PORT", os.getenv("PORT", "5057"))),
        "ADMIN_PASSWORD": os.getenv("ADMIN_PASSWORD", "admin123"),
        "DELTA_BASE_URL": os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange"),
        "DELTA_API_KEY": os.getenv("DELTA_API_KEY", ""),
        "DELTA_API_SECRET": os.getenv("DELTA_API_SECRET", ""),
        "DELTA_ACCOUNT_ID": os.getenv("DELTA_ACCOUNT_ID", ""),
        "LEVERAGE": int(os.getenv("LEVERAGE", "100")),
        "FIXED_RISK_USD": float(os.getenv("FIXED_RISK_USD", "5.0")),
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", ""),
        "TELEGRAM_DAILY_IST": os.getenv("TELEGRAM_DAILY_IST", "23:55"),
        "INBOUND_WEBHOOK_TOKEN": os.getenv("INBOUND_WEBHOOK_TOKEN", ""),
    }
    if os.path.exists(ENV_PATH):
        try:
            with open(ENV_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        val = v.strip().strip('"').strip("'")
                        if k in ("PORT", "MULTIBROKER_PORT"):
                            if k == "MULTIBROKER_PORT":
                                cfg["PORT"] = int(val)
                        elif k == "LEVERAGE":
                            cfg["LEVERAGE"] = int(val)
                        elif k == "FIXED_RISK_USD":
                            cfg["FIXED_RISK_USD"] = float(val)
                        elif k in cfg:
                            cfg[k] = val
        except Exception:
            pass
    if cfg["PORT"] in (5050, 5055, 5056) and not os.getenv("FORCE_CUSTOM_PORT"):
        cfg["PORT"] = 5057
    return cfg


ENV_CFG = load_env_config()


# ==============================================================================
# MULTI-BROKER ACCOUNT CLIENT (DELTA, PICKMYTRADE, WEBHOOK, MT5_API, TRADOVATE)
# ==============================================================================
class MultiBrokerAccountClient:
    def __init__(self, acct_dict: Dict[str, Any], base_url: str = "https://api.india.delta.exchange"):
        self.id = str(acct_dict.get("id", f"acct_{int(time.time()*1000)}"))
        self.label = str(acct_dict.get("label", "Primary Account"))
        btype = str(acct_dict.get("broker_type", "DELTA")).upper()
        self.broker_type = btype if btype in BROKER_TYPES else "DELTA"
        self.mode = str(acct_dict.get("mode", "PAPER")).upper()
        smode = str(acct_dict.get("sizing_mode") or ("RISK_USD" if self.broker_type == "DELTA" else "FIXED_LOT")).upper()
        self.sizing_mode = smode if smode in SIZING_MODES else "FIXED_LOT"

        self.leverage = int(acct_dict.get("leverage", 100))
        self.risk_usd = float(acct_dict.get("risk_usd", 5.0))
        self.liq_guard = bool(acct_dict.get("liq_guard", True))
        # Strict privacy guarantee: ONLY a DELTA account can ever be the public default account
        self.is_default = bool(acct_dict.get("is_default", False)) if self.broker_type == "DELTA" else False
        emode = str(acct_dict.get("entry_order_mode", "MAKER_CHASER")).upper()
        self.entry_order_mode = emode if emode in DELTA_ENTRY_MODES else "MAKER_CHASER"

        # Per-pair symbol, fixed lots, $/point, tick size
        raw_enabled = acct_dict.get("enabled_pairs") if isinstance(acct_dict.get("enabled_pairs"), list) else None
        self.pair_configs: Dict[str, Dict[str, Any]] = normalize_pair_configs(
            acct_dict.get("pair_configs"), self.broker_type, enabled_pairs_fallback=raw_enabled
        )
        self.enabled_pairs: List[str] = [s for s in PAIRS if self.pair_configs[s]["enabled"]]

        # Delta Exchange credentials
        self.delta_account_id = str(acct_dict.get("delta_account_id", ""))
        self.api_key = str(acct_dict.get("api_key", ""))
        self.api_secret = str(acct_dict.get("api_secret", ""))
        self.base_url = base_url
        self.time_offset = 0

        # PickMyTrade & Generic Webhook credentials
        self.webhook_url = str(acct_dict.get("webhook_url", "")).strip()
        self.webhook_token = str(acct_dict.get("webhook_token", "")).strip()
        self.webhook_secret = str(acct_dict.get("webhook_secret", "")).strip()
        self.webhook_preset = str(acct_dict.get("webhook_preset", "STANDARD_JSON")).upper()
        self.custom_template = str(acct_dict.get("custom_template", "")).strip()
        self.broker_account_id = str(acct_dict.get("broker_account_id", "")).strip()
        self.strategy_name = str(acct_dict.get("strategy_name", "SyndicateGold")).strip() or "SyndicateGold"
        self.pickmytrade_template_json = str(acct_dict.get("pickmytrade_template_json", "")).strip()
        try:
            self.sl_buffer_pts = max(0.0, float(acct_dict.get("sl_buffer_pts", 3.0)))
        except Exception:
            self.sl_buffer_pts = 3.0
        try:
            self.fixed_sl_pts = max(0.0, float(acct_dict.get("fixed_sl_pts", 20.0)))
        except Exception:
            self.fixed_sl_pts = 20.0
        try:
            self.fixed_tp_pts = max(0.0, float(acct_dict.get("fixed_tp_pts", 35.0)))
        except Exception:
            self.fixed_tp_pts = 35.0

        raw_sl_mode = str(acct_dict.get("sl_mode") or "").upper()
        if raw_sl_mode in ("DOLLAR", "POINTS"):
            raw_sl_mode = "POINTS_BUFFER"
        if raw_sl_mode not in ("TEMPLATE_RAW", "FIXED_POINTS", "POINTS_BUFFER", "PERCENTAGE"):
            raw_sl_mode = "TEMPLATE_RAW" if self.pickmytrade_template_json else "FIXED_POINTS"
        self.sl_mode = raw_sl_mode
        self.send_tp = bool(acct_dict.get("send_tp", True))
        self.reverse_order_close = bool(acct_dict.get("reverse_order_close", True))
        self.multiple_accounts_json = str(acct_dict.get("multiple_accounts_json", "")).strip()

        # MT5 API / Bridge credentials
        self.mt5_login = str(acct_dict.get("mt5_login", "")).strip()
        self.mt5_server = str(acct_dict.get("mt5_server", "")).strip()
        self.mt5_api_url = str(acct_dict.get("mt5_api_url", "")).strip()
        self.bridge_token = str(acct_dict.get("bridge_token", "")).strip()
        if self.broker_type == "MT5_API" and not self.bridge_token:
            self.bridge_token = secrets.token_urlsafe(18)
        try:
            self.max_spread_pts = float(acct_dict.get("max_spread_pts", 1.5))
        except Exception:
            self.max_spread_pts = 1.5
        try:
            self.magic_number = int(acct_dict.get("magic_number", 505701))
        except Exception:
            self.magic_number = 505701
        self.last_bridge_heartbeat_ts = float(acct_dict.get("last_bridge_heartbeat_ts", 0.0))

        # Direct Tradovate REST API credentials
        self.tradovate_env = str(acct_dict.get("tradovate_env", "demo")).lower()
        if self.tradovate_env not in ("live", "demo"):
            self.tradovate_env = "demo"
        self.tradovate_user = str(acct_dict.get("tradovate_user", "")).strip()
        self.tradovate_pass = str(acct_dict.get("tradovate_pass", ""))
        self.tradovate_cid = str(acct_dict.get("tradovate_cid", "0")).strip()
        self.tradovate_sec = str(acct_dict.get("tradovate_sec", "")).strip()
        self.tradovate_access_token = ""
        self.tradovate_token_expiry = 0.0
        self.tradovate_resolved_acct_id: Optional[int] = None

        default_bal = 50000.0 if self.broker_type in ("PICKMYTRADE", "TRADOVATE_API") else 100.0
        raw_bal = acct_dict.get("wallet_balance_usd")
        raw_free = acct_dict.get("free_margin_usd")
        self.wallet_balance_usd = float(raw_bal) if raw_bal is not None else default_bal
        self.free_margin_usd = float(raw_free) if raw_free is not None else self.wallet_balance_usd
        self.last_balance_ts = 0.0

    @property
    def is_live_authenticated(self) -> bool:
        if self.mode != "LIVE":
            return False
        if self.broker_type == "DELTA":
            return bool(self.api_key and self.api_secret)
        if self.broker_type in ("PICKMYTRADE", "WEBHOOK"):
            return bool(self.webhook_url)
        if self.broker_type == "MT5_API":
            return bool(self.bridge_token or self.mt5_api_url)
        if self.broker_type == "TRADOVATE_API":
            return bool(self.tradovate_user and self.tradovate_pass and self.tradovate_sec)
        return False

    def get_pair_cfg(self, canonical_sym: str) -> Dict[str, Any]:
        return self.pair_configs.get(canonical_sym) or normalize_pair_configs({}, self.broker_type)[canonical_sym]

    def _masked_identifier(self) -> str:
        if self.broker_type == "DELTA":
            return mask_secret(self.api_key, "dk_")
        if self.broker_type == "PICKMYTRADE":
            return mask_secret(self.webhook_token or self.webhook_url, "pmt_")
        if self.broker_type == "WEBHOOK":
            return mask_secret(self.webhook_secret or self.webhook_url, "wh_")
        if self.broker_type == "MT5_API":
            return f"MT5:{self.mt5_login or 'bridge'}"
        if self.broker_type == "TRADOVATE_API":
            return f"TV:{self.tradovate_user or 'api'}"
        return "configured"

    def to_public_dict(self) -> Dict[str, Any]:
        self.enabled_pairs = [s for s in PAIRS if self.pair_configs[s]["enabled"]]
        return {
            "id": self.id,
            "label": self.label,
            "broker_type": self.broker_type,
            "sizing_mode": self.sizing_mode,
            "delta_account_id": self.delta_account_id or "—",
            "masked_key": self._masked_identifier(),
            "is_default": self.is_default if self.broker_type == "DELTA" else False,
            "mode": self.mode,
            "entry_order_mode": self.entry_order_mode,
            "risk_usd": round(self.risk_usd, 2),
            "leverage": self.leverage,
            "liq_guard": self.liq_guard,
            "enabled_pairs": list(self.enabled_pairs),
            "pair_configs": self.pair_configs,
            "webhook_url": self.webhook_url,
            "webhook_token_masked": mask_secret(self.webhook_token, "tok_") if self.webhook_token else "",
            "webhook_secret_masked": mask_secret(self.webhook_secret, "sec_") if self.webhook_secret else "",
            "webhook_preset": self.webhook_preset,
            "custom_template": self.custom_template,
            "broker_account_id": self.broker_account_id,
            "strategy_name": self.strategy_name,
            "pickmytrade_template_json": self.pickmytrade_template_json,
            "sl_buffer_pts": self.sl_buffer_pts,
            "fixed_sl_pts": self.fixed_sl_pts,
            "fixed_tp_pts": self.fixed_tp_pts,
            "sl_mode": self.sl_mode,
            "send_tp": self.send_tp,
            "reverse_order_close": self.reverse_order_close,
            "multiple_accounts_json": self.multiple_accounts_json,
            "mt5_login": self.mt5_login,
            "mt5_server": self.mt5_server,
            "mt5_api_url": self.mt5_api_url,
            "bridge_token": self.bridge_token,
            "bridge_online": (time.time() - self.last_bridge_heartbeat_ts) < 30.0 if self.last_bridge_heartbeat_ts > 0 else False,
            "max_spread_pts": self.max_spread_pts,
            "magic_number": self.magic_number,
            "tradovate_env": self.tradovate_env,
            "tradovate_user": self.tradovate_user,
            "tradovate_cid": self.tradovate_cid,
            "wallet_balance_usd": round(self.wallet_balance_usd, 2),
            "free_margin_usd": round(self.free_margin_usd, 2),
        }

    def to_storage_dict(self) -> Dict[str, Any]:
        d = self.to_public_dict()
        d["api_key"] = self.api_key
        d["api_secret"] = self.api_secret
        d["webhook_token"] = self.webhook_token
        d["webhook_secret"] = self.webhook_secret
        d["tradovate_pass"] = self.tradovate_pass
        d["tradovate_sec"] = self.tradovate_sec
        return d

    # --------------------------------------------------------------------------
    # DELTA EXCHANGE INDIA REST METHODS
    # --------------------------------------------------------------------------
    def _sign(self, method: str, path: str, body: str, ts: str) -> str:
        msg = method + ts + path + body
        return hmac.new(self.api_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

    def _headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        ts = str(int(time.time() + self.time_offset))
        sig = self._sign(method, path, body, ts) if self.api_secret else ""
        return {
            "api-key": self.api_key,
            "timestamp": ts,
            "signature": sig,
            "Content-Type": "application/json",
            "User-Agent": "SyndicateMultiBroker/4.0",
        }

    def request(self, method: str, path: str, payload_dict: Optional[Dict[str, Any]] = None, tag: str = "DELTA") -> Dict[str, Any]:
        url = self.base_url + path
        payload = json.dumps(payload_dict) if payload_dict is not None else ""
        data = payload.encode("utf-8") if payload else None
        req = urllib.request.Request(url, data=data, headers=self._headers(method, path, payload), method=method)
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                date_hdr = r.headers.get("Date")
                if date_hdr:
                    try:
                        from email.utils import parsedate_to_datetime
                        srv_ts = int(parsedate_to_datetime(date_hdr).timestamp())
                        self.time_offset = srv_ts - int(time.time())
                    except Exception:
                        pass
                res = json.loads(r.read().decode("utf-8"))
                if res.get("success"):
                    return res
                return {"success": False, "error": str(res.get("error", res))}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            return {"success": False, "error": f"HTTP {e.code}: {err_body}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def fetch_balance(self, force: bool = False) -> Tuple[float, float]:
        if self.broker_type != "DELTA" or not self.api_key or not self.api_secret:
            return self.wallet_balance_usd, self.free_margin_usd
        if not force and (time.time() - self.last_balance_ts) < 12.0:
            return self.wallet_balance_usd, self.free_margin_usd
        res = self.request("GET", "/v2/wallet/balances", tag="BALANCE")
        if res.get("success"):
            for b in res.get("result", []):
                if b.get("asset_symbol") in ("USD", "USDT"):
                    avail = float(b.get("available_balance", 0.0))
                    bal = float(b.get("balance", avail))
                    self.free_margin_usd = round(avail, 2)
                    self.wallet_balance_usd = round(bal, 2)
                    self.last_balance_ts = time.time()
                    break
        return self.wallet_balance_usd, self.free_margin_usd

    def ensure_leverage(self, symbol: str):
        if self.broker_type != "DELTA" or not self.is_live_authenticated:
            return
        pid = PAIR_SPECS[symbol]["product_id"]
        self.request("POST", f"/v2/products/{pid}/orders/leverage", {"leverage": str(self.leverage)}, tag=f"LEV_{symbol}")

    def fetch_open_positions_map(self) -> Optional[Dict[str, Dict[str, Any]]]:
        if self.broker_type != "DELTA" or not self.is_live_authenticated:
            return None
        res = self.request("GET", "/v2/positions/margined", tag="POS_SYNC")
        if not res.get("success"):
            return None
        out: Dict[str, Dict[str, Any]] = {s: {"size": 0} for s in PAIRS}
        for pos in res.get("result", []):
            sym = str(pos.get("product_symbol", ""))
            pid = int(pos.get("product_id", 0) or 0)
            matched_sym = None
            for s, spec in PAIR_SPECS.items():
                if sym == s or (pid and pid == spec["product_id"]):
                    matched_sym = s
                    break
            if matched_sym:
                sz = int(float(pos.get("size", 0)))
                if sz != 0:
                    out[matched_sym] = {
                        "size": sz,
                        "direction": "LONG" if sz > 0 else "SHORT",
                        "entry_price": float(pos.get("entry_price", 0)),
                        "margin": float(pos.get("margin", 0)),
                        "unrealized_pnl": float(pos.get("unrealized_pnl", 0)),
                    }
        return out

    def place_market_order(self, symbol: str, direction: str, lots: Any, ref_price: float, reduce_only: bool = False) -> Dict[str, Any]:
        side = "buy" if direction == "LONG" else "sell"
        if reduce_only:
            side = "sell" if direction == "LONG" else "buy"
        if self.broker_type == "DELTA" and self.is_live_authenticated:
            payload: Dict[str, Any] = {
                "product_id": PAIR_SPECS[symbol]["product_id"],
                "product_symbol": symbol,
                "size": int(round(float(lots))),
                "side": side,
                "order_type": "market_order",
            }
            if reduce_only:
                payload["reduce_only"] = True
            return self.request("POST", "/v2/orders", payload, tag=f"MKT_{symbol}")
        return {
            "success": True,
            "result": {"id": f"PAPER_{int(time.time()*1000)}", "average_fill_price": str(ref_price)},
        }

    def place_entry_order(self, symbol: str, direction: str, lots: Any, ref_price: float) -> Dict[str, Any]:
        """
        Delta Maker-First Entry Chaser:
        - In MAKER_CHASER mode on LIVE Delta, places a post_only limit order at best bid (LONG) / best ask (SHORT)
          for up to 2.4s to capture Maker fee (0.01% Gold / 0.02% BTC/ETH + GST), then falls back to Market
          for any unfilled remainder so zero valid setups are ever missed.
        """
        int_lots = max(1, int(round(float(lots))))
        if self.broker_type != "DELTA" or not self.is_live_authenticated or self.entry_order_mode == "MARKET":
            res = self.place_market_order(symbol, direction, int_lots, ref_price, reduce_only=False)
            res["entry_is_maker"] = False
            return res

        pid = PAIR_SPECS[symbol]["product_id"]
        side = "buy" if direction == "LONG" else "sell"
        tk = float(self.get_pair_cfg(symbol).get("tick_size", 0.01) or 0.01)
        l2_d = HUB.markets[symbol].l2_depth if symbol in HUB.markets else {}
        bids = l2_d.get("bids") or []
        asks = l2_d.get("asks") or []
        if direction == "LONG":
            best_px = float(bids[0][0]) if bids else (float(ref_price) - tk)
        else:
            best_px = float(asks[0][0]) if asks else (float(ref_price) + tk)
        px_str = f"{round_to_tick(best_px, tk):.2f}"

        lim_res = self.request("POST", "/v2/orders", {
            "product_id": pid,
            "product_symbol": symbol,
            "size": int_lots,
            "side": side,
            "order_type": "limit_order",
            "limit_price": px_str,
            "post_only": True,
        }, tag=f"MAKER_ENT_{symbol}")

        if not lim_res.get("success") or not isinstance(lim_res.get("result"), dict):
            mkt_res = self.place_market_order(symbol, direction, int_lots, ref_price, reduce_only=False)
            mkt_res["entry_is_maker"] = False
            return mkt_res

        ord_obj = lim_res["result"]
        oid = ord_obj.get("id")
        init_state = str(ord_obj.get("state", "")).lower()
        if not oid or init_state in ("cancelled", "rejected"):
            mkt_res = self.place_market_order(symbol, direction, int_lots, ref_price, reduce_only=False)
            mkt_res["entry_is_maker"] = False
            return mkt_res

        filled_lots = 0
        fill_px_lim = float(px_str)
        for _ in range(4):
            time.sleep(0.6)
            chk = self.request("GET", f"/v2/orders/{oid}", tag=f"CHK_ENT_{symbol}")
            if chk.get("success") and isinstance(chk.get("result"), dict):
                o_cur = chk["result"]
                st = str(o_cur.get("state", "")).lower()
                unfilled = int(round(float(o_cur.get("unfilled_size", int_lots) or 0)))
                filled_lots = max(0, int_lots - unfilled)
                avg_p = o_cur.get("average_fill_price") or o_cur.get("limit_price")
                if avg_p:
                    try:
                        fill_px_lim = float(avg_p)
                    except Exception:
                        pass
                if unfilled <= 0 or st == "closed":
                    if unfilled <= 0:
                        return {
                            "success": True,
                            "entry_is_maker": True,
                            "maker_lots": int_lots,
                            "taker_lots": 0,
                            "result": {"id": oid, "average_fill_price": str(round(fill_px_lim, 2))},
                        }
                    break
                if st in ("cancelled", "rejected"):
                    break

        # Cancel remaining resting order and market-fill any remainder so no trade is missed
        self.request("DELETE", "/v2/orders", {"id": oid, "product_id": pid}, tag=f"CANCEL_ENT_{symbol}")
        rem_lots = max(0, int_lots - filled_lots)
        if rem_lots <= 0:
            return {
                "success": True,
                "entry_is_maker": True,
                "maker_lots": int_lots,
                "taker_lots": 0,
                "result": {"id": oid, "average_fill_price": str(round(fill_px_lim, 2))},
            }

        mkt_res = self.place_market_order(symbol, direction, rem_lots, ref_price, reduce_only=False)
        if not mkt_res.get("success"):
            if filled_lots > 0:
                return {
                    "success": True,
                    "entry_is_maker": True,
                    "partial_entry_lots": filled_lots,
                    "maker_lots": filled_lots,
                    "taker_lots": 0,
                    "result": {"id": oid, "average_fill_price": str(round(fill_px_lim, 2))},
                }
            return mkt_res

        mkt_px_raw = (mkt_res.get("result") or {}).get("average_fill_price") if isinstance(mkt_res.get("result"), dict) else None
        mkt_px = float(mkt_px_raw) if mkt_px_raw else float(ref_price)
        blended_px = ((filled_lots * fill_px_lim) + (rem_lots * mkt_px)) / float(int_lots)
        mkt_res["entry_is_maker"] = (filled_lots > 0 and rem_lots == 0)
        mkt_res["maker_lots"] = filled_lots
        mkt_res["taker_lots"] = rem_lots
        if isinstance(mkt_res.get("result"), dict):
            mkt_res["result"]["average_fill_price"] = str(round(blended_px, 2))
        return mkt_res

    def place_maker_tp_order(self, symbol: str, direction: str, lots: Any, tp_price: float) -> Optional[Any]:
        """Places a resting exchange-side reduce-only Maker Limit TP order on Delta Exchange."""
        if self.broker_type != "DELTA" or float(lots) <= 0 or not self.is_live_authenticated:
            return None
        side = "sell" if direction == "LONG" else "buy"
        px_str = f"{round(float(tp_price), 2):.2f}"
        res = self.request("POST", "/v2/orders", {
            "product_id": PAIR_SPECS[symbol]["product_id"],
            "product_symbol": symbol,
            "size": int(round(float(lots))),
            "side": side,
            "order_type": "limit_order",
            "limit_price": px_str,
            "post_only": True,
            "reduce_only": True,
        }, tag=f"MAKER_TP_{symbol}")
        if res.get("success") and isinstance(res.get("result"), dict):
            return res["result"].get("id")
        return None

    def sync_exchange_sl(self, symbol: str, direction: str, lots: Any, sl_price: float, pos_state: Dict[str, Any]):
        """Places or updates a resting exchange-side reduce-only Stop-Loss order on Delta Exchange."""
        if self.broker_type != "DELTA":
            pos_state["exchange_sl_price"] = round(float(sl_price), 2)
            return
        target_sl = round(float(sl_price), 2)
        sl_str = f"{target_sl:.2f}"
        side = "sell" if direction == "LONG" else "buy"
        pid = PAIR_SPECS[symbol]["product_id"]
        int_lots = int(round(float(lots)))

        if not self.is_live_authenticated:
            pos_state["exchange_sl_price"] = target_sl
            return

        if pos_state.get("sl_order_id") and pos_state.get("exchange_sl_price") == target_sl and pos_state.get("sl_order_lots") == int_lots:
            return

        if pos_state.get("sl_order_id") is not None:
            upd = self.request("PUT", "/v2/orders", {
                "id": pos_state["sl_order_id"],
                "product_id": pid,
                "product_symbol": symbol,
                "size": int_lots,
                "stop_price": sl_str,
            }, tag=f"SL_UPD_{symbol}")
            if upd.get("success"):
                pos_state["exchange_sl_price"] = target_sl
                pos_state["sl_order_lots"] = int_lots
                return
            self.request("DELETE", "/v2/orders", {"id": pos_state["sl_order_id"], "product_id": pid}, tag=f"SL_DEL_{symbol}")
            pos_state["sl_order_id"] = None

        place = self.request("POST", "/v2/orders", {
            "product_id": pid,
            "product_symbol": symbol,
            "size": int_lots,
            "side": side,
            "order_type": "market_order",
            "stop_order_type": "stop_loss_order",
            "stop_price": sl_str,
            "stop_trigger_method": "mark_price",
            "reduce_only": True,
        }, tag=f"SL_PLACE_{symbol}")
        if place.get("success") and isinstance(place.get("result"), dict):
            pos_state["sl_order_id"] = place["result"].get("id")
            pos_state["exchange_sl_price"] = target_sl
            pos_state["sl_order_lots"] = int_lots

    def cancel_all_symbol_orders(self, symbol: str, pos_state: Optional[Dict[str, Any]] = None):
        """Zero-Ghost Cleanup: Cancels all resting TP & SL orders for this symbol on Delta Exchange."""
        if self.broker_type != "DELTA":
            if pos_state:
                pos_state["sl_order_id"] = None
                pos_state["exchange_sl_price"] = None
                pos_state["tp_order_ids"] = []
            return
        pid = PAIR_SPECS[symbol]["product_id"]
        if self.is_live_authenticated:
            if pos_state:
                if pos_state.get("sl_order_id"):
                    self.request("DELETE", "/v2/orders", {"id": pos_state["sl_order_id"], "product_id": pid}, tag=f"GHOST_SL_{symbol}")
                for tp_oid in pos_state.get("tp_order_ids", []):
                    if tp_oid:
                        self.request("DELETE", "/v2/orders", {"id": tp_oid, "product_id": pid}, tag=f"GHOST_TP_{symbol}")
            open_res = self.request("GET", f"/v2/orders?product_id={pid}&states=open,pending", tag=f"SWEEP_GET_{symbol}")
            if open_res.get("success"):
                for o in open_res.get("result", []):
                    oid = o.get("id")
                    if oid:
                        self.request("DELETE", "/v2/orders", {"id": oid, "product_id": pid}, tag=f"SWEEP_DEL_{symbol}")
        if pos_state:
            pos_state["sl_order_id"] = None
            pos_state["exchange_sl_price"] = None
            pos_state["tp_order_ids"] = []

    def fetch_latest_fill(self, symbol: str, since_ts: int) -> Optional[Dict[str, Any]]:
        """Query /v2/fills for the latest realized execution price, commission & maker/taker role."""
        if self.broker_type != "DELTA" or not self.is_live_authenticated:
            return None
        pid = PAIR_SPECS[symbol]["product_id"]
        res = self.request("GET", f"/v2/fills?product_id={pid}&page_size=10", tag=f"FILLS_{symbol}")
        if not res.get("success"):
            return None
        fills = res.get("result") or []
        best_fill = None
        best_ts = -1
        for f in fills:
            if not isinstance(f, dict):
                continue
            raw_t = f.get("created_at", f.get("timestamp", 0))
            f_ts = 0
            if isinstance(raw_t, (int, float)):
                f_ts = int(raw_t // 1_000_000) if raw_t > 10_000_000_000 else int(raw_t)
            elif isinstance(raw_t, str) and raw_t:
                try:
                    dt_f = datetime.fromisoformat(raw_t.replace("Z", "+00:00"))
                    f_ts = int(dt_f.timestamp())
                except Exception:
                    f_ts = 0
            if f_ts and f_ts < since_ts - 5:
                continue
            try:
                px = float(f.get("price", 0.0))
            except Exception:
                px = 0.0
            if px <= 0:
                continue
            if f_ts >= best_ts:
                best_ts = f_ts
                paid_comm = f.get("paid_commission")
                raw_comm = f.get("commission")
                if paid_comm is not None and abs(float(paid_comm or 0.0)) > 0:
                    comm_usd = abs(float(paid_comm))
                elif raw_comm is not None and abs(float(raw_comm or 0.0)) > 0:
                    comm_usd = round(abs(float(raw_comm)) * DELTA_GST_MULT, 4)
                else:
                    comm_usd = 0.0
                best_fill = {
                    "price": round(px, 2),
                    "role": str(f.get("role", "taker")).lower(),
                    "commission": comm_usd,
                    "size": int(abs(float(f.get("size", 0) or 0))),
                    "ts": f_ts,
                }
        return best_fill

    # --------------------------------------------------------------------------
    # PICKMYTRADE, WEBHOOK, MT5 API & TRADOVATE API PAYLOADS + DISPATCHERS
    # --------------------------------------------------------------------------
    def build_pickmytrade_payload(
        self,
        action: str,
        canonical_sym: str,
        direction: str,
        qty: float,
        ref_px: float,
        sl_dist: float,
        tp_dist: float,
        reason: str = "",
    ) -> Dict[str, Any]:
        pcfg = self.get_pair_cfg(canonical_sym)
        b_sym = pcfg["symbol"]
        tk = float(pcfg["tick_size"])
        is_long = direction == "LONG"
        qty_val = fmt_qty_json(qty)
        utc_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        px_rounded = round_to_tick(ref_px, tk)

        parsed_tpl: Optional[Dict[str, Any]] = None
        if self.pickmytrade_template_json:
            try:
                tmp = json.loads(self.pickmytrade_template_json)
                if isinstance(tmp, dict):
                    parsed_tpl = json.loads(json.dumps(tmp))
            except Exception:
                parsed_tpl = None

        use_market_pos_verbs = True
        if parsed_tpl is not None:
            raw_data_str = str(parsed_tpl.get("data", ""))
            if raw_data_str and ("action" in raw_data_str or raw_data_str.lower() in ("buy", "sell")):
                use_market_pos_verbs = False
            payload = parsed_tpl
        else:
            payload = {
                "symbol": b_sym,
                "strategy_name": self.strategy_name,
                "date": utc_iso,
                "data": "long" if is_long else "short",
                "quantity": qty_val,
                "risk_percentage": 0,
                "price": px_rounded,
                "tp": 0,
                "percentage_tp": 0,
                "dollar_tp": 0,
                "sl": 0,
                "dollar_sl": 0,
                "percentage_sl": 0,
                "trail": 0,
                "trail_stop": 0,
                "trail_trigger": 0,
                "trail_freq": 0,
                "update_tp": False,
                "update_sl": False,
                "breakeven": 0,
                "breakeven_offset": 0,
                "token": self.webhook_token,
                "pyramid": False,
                "same_direction_ignore": False,
                "reverse_order_close": bool(self.reverse_order_close),
            }

        payload["symbol"] = b_sym
        payload["date"] = utc_iso
        payload["price"] = px_rounded
        if self.strategy_name and not payload.get("strategy_name"):
            payload["strategy_name"] = self.strategy_name
        if self.webhook_token:
            payload["token"] = self.webhook_token
        payload["reverse_order_close"] = bool(self.reverse_order_close)

        if action == "CLOSE":
            payload["data"] = "flat" if use_market_pos_verbs else "close"
            payload["quantity"] = 0 if use_market_pos_verbs else qty_val
            payload["tp"] = 0
            payload["percentage_tp"] = 0
            payload["dollar_tp"] = 0
            payload["sl"] = 0
            payload["dollar_sl"] = 0
            payload["percentage_sl"] = 0
        elif action == "PARTIAL_CLOSE":
            payload["data"] = "sell" if is_long else "buy"
            payload["quantity"] = qty_val
            payload["reverse_order_close"] = False
            payload["tp"] = 0
            payload["percentage_tp"] = 0
            payload["dollar_tp"] = 0
            payload["sl"] = 0
            payload["dollar_sl"] = 0
            payload["percentage_sl"] = 0
        else:
            payload["data"] = ("long" if is_long else "short") if use_market_pos_verbs else ("buy" if is_long else "sell")
            payload["quantity"] = qty_val
            if self.sl_mode == "TEMPLATE_RAW":
                if parsed_tpl is None:
                    payload["tp"] = 0
                    payload["percentage_tp"] = 0
                    payload["sl"] = 0
                    payload["percentage_sl"] = 0
                    payload["dollar_sl"] = round(float(self.fixed_sl_pts), 2)
                    payload["dollar_tp"] = round(float(self.fixed_tp_pts), 2) if self.send_tp else 0
                elif not self.send_tp:
                    payload["tp"] = 0
                    payload["percentage_tp"] = 0
                    payload["dollar_tp"] = 0
            elif self.sl_mode == "FIXED_POINTS":
                payload["tp"] = 0
                payload["percentage_tp"] = 0
                payload["sl"] = 0
                payload["percentage_sl"] = 0
                payload["dollar_sl"] = round(float(self.fixed_sl_pts), 2)
                payload["dollar_tp"] = round(float(self.fixed_tp_pts), 2) if self.send_tp else 0
            elif self.sl_mode == "POINTS_BUFFER":
                # PickMyTrade V2 uses raw price points under dollar_sl / dollar_tp (NEVER multiply by $/pt)
                payload["tp"] = 0
                payload["percentage_tp"] = 0
                payload["sl"] = 0
                payload["percentage_sl"] = 0
                payload["dollar_sl"] = round(float(sl_dist) + float(self.sl_buffer_pts), 2)
                payload["dollar_tp"] = round(float(tp_dist), 2) if (self.send_tp and float(tp_dist) > 0) else 0
            else:  # PERCENTAGE
                payload["tp"] = 0
                payload["dollar_tp"] = 0
                payload["sl"] = 0
                payload["dollar_sl"] = 0
                payload["percentage_sl"] = round((float(sl_dist) / max(float(ref_px), 1e-6)) * 100.0, 4)
                payload["percentage_tp"] = round((float(tp_dist) / max(float(ref_px), 1e-6)) * 100.0, 4) if (self.send_tp and float(tp_dist) > 0) else 0

        if self.multiple_accounts_json:
            try:
                parsed_multi = json.loads(self.multiple_accounts_json)
                if isinstance(parsed_multi, list) and parsed_multi:
                    payload["multiple_accounts"] = parsed_multi
            except Exception:
                pass
        elif "multiple_accounts" not in payload and self.broker_account_id:
            payload["multiple_accounts"] = [{
                "token": payload.get("token", ""),
                "account_id": self.broker_account_id,
                "risk_percentage": 0,
                "quantity_multiplier": 1,
            }]
        return payload

    def build_generic_webhook_payload(
        self,
        action: str,
        canonical_sym: str,
        direction: str,
        qty: float,
        ref_px: float,
        sl_dist: float,
        tp_dist: float,
        trail_offset: float = 0.0,
        reason: str = "",
    ) -> Dict[str, Any]:
        pcfg = self.get_pair_cfg(canonical_sym)
        b_sym = pcfg["symbol"]
        pv = float(pcfg["point_value"])
        tk = float(pcfg["tick_size"])
        is_long = direction == "LONG"
        side = "buy" if is_long else "sell"
        qty_val = fmt_qty_json(qty)
        utc_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        preset = self.webhook_preset

        if preset == "TRADERSPOST":
            if action == "CLOSE":
                return {"ticker": b_sym, "action": "exit", "quantity": qty_val}
            if action == "PARTIAL_CLOSE":
                return {"ticker": b_sym, "action": "sell" if is_long else "buy", "orderType": "market", "quantity": qty_val}
            out_tp: Dict[str, Any] = {
                "ticker": b_sym,
                "action": side,
                "orderType": "market",
                "quantityType": "fixed_quantity",
                "quantity": qty_val,
                "stopLoss": {"type": "stop", "amount": round(float(sl_dist), 4)},
            }
            if float(tp_dist) > 0:
                out_tp["takeProfit"] = {"amount": round(float(tp_dist), 4)}
            return out_tp

        if preset == "PINECONNECTOR":
            if action == "CLOSE":
                cmd = f"{self.webhook_secret},{'closelong' if is_long else 'closeshort'},{b_sym}"
            elif action == "PARTIAL_CLOSE":
                cmd = f"{self.webhook_secret},{'closelongpct' if is_long else 'closeshortpct'},{b_sym},risk=50"
            elif action == "MODIFY_SL":
                cmd = f"{self.webhook_secret},{'newsltplong' if is_long else 'newsltpshort'},{b_sym},sl={round(float(trail_offset), 4)}"
            else:
                cmd = f"{self.webhook_secret},{side},{b_sym},risk={qty_val},sl={round(float(sl_dist), 4)},tp={round(float(tp_dist), 4)}"
            return {"command": cmd, "symbol": b_sym, "action": action, "quantity": qty_val}

        if preset == "CUSTOM_TEMPLATE" and self.custom_template:
            repl = {
                "{{action}}": action,
                "{{symbol}}": b_sym,
                "{{canonical_symbol}}": canonical_sym,
                "{{side}}": side,
                "{{direction}}": direction,
                "{{qty}}": str(qty_val),
                "{{quantity}}": str(qty_val),
                "{{price}}": str(round_to_tick(ref_px, tk)),
                "{{sl_dist}}": str(round(float(sl_dist), 4)),
                "{{tp_dist}}": str(round(float(tp_dist), 4)),
                "{{trail_offset}}": str(round(float(trail_offset), 4)),
                "{{dollar_sl}}": str(round(float(sl_dist) * pv, 2)),
                "{{dollar_tp}}": str(round(float(tp_dist) * pv, 2)),
                "{{token}}": self.webhook_token,
                "{{secret}}": self.webhook_secret,
                "{{account_id}}": self.broker_account_id,
                "{{reason}}": reason,
                "{{timenow}}": utc_iso,
            }
            txt = self.custom_template
            for k, v in repl.items():
                txt = txt.replace(k, v)
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {"text": txt}

        return {
            "event": action,
            "account_label": self.label,
            "canonical_symbol": canonical_sym,
            "symbol": b_sym,
            "side": side,
            "direction": direction,
            "quantity": qty_val,
            "ref_price": round_to_tick(ref_px, tk),
            "sl_dist": round(float(sl_dist), 4),
            "tp_dist": round(float(tp_dist), 4),
            "trail_offset": round(float(trail_offset), 4),
            "point_value": pv,
            "tick_size": tk,
            "reason": reason,
            "secret": self.webhook_secret,
            "timestamp_utc": utc_iso,
        }

    def build_mt5_command(
        self,
        action: str,
        canonical_sym: str,
        direction: str,
        lots: float,
        sl_dist: float,
        tp_dist: float,
        trail_offset: float = 0.0,
        reason: str = "",
    ) -> Dict[str, Any]:
        pcfg = self.get_pair_cfg(canonical_sym)
        return {
            "cmd_id": f"cmd_{int(time.time()*1000)}_{secrets.token_hex(3)}",
            "ts": int(time.time()),
            "account_id": self.id,
            "action": action,
            "symbol": pcfg["symbol"],
            "canonical_symbol": canonical_sym,
            "direction": direction,
            "lots": round(float(lots), 2),
            "sl_dist": round(float(sl_dist), 4),
            "tp_dist": round(float(tp_dist), 4),
            "trail_offset": round(float(trail_offset), 4),
            "tick_size": float(pcfg["tick_size"]),
            "max_spread_pts": round(float(self.max_spread_pts), 4),
            "magic_number": int(self.magic_number),
            "reason": reason,
        }

    def post_webhook_json(self, url: str, payload: Dict[str, Any], secret: str = "") -> Dict[str, Any]:
        if not url or not (url.startswith("http://") or url.startswith("https://")):
            return {"success": False, "http_code": 0, "response": "", "error": "Invalid webhook URL"}
        hdrs = {
            "Content-Type": "application/json",
            "User-Agent": "SyndicateMultiBroker/4.0",
        }
        if secret:
            hdrs["X-Webhook-Secret"] = secret
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                body = r.read().decode("utf-8", errors="replace")[:500]
                return {"success": True, "http_code": int(r.status), "response": body, "error": ""}
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:500]
            return {"success": False, "http_code": int(e.code), "response": body, "error": f"HTTP {e.code}: {body}"}
        except Exception as e:
            return {"success": False, "http_code": 0, "response": "", "error": str(e)}

    # --------------------------------------------------------------------------
    # DIRECT TRADOVATE REST API METHODS
    # --------------------------------------------------------------------------
    def _tradovate_base_url(self) -> str:
        return "https://live.tradovateapi.com/v1" if self.tradovate_env == "live" else "https://demo.tradovateapi.com/v1"

    def _tradovate_req(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None, auth_token: str = "") -> Dict[str, Any]:
        url = self._tradovate_base_url() + path
        hdrs = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "SyndicateMultiBroker/4.0"}
        if auth_token:
            hdrs["Authorization"] = f"Bearer {auth_token}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                txt = r.read().decode("utf-8", errors="replace")
                return {"success": True, "http_code": int(r.status), "data": json.loads(txt) if txt else {}}
        except urllib.error.HTTPError as e:
            txt = e.read().decode("utf-8", errors="replace")[:500]
            return {"success": False, "http_code": int(e.code), "error": f"HTTP {e.code}: {txt}"}
        except Exception as e:
            return {"success": False, "http_code": 0, "error": str(e)}

    def tradovate_authenticate(self, force: bool = False) -> Tuple[bool, str]:
        if not force and self.tradovate_access_token and time.time() < self.tradovate_token_expiry:
            return True, "Cached token valid"
        if not self.tradovate_user or not self.tradovate_pass or not self.tradovate_sec:
            return False, "Tradovate Username, Password, and API Secret (SEC) are required"
        try:
            cid_val = int(self.tradovate_cid or 0)
        except Exception:
            cid_val = 0
        res = self._tradovate_req("POST", "/auth/accesstokenrequest", {
            "name": self.tradovate_user,
            "password": self.tradovate_pass,
            "appId": "SyndicateMultiBroker",
            "appVersion": "1.0",
            "cid": cid_val,
            "sec": self.tradovate_sec,
            "deviceId": self.id,
        })
        if not res.get("success"):
            return False, str(res.get("error", "Tradovate auth failed"))
        d = res.get("data") or {}
        if isinstance(d, dict) and d.get("accessToken"):
            self.tradovate_access_token = str(d["accessToken"])
            self.tradovate_token_expiry = time.time() + 3600.0
            return True, f"Authenticated ({self.tradovate_env.upper()})"
        err = d.get("errorText") if isinstance(d, dict) else str(d)
        return False, str(err or "Tradovate rejected credentials / API key not enabled")

    def test_tradovate_connection(self) -> Dict[str, Any]:
        ok, msg = self.tradovate_authenticate(force=True)
        if not ok:
            return {"success": False, "error": msg}
        accts_res = self._tradovate_req("GET", "/account/list", auth_token=self.tradovate_access_token)
        if not accts_res.get("success"):
            return {"success": True, "message": msg, "accounts": []}
        alist = accts_res.get("data") if isinstance(accts_res.get("data"), list) else []
        if alist and isinstance(alist[0], dict):
            self.tradovate_resolved_acct_id = int(alist[0].get("id") or 0) or None
        return {"success": True, "message": msg, "accounts": alist}

    def _tradovate_resolve_account_id(self) -> Optional[int]:
        if self.broker_account_id:
            try:
                return int(self.broker_account_id)
            except Exception:
                pass
        if self.tradovate_resolved_acct_id:
            return self.tradovate_resolved_acct_id
        accts_res = self._tradovate_req("GET", "/account/list", auth_token=self.tradovate_access_token)
        alist = accts_res.get("data") if isinstance(accts_res.get("data"), list) else []
        for a in alist:
            if isinstance(a, dict):
                if self.broker_account_id and str(a.get("name", "")) == self.broker_account_id:
                    self.tradovate_resolved_acct_id = int(a["id"])
                    return self.tradovate_resolved_acct_id
        if alist and isinstance(alist[0], dict) and alist[0].get("id"):
            self.tradovate_resolved_acct_id = int(alist[0]["id"])
            return self.tradovate_resolved_acct_id
        return None

    def dispatch_tradovate_order(self, action: str, canonical_sym: str, direction: str, qty: float) -> Dict[str, Any]:
        ok, msg = self.tradovate_authenticate()
        if not ok:
            return {"success": False, "http_code": 0, "error": msg}
        acct_int = self._tradovate_resolve_account_id()
        if not acct_int:
            return {"success": False, "http_code": 0, "error": "Tradovate accountId could not be resolved"}
        b_sym = self.get_pair_cfg(canonical_sym)["symbol"]
        if action == "CLOSE":
            c_res = self._tradovate_req("GET", f"/contract/find?name={b_sym}", auth_token=self.tradovate_access_token)
            cid = (c_res.get("data") or {}).get("id") if isinstance(c_res.get("data"), dict) else None
            if not cid:
                return {"success": False, "http_code": 0, "error": f"Contract {b_sym} not found on Tradovate"}
            return self._tradovate_req("POST", "/order/liquidateposition", {
                "accountId": acct_int,
                "contractId": int(cid),
                "admin": False,
            }, auth_token=self.tradovate_access_token)
        tv_action = "Buy" if ((direction == "LONG") != (action == "PARTIAL_CLOSE")) else "Sell"
        return self._tradovate_req("POST", "/order/placeorder", {
            "accountSpec": self.broker_account_id or self.tradovate_user,
            "accountId": acct_int,
            "action": tv_action,
            "symbol": b_sym,
            "orderQty": max(1, int(round(float(qty)))),
            "orderType": "Market",
            "isAutomated": True,
        }, auth_token=self.tradovate_access_token)

    # --------------------------------------------------------------------------
    # UNIFIED DISPATCH FACADE FOR NON-DELTA & MULTI-BROKER ACTIONS
    # --------------------------------------------------------------------------
    def dispatch_broker_action(
        self,
        action: str,
        canonical_sym: str,
        direction: str,
        qty: float,
        ref_px: float,
        sl_dist: float,
        tp_dist: float = 0.0,
        trail_offset: float = 0.0,
        reason: str = "",
    ) -> Dict[str, Any]:
        pcfg = self.get_pair_cfg(canonical_sym)
        b_sym = pcfg["symbol"]

        if self.broker_type == "PICKMYTRADE":
            if action == "MODIFY_SL":
                # PickMyTrade manages OCO bracket; Brain runs 1s RAM failsafe trail & fires CLOSE on hit
                return {"success": True, "http_code": 0, "payload": {"note": "RAM trail updated; OCO active on Tradovate"}, "skipped": True}
            payload = self.build_pickmytrade_payload(action, canonical_sym, direction, qty, ref_px, sl_dist, tp_dist, reason=reason)
            if not self.is_live_authenticated:
                HUB.record_dispatch_log(self, action, b_sym, qty, True, 0, f"[PAPER] {json.dumps(payload)[:180]}")
                return {"success": True, "http_code": 0, "payload": payload, "paper": True}
            res = self.post_webhook_json(self.webhook_url, payload)
            HUB.record_dispatch_log(
                self, action, b_sym, qty, bool(res.get("success")),
                int(res.get("http_code", 0)),
                res.get("error") or res.get("response") or json.dumps(payload)[:180],
            )
            if not res.get("success"):
                HUB.send_telegram_async(
                    f"⚠️ <b>PICKMYTRADE ERROR ({b_sym})</b>\nAccount: {self.label}\nAction: {action}\n{res.get('error')}",
                    kind="error",
                )
            return {**res, "payload": payload}

        if self.broker_type == "WEBHOOK":
            payload = self.build_generic_webhook_payload(action, canonical_sym, direction, qty, ref_px, sl_dist, tp_dist, trail_offset, reason=reason)
            if not self.is_live_authenticated:
                HUB.record_dispatch_log(self, action, b_sym, qty, True, 0, f"[PAPER] {json.dumps(payload)[:180]}")
                return {"success": True, "http_code": 0, "payload": payload, "paper": True}
            res = self.post_webhook_json(self.webhook_url, payload, secret=self.webhook_secret)
            HUB.record_dispatch_log(
                self, action, b_sym, qty, bool(res.get("success")),
                int(res.get("http_code", 0)),
                res.get("error") or res.get("response") or json.dumps(payload)[:180],
            )
            if not res.get("success"):
                HUB.send_telegram_async(
                    f"⚠️ <b>WEBHOOK ERROR ({b_sym})</b>\nAccount: {self.label}\nAction: {action}\n{res.get('error')}",
                    kind="error",
                )
            return {**res, "payload": payload}

        if self.broker_type == "MT5_API":
            cmd = self.build_mt5_command(action, canonical_sym, direction, qty, sl_dist, tp_dist, trail_offset, reason=reason)
            if not self.is_live_authenticated:
                HUB.record_dispatch_log(self, action, b_sym, qty, True, 0, f"[PAPER] {json.dumps(cmd)[:180]}")
                return {"success": True, "http_code": 0, "payload": cmd, "paper": True}
            HUB.enqueue_bridge_cmd(self.id, cmd)
            if self.mt5_api_url:
                res = self.post_webhook_json(self.mt5_api_url, cmd, secret=self.bridge_token)
                HUB.record_dispatch_log(
                    self, action, b_sym, qty, bool(res.get("success")),
                    int(res.get("http_code", 0)),
                    res.get("error") or f"MT5 API + Queue ({cmd['cmd_id']})",
                )
                return {**res, "payload": cmd}
            HUB.record_dispatch_log(self, action, b_sym, qty, True, 200, f"Queued for MT5 Bridge ({cmd['cmd_id']})")
            return {"success": True, "http_code": 200, "payload": cmd}

        if self.broker_type == "TRADOVATE_API":
            if action == "MODIFY_SL":
                return {"success": True, "http_code": 0, "skipped": True}
            if not self.is_live_authenticated:
                HUB.record_dispatch_log(self, action, b_sym, qty, True, 0, f"[PAPER] TRADOVATE {action} {direction} {qty} {b_sym}")
                return {"success": True, "http_code": 0, "paper": True}
            res = self.dispatch_tradovate_order(action, canonical_sym, direction, qty)
            HUB.record_dispatch_log(
                self, action, b_sym, qty, bool(res.get("success")),
                int(res.get("http_code", 0)),
                res.get("error") or json.dumps(res.get("data", {}))[:180],
            )
            if not res.get("success"):
                HUB.send_telegram_async(
                    f"⚠️ <b>TRADOVATE API ERROR ({b_sym})</b>\nAccount: {self.label}\nAction: {action}\n{res.get('error')}",
                    kind="error",
                )
            return res

        return {"success": True, "http_code": 0}


# ==============================================================================
# IN-MEMORY 3-ASSET MARKET & MULTI-BROKER ACCOUNT STATE (0 MB DISK TICK BLOAT)
# ==============================================================================
class PairRamBuffer:
    def __init__(self, symbol: str, max_bars: int = 26000):
        self.symbol = symbol
        self.bars_1m: deque = deque(maxlen=max_bars)
        self.l2_depth: Dict[str, Any] = {
            "bids": [],
            "asks": [],
            "ratio": 1.0,
            "ratio_10": 1.0,
            "l2_max": 1.15,
            "imbalance": 0.0,
            "spread": PAIR_SPECS[symbol]["default_spread"],
            "timestamp": 0.0,
        }
        self.cur_l2_min_ts: int = 0
        self.cur_l2_samples: List[Tuple[float, float, float, float]] = []
        self.bar_l2_map: Dict[int, Dict[str, Any]] = {}
        self.last_1m_l2: Optional[Dict[str, Any]] = None
        self.last_price: float = 0.0
        self.cur_min: int = -1
        self.cur_min_ts: int = 0
        self.cur_bar: Optional[Dict[str, Any]] = None
        self.prev_fallback_bar: Optional[Dict[str, Any]] = None
        self.bar_vol_sum: float = 0.0
        self.bar_ticks: int = 0
        self.last_closed_min_ts: int = 0
        self.last_official_vol: float = 1000.0
        self.last_ctx: Optional[Dict[str, Any]] = None
        self.cooldown_until_ts: int = 0
        self.specialist_cd_until: Dict[str, int] = {}

    def finalize_l2_minute(self, min_ts: int, persist_disk: bool = True) -> Optional[Dict[str, Any]]:
        if min_ts <= 0 or not self.cur_l2_samples:
            return self.bar_l2_map.get(min_ts)
        samples = list(self.cur_l2_samples)
        self.cur_l2_samples.clear()
        n_s = len(samples)
        r5_mean = round(sum(x[0] for x in samples) / n_s, 4)
        r10_mean = round(sum(x[1] for x in samples) / n_s, 4)
        l2_max = round(
            max(max(x[0], x[1], 1.0 / max(x[0], 0.1), 1.0 / max(x[1], 0.1)) for x in samples),
            3,
        )
        imb_mean = round(sum(x[2] for x in samples) / n_s, 4)
        sp_mean = round(sum(x[3] for x in samples) / n_s, 4)
        agg = {
            "time": int(min_ts),
            "ratio": r5_mean,
            "ratio_10": r10_mean,
            "l2_max": l2_max,
            "imbalance": imb_mean,
            "spread": sp_mean,
            "samples": n_s,
            "from_live_1s": True,
        }
        self.bar_l2_map[int(min_ts)] = agg
        self.last_1m_l2 = agg
        cutoff = int(min_ts) - 7200
        if len(self.bar_l2_map) > 150:
            for k in [k for k in self.bar_l2_map.keys() if k < cutoff]:
                self.bar_l2_map.pop(k, None)
        if persist_disk and n_s >= 3:
            try:
                if os.path.exists(L2_1M_HISTORY_PATH) and os.path.getsize(L2_1M_HISTORY_PATH) > 12 * 1024 * 1024:
                    os.replace(L2_1M_HISTORY_PATH, L2_1M_HISTORY_PATH + ".prev")
                row_json = json.dumps({"sym": self.symbol, **agg})
                with open(L2_1M_HISTORY_PATH, "a", encoding="utf-8") as f:
                    f.write(row_json + "\n")
            except Exception:
                pass
        return agg

    def update_l2(self, bids: List[List[float]], asks: List[List[float]], now_ts: Optional[float] = None):
        ts_val = float(now_ts if now_ts is not None else time.time())
        bid5 = sum(b[1] for b in bids[:5]) if bids else 1.0
        ask5 = sum(a[1] for a in asks[:5]) if asks else 1.0
        bid10 = sum(b[1] for b in bids[:10]) if bids else 1.0
        ask10 = sum(a[1] for a in asks[:10]) if asks else 1.0
        r5 = round(bid5 / ask5, 3) if ask5 > 0 else 1.0
        r10 = round(bid10 / ask10, 3) if ask10 > 0 else 1.0
        imb = round((bid10 - ask10) / max(bid10 + ask10, 1e-6), 4)
        spread = round(max(0.01, asks[0][0] - bids[0][0]), 2) if (bids and asks) else PAIR_SPECS[self.symbol]["default_spread"]
        self.l2_depth = {
            "bids": bids[:10],
            "asks": asks[:10],
            "ratio": round(r5, 2),
            "ratio_10": round(r10, 2),
            "l2_max": round(max(r5, r10, 1.0 / max(r5, 0.1), 1.0 / max(r10, 0.1)), 2),
            "imbalance": round(imb, 3),
            "spread": spread,
            "timestamp": ts_val,
        }
        min_ts = (int(ts_val) // 60) * 60
        if self.cur_l2_min_ts > 0 and min_ts > self.cur_l2_min_ts and self.cur_l2_samples:
            self.finalize_l2_minute(self.cur_l2_min_ts)
        self.cur_l2_min_ts = min_ts
        # Filter extreme empty-book spread glitches from polluting the 1-minute entry L2 mean
        if spread <= PAIR_SPECS[self.symbol]["max_spread"] * 2.0:
            self.cur_l2_samples.append((r5, r10, imb, spread))

    def append_bar(self, bar: Dict[str, Any]):
        b_ts = int(bar.get("time", 0))
        if self.bars_1m and b_ts > 0:
            prev_ts = int(self.bars_1m[-1].get("time", 0))
            if b_ts == prev_ts:
                self.bars_1m[-1] = bar
                self.last_price = float(bar["close"])
                return
            if b_ts < prev_ts:
                return
        self.bars_1m.append(bar)
        self.last_price = float(bar["close"])


class UnifiedFloorHub:
    def __init__(self):
        self.lock = threading.RLock()
        self.engine_paused: bool = False
        self.pair_states: Dict[str, str] = {s: "RUNNING" for s in PAIRS}
        self.pair_allow_1m: Dict[str, bool] = {s: True for s in PAIRS}
        self.markets: Dict[str, PairRamBuffer] = {s: PairRamBuffer(s) for s in PAIRS}
        self.accounts: List[MultiBrokerAccountClient] = []
        self.open_positions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.closed_trades: deque = deque(maxlen=1000)
        self.dispatch_logs: deque = deque(maxlen=250)
        self.bridge_queues: Dict[str, deque] = {}
        self.admin_password: str = ENV_CFG["ADMIN_PASSWORD"]
        self.inbound_webhook_token: str = ENV_CFG["INBOUND_WEBHOOK_TOKEN"] or secrets.token_urlsafe(24)
        self.active_sessions: Dict[str, float] = {}
        self.telegram: Dict[str, Any] = {
            "bot_token": ENV_CFG["TELEGRAM_BOT_TOKEN"],
            "chat_id": ENV_CFG["TELEGRAM_CHAT_ID"],
            "daily_summary_ist": ENV_CFG["TELEGRAM_DAILY_IST"] or "23:55",
            "alert_open": True,
            "alert_partial": True,
            "alert_close": True,
            "alert_daily": True,
            "alert_error": True,
        }
        self.last_daily_summary_date: str = ""
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.load_runtime_config()
        self.load_journal()

    def record_dispatch_log(self, acct: MultiBrokerAccountClient, action: str, sym: str, qty: float, ok: bool, code: int, msg: str):
        with self.lock:
            self.dispatch_logs.appendleft({
                "id": f"dl_{int(time.time()*1000)}_{len(self.dispatch_logs)}",
                "time_ist": fmt_ist_datetime(),
                "account_id": acct.id,
                "account_label": acct.label,
                "broker_type": acct.broker_type,
                "action": action,
                "symbol": sym,
                "qty": fmt_qty_json(qty),
                "success": ok,
                "http_code": code,
                "summary": str(msg)[:240],
            })

    def enqueue_bridge_cmd(self, acct_id: str, cmd: Dict[str, Any]):
        with self.lock:
            q = self.bridge_queues.setdefault(acct_id, deque(maxlen=100))
            q.append(cmd)

    def pop_bridge_cmds(self, acct_id: str) -> List[Dict[str, Any]]:
        with self.lock:
            q = self.bridge_queues.get(acct_id)
            if not q:
                return []
            items = list(q)
            q.clear()
            return items

    def load_runtime_config(self):
        if os.path.exists(MULTIBROKER_CONFIG_PATH):
            try:
                with open(MULTIBROKER_CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("admin_password"):
                    self.admin_password = str(data["admin_password"])
                if data.get("inbound_webhook_token"):
                    self.inbound_webhook_token = str(data["inbound_webhook_token"])
                if isinstance(data.get("pair_states"), dict):
                    for s in PAIRS:
                        if s in data["pair_states"]:
                            self.pair_states[s] = str(data["pair_states"][s])
                if isinstance(data.get("pair_allow_1m"), dict):
                    for s in PAIRS:
                        if s in data["pair_allow_1m"]:
                            self.pair_allow_1m[s] = bool(data["pair_allow_1m"][s])
                if isinstance(data.get("specialist_cd_until"), dict):
                    now_epoch = int(time.time())
                    for s in PAIRS:
                        raw_cds = data["specialist_cd_until"].get(s)
                        if isinstance(raw_cds, dict):
                            self.markets[s].specialist_cd_until = {
                                str(k): int(v) for k, v in raw_cds.items() if int(v or 0) > now_epoch
                            }
                self.engine_paused = bool(data.get("engine_paused", False))
                if isinstance(data.get("telegram"), dict):
                    self.telegram.update(data["telegram"])
                accts = data.get("accounts", [])
                if isinstance(accts, list) and accts:
                    self.accounts = [MultiBrokerAccountClient(a, ENV_CFG["DELTA_BASE_URL"]) for a in accts]
            except Exception as e:
                print(f"[CONFIG LOAD NOTICE] {e}")

        if not any(a.broker_type == "DELTA" for a in self.accounts):
            default_mode = "LIVE" if (ENV_CFG["DELTA_API_KEY"] and ENV_CFG["DELTA_API_SECRET"]) else "PAPER"
            delta_default = MultiBrokerAccountClient({
                "id": "acct_default",
                "label": "Primary Delta Account",
                "broker_type": "DELTA",
                "sizing_mode": "RISK_USD",
                "entry_order_mode": "MAKER_CHASER",
                "delta_account_id": ENV_CFG["DELTA_ACCOUNT_ID"] or "PRIMARY-01",
                "api_key": ENV_CFG["DELTA_API_KEY"],
                "api_secret": ENV_CFG["DELTA_API_SECRET"],
                "mode": default_mode,
                "leverage": ENV_CFG["LEVERAGE"],
                "risk_usd": ENV_CFG["FIXED_RISK_USD"],
                "enabled_pairs": list(PAIRS),
                "liq_guard": True,
                "is_default": True,
                "wallet_balance_usd": 100.0,
                "free_margin_usd": 100.0,
            }, ENV_CFG["DELTA_BASE_URL"])
            self.accounts.insert(0, delta_default)
            self.enforce_delta_default()
            self.save_runtime_config()
        else:
            self.enforce_delta_default()

    def enforce_delta_default(self):
        """Ensures ONLY a DELTA account is ever marked is_default for public dashboard calculations."""
        found_default = False
        for a in self.accounts:
            if a.broker_type != "DELTA":
                a.is_default = False
            elif a.is_default:
                if not found_default:
                    found_default = True
                else:
                    a.is_default = False
        if not found_default:
            for a in self.accounts:
                if a.broker_type == "DELTA":
                    a.is_default = True
                    break

    def save_runtime_config(self):
        with self.lock:
            self.enforce_delta_default()
            now_epoch = int(time.time())
            spec_cds = {
                s: {k: int(v) for k, v in self.markets[s].specialist_cd_until.items() if int(v) > now_epoch}
                for s in PAIRS
            }
            payload = {
                "admin_password": self.admin_password,
                "inbound_webhook_token": self.inbound_webhook_token,
                "engine_paused": self.engine_paused,
                "pair_states": self.pair_states,
                "pair_allow_1m": self.pair_allow_1m,
                "specialist_cd_until": spec_cds,
                "telegram": self.telegram,
                "accounts": [a.to_storage_dict() for a in self.accounts],
            }
            try:
                with open(MULTIBROKER_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
            except Exception as e:
                print(f"[CONFIG SAVE ERROR] {e}")

    def load_journal(self):
        if os.path.exists(MULTIBROKER_JOURNAL_PATH):
            try:
                with open(MULTIBROKER_JOURNAL_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for idx, t in enumerate(data[-1000:], 1):
                        if isinstance(t, dict):
                            t["id"] = idx
                            self.closed_trades.append(t)
            except Exception as e:
                print(f"[JOURNAL LOAD NOTICE] {e}")

    def save_trade(self, trade: Dict[str, Any]):
        with self.lock:
            trade["id"] = len(self.closed_trades) + 1
            trade["journal_v"] = JOURNAL_VERSION
            self.closed_trades.append(trade)
            try:
                with open(MULTIBROKER_JOURNAL_PATH, "w", encoding="utf-8") as f:
                    json.dump(list(self.closed_trades), f, indent=2)
            except Exception as e:
                print(f"[JOURNAL SAVE ERROR] {e}")

    def get_default_account(self) -> Optional[MultiBrokerAccountClient]:
        """Returns ONLY the Default DELTA account (never returns a non-Delta account)."""
        for a in self.accounts:
            if a.broker_type == "DELTA" and a.is_default:
                return a
        for a in self.accounts:
            if a.broker_type == "DELTA":
                return a
        return None

    def get_tickers_snapshot(self, public_only: bool = False, account_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        def_acct = self.get_default_account()
        for s in PAIRS:
            m = self.markets[s]
            if public_only:
                in_trade = bool(def_acct and (def_acct.id, s) in self.open_positions)
            elif account_id and account_id != "ALL_ACCOUNTS":
                tid = (def_acct.id if (account_id == "DEFAULT" and def_acct) else account_id)
                in_trade = (tid, s) in self.open_positions
            else:
                in_trade = any(k[1] == s for k in self.open_positions)
            if self.engine_paused or self.pair_states.get(s) == "PAUSED":
                st = "PAUSED"
            elif in_trade:
                st = "IN TRADE"
            else:
                st = "SCANNING"
            l2_1s = round(float(m.l2_depth.get("ratio", 1.0)), 2)
            l2_1m = round(float(m.last_1m_l2["ratio"]), 2) if m.last_1m_l2 else l2_1s
            out[s] = {
                "price": round(m.last_price, 2),
                "spread": round(float(m.l2_depth.get("spread", PAIR_SPECS[s]["default_spread"])), 2),
                "ratio": l2_1s,
                "ratio_1m": l2_1m,
                "allow_1m": bool(self.pair_allow_1m.get(s, True)),
                "state": st,
            }
        return out

    def send_telegram(self, text: str, kind: str = "info") -> Tuple[bool, str]:
        tg = self.telegram
        token = str(tg.get("bot_token", "")).strip()
        if token.lower().startswith("bot") and ":" in token:
            token = token[3:].strip()
        chat_id = str(tg.get("chat_id", "")).strip()
        if not token or not chat_id:
            return False, "Telegram Bot Token or Chat ID not configured (enter both and click Save or Send Test Alert)"
        if kind == "open" and not tg.get("alert_open", True):
            return False, "Open alerts disabled"
        if kind == "partial" and not tg.get("alert_partial", True):
            return False, "Partial/Trail alerts disabled"
        if kind == "close" and not tg.get("alert_close", True):
            return False, "Close alerts disabled"
        if kind == "daily" and not tg.get("alert_daily", True):
            return False, "Daily summary alerts disabled"
        if kind == "error" and not tg.get("alert_error", True):
            return False, "Error alerts disabled"

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                res = json.loads(r.read().decode("utf-8"))
                if res.get("ok"):
                    return True, "Sent"
                return False, str(res.get("description", "Telegram error"))
        except urllib.error.HTTPError as e:
            err_raw = e.read().decode("utf-8", errors="replace")
            desc = err_raw
            try:
                desc = str(json.loads(err_raw).get("description") or err_raw)
            except Exception:
                pass
            if "chat not found" in desc.lower():
                return False, "Telegram: Chat not found — open your Bot in Telegram and press Start (/start) first!"
            if e.code == 401 or "unauthorized" in desc.lower():
                return False, "Telegram: Invalid Bot Token — copy the exact token from @BotFather."
            return False, f"Telegram HTTP {e.code}: {desc}"
        except Exception as e:
            return False, str(e)

    def send_telegram_async(self, text: str, kind: str = "info"):
        self.executor.submit(self.send_telegram, text, kind)

    def check_ist_daily_summary(self):
        tg = self.telegram
        if not tg.get("bot_token") or not tg.get("chat_id") or not tg.get("alert_daily", True):
            return
        dt_ist = now_ist()
        cur_hm = dt_ist.strftime("%H:%M")
        cur_date = dt_ist.strftime("%Y-%m-%d")
        target_hm = str(tg.get("daily_summary_ist", "23:55")).strip()
        if cur_hm == target_hm and self.last_daily_summary_date != cur_date:
            self.last_daily_summary_date = cur_date
            def_acct = self.get_default_account()
            if not def_acct:
                return
            def_id = def_acct.id
            day_trades = [t for t in self.closed_trades if t.get("date_ist") == cur_date and t.get("account_id") == def_id]
            wins = sum(1 for t in day_trades if float(t.get("net_pnl_usd", 0)) > 0)
            gross = sum(float(t.get("gross_pnl_usd", 0)) for t in day_trades)
            fees = sum(float(t.get("fee_usd", 0)) for t in day_trades)
            net = sum(float(t.get("net_pnl_usd", 0)) for t in day_trades)
            msg = (
                f"📊 <b>MULTI-BROKER JOURNAL · IST DAILY SUMMARY ({cur_date})</b>\n"
                f"Account: <b>{def_acct.label}</b>\n"
                f"Trades Closed: <b>{len(day_trades)}</b> ({wins}W / {len(day_trades)-wins}L)\n"
                f"Gross P&amp;L: <b>${gross:+.2f}</b>\n"
                f"Brokerage Fees: <b>-${fees:.2f}</b>\n"
                f"Net Realized P&amp;L: <b>${net:+.2f}</b>"
            )
            self.send_telegram_async(msg, kind="daily")


HUB = UnifiedFloorHub()


# ==============================================================================
# CAUSAL 1-MINUTE MULTI-ASSET FEATURE & INDICATOR ENGINE (IN-MEMORY)
# ==============================================================================
def _ema_series(series: List[float], period: int) -> List[float]:
    if not series:
        return [0.0]
    k = 2.0 / (period + 1.0)
    out = [series[0]]
    for val in series[1:]:
        out.append(val * k + out[-1] * (1.0 - k))
    return out


def _sample_std(vals: List[float], mean_val: Optional[float] = None) -> float:
    n = len(vals)
    if n < 2:
        return 0.0
    mu = (sum(vals) / n) if mean_val is None else mean_val
    var = sum((x - mu) * (x - mu) for x in vals) / (n - 1)
    return math.sqrt(var) if var > 0.0 else 0.0


def compute_symbol_context(symbol: str, bar_ts: Optional[int] = None) -> Dict[str, Any]:
    """
    Computes all causal 1m/5m/15m/1h/4h indicators, 16-agent features, 1m Mean-L2 depth ratios,
    and rolling 1H-6H chart S/R walls purely in RAM for XAUTUSD, BTCUSD, or ETHUSD
    with exact parity to the vectorized NumPy/Pandas backtests (<15ms on 25,000 bars).
    """
    m = HUB.markets[symbol]
    bars_full = list(m.bars_1m)
    if len(bars_full) < 30:
        return {}

    n_full = len(bars_full)
    closes_full = [float(b["close"]) for b in bars_full]
    emad = _ema_series(closes_full, 1440)[-1]
    emam = _ema_series(closes_full, 7200)[-1]

    closes_4k = closes_full[-min(4000, n_full):]
    ema9 = _ema_series(closes_4k, 9)[-1]
    ema15 = _ema_series(closes_4k, 15)[-1]
    ema50 = _ema_series(closes_4k, 60)[-1]
    ema60 = ema50
    ema200 = _ema_series(closes_4k, 240)[-1]
    ema240 = ema200

    ema12_5m = _ema_series(closes_4k, 60)
    ema26_5m = _ema_series(closes_4k, 130)
    macd_l = [a - b for a, b in zip(ema12_5m, ema26_5m)]
    macd_s = _ema_series(macd_l, 45)
    macd_h = [a - b for a, b in zip(macd_l, macd_s)]
    macd_up = (macd_h[-1] > macd_h[-4]) if len(macd_h) >= 4 else True
    macd_dn = (macd_h[-1] < macd_h[-4]) if len(macd_h) >= 4 else False

    # All non-EMA features look back <= 1500 bars (VWAP=1440, TR15=460, S/R=360)
    bars = bars_full[-min(1600, n_full):]
    n_bars = len(bars)
    opens = [float(b["open"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = [float(b["close"]) for b in bars]
    raw_vols = [float(b.get("volume", 1.0)) for b in bars]

    o, h, l, c = opens[-1], highs[-1], lows[-1], closes[-1]

    d_vols = [max(rv / 1000.0, 1.0) for rv in raw_vols]
    frozen_cap = float(FROZEN_MODELS.get(symbol, {}).get("vol_cap", 0.0) or 0.0)
    if frozen_cap > 1.0:
        vol_cap = frozen_cap
    else:
        all_d_vols = [max(float(b.get("volume", 1.0)) / 1000.0, 1.0) for b in bars_full]
        sorted_v = sorted(all_d_vols)
        p985 = sorted_v[min(len(sorted_v) - 1, int(0.985 * (len(sorted_v) - 1)))]
        med_v = sorted_v[len(sorted_v) // 2]
        vol_cap = max(p985, med_v * 12.0, 1.0)
    norm_vols = [min(max(x, 1.0), vol_cap) for x in d_vols]
    v = norm_vols[-1]

    eps_body = 0.50 if symbol == "BTCUSD" else 0.05
    body = max(abs(c - o), eps_body)
    lw = max(0.0, min(o, c) - l) / body
    uw = max(0.0, h - max(o, c)) / body
    rng_bar = max(h - l, eps_body)
    lw_rng = max(0.0, min(o, c) - l) / rng_bar
    uw_rng = max(0.0, h - max(o, c)) / rng_bar
    body_ratio = abs(c - o) / max(h - l, 0.05)
    bull_disp = (c > o) and (body_ratio >= 0.55)
    bear_disp = (c < o) and (body_ratio >= 0.55)

    prev_h = highs[-2] if n_bars >= 2 else h
    prev_l = lows[-2] if n_bars >= 2 else l
    b_mss = (c > o) and (c > prev_h)
    s_mss = (c < o) and (c < prev_l)
    sh5_prev = max(highs[-min(6, n_bars):-1]) if n_bars >= 2 else h
    sl5_prev = min(lows[-min(6, n_bars):-1]) if n_bars >= 2 else l
    b_mss_5b = (c > o) and (c > sh5_prev)
    s_mss_5b = (c < o) and (c < sl5_prev)

    min_clv_rng = 1e-4 if symbol in ("BTCUSD", "ETHUSD") else 1e-6
    bar_deltas: List[float] = []
    cvd_vals: List[float] = []
    cvd_acc = 0.0
    for bo, bh, bl, bc, v_n in zip(opens, highs, lows, closes, norm_vols):
        brng = max(bh - bl, min_clv_rng)
        clv_body = (bc - bo) / brng
        clv_wick = ((min(bo, bc) - bl) - (bh - max(bo, bc))) / brng
        eff_bias = max(-1.0, min(1.0, 0.7 * clv_body + 0.3 * clv_wick))
        bd = eff_bias * v_n * 0.45
        bar_deltas.append(bd)
        cvd_acc += bd
        cvd_vals.append(cvd_acc)

    d5 = sum(bar_deltas[-5:])
    d15 = sum(bar_deltas[-15:])
    bd_win = bar_deltas[-min(30, n_bars):]
    bd_mean = sum(bd_win) / len(bd_win)
    bd_std = _sample_std(bd_win, bd_mean) + 1e-6
    delta_z = (bar_deltas[-1] - bd_mean) / bd_std

    # 30-bar vol_z (F["vol_z"]) AND 60-bar vz (A["vz"] = precompute_features vol_z_score)
    vol_win30 = norm_vols[-min(30, n_bars):]
    mean_v30 = sum(vol_win30) / len(vol_win30)
    std_v30 = _sample_std(vol_win30, mean_v30) + 1e-6
    vol_z_30 = (v - mean_v30) / std_v30

    vol_win60 = norm_vols[-min(60, n_bars):]
    mean_v = sum(vol_win60) / len(vol_win60)
    std_v_raw = _sample_std(vol_win60, mean_v)
    std_v = 1.0 if std_v_raw == 0.0 else std_v_raw
    vz = (v - mean_v) / std_v

    h_src = highs[:-1] if n_bars >= 5 else highs
    l_src = lows[:-1] if n_bars >= 5 else lows

    sh15 = max(h_src[-min(15, len(h_src)):])
    sl15 = min(l_src[-min(15, len(l_src)):])
    sh20 = max(h_src[-min(20, len(h_src)):])
    sl20 = min(l_src[-min(20, len(l_src)):])
    rh60 = max(h_src[-min(60, len(h_src)):])
    rl60 = min(l_src[-min(60, len(h_src)):])
    min_box60 = 10.0 if symbol == "BTCUSD" else 1.0
    rng_60 = max(rh60 - rl60, 0.10)
    rng_60_f = max(rh60 - rl60, min_box60)
    poc60_slice = sorted(closes[-min(61, n_bars):-1] if n_bars >= 5 else closes)
    poc60 = poc60_slice[len(poc60_slice) // 2]
    rpos60 = max(0.0, min(1.0, (c - rl60) / rng_60_f))
    fib60_l = (rh60 - l) / rng_60
    fib60_s = (h - rl60) / rng_60
    room_h60 = max(0.10, rh60 - c)
    room_l60 = max(0.10, c - rl60)

    rh120 = max(h_src[-min(120, len(h_src)):])
    rl120 = min(l_src[-min(120, len(l_src)):])
    rh240 = max(h_src[-min(240, len(h_src)):])
    rl240 = min(l_src[-min(240, len(l_src)):])
    rng_240 = max(rh240 - rl240, 0.10)
    rng_240_f = max(rh240 - rl240, min_box60)
    bpos240 = max(0.0, min(1.0, (c - rl240) / rng_240_f))
    fib240_l = (rh240 - l) / rng_240
    fib240_s = (h - rl240) / rng_240
    room_h240 = max(0.10, rh240 - c)
    room_l240 = max(0.10, c - rl240)

    rh360 = max(h_src[-min(360, len(h_src)):])
    rl360 = min(l_src[-min(360, len(l_src)):])
    swing_10_low = min(lows[-min(10, n_bars):])
    swing_10_high = max(highs[-min(10, n_bars):])
    swing_12_low = min(lows[-min(12, n_bars):])
    swing_12_high = max(highs[-min(12, n_bars):])
    swing_20_low = min(lows[-min(20, n_bars):])
    swing_20_high = max(highs[-min(20, n_bars):])

    # Tail-only ATR/TR15/CHOP/ADX computation (exact match to 210+210 window, <2ms on 8,500 bars)
    tail_tr_start = max(0, n_bars - 300)
    tr1_tail = []
    for i in range(tail_tr_start, n_bars):
        if i == 0:
            tr1_tail.append(highs[0] - lows[0])
        else:
            tr1_tail.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr_1m = sum(tr1_tail[-min(14, len(tr1_tail)):]) / min(14, len(tr1_tail))
    atr_15m_f = (sum(tr1_tail[-min(60, len(tr1_tail)):]) / min(60, len(tr1_tail))) * 2.2

    tail15_start = max(0, n_bars - 460)
    tr15_tail: List[float] = []
    pdm_tail: List[float] = []
    mdm_tail: List[float] = []
    for i in range(tail15_start, n_bars):
        i0 = max(0, i - 14)
        h15_i = max(highs[i0:i + 1])
        l15_i = min(lows[i0:i + 1])
        c15_p = closes[max(0, i - 15)]
        tr15_tail.append(max(h15_i - l15_i, abs(h15_i - c15_p), abs(l15_i - c15_p)))
        up_m = highs[i] - highs[max(0, i - 15)]
        dn_m = -(lows[i] - lows[max(0, i - 15)])
        pdm_tail.append(up_m if (up_m > dn_m and up_m > 0) else 0.0)
        mdm_tail.append(dn_m if (dn_m > up_m and dn_m > 0) else 0.0)

    n_t15 = len(tr15_tail)
    w_210 = min(210, n_t15)
    raw_atr_15m = sum(tr15_tail[-w_210:]) / w_210
    atr_5m = raw_atr_15m

    # Rolling 240-bar mean of 60-bar range (exact BTC F["atr_60m"])
    rng60_start = max(0, n_bars - 240)
    rng60_list = [max(highs[max(0, i - 59):i + 1]) - min(lows[max(0, i - 59):i + 1]) for i in range(rng60_start, n_bars)]
    atr_60m = (sum(rng60_list) / len(rng60_list)) if rng60_list else (raw_atr_15m * 1.85)

    tr15_sum = sum(tr15_tail[-1 - 15 * k] for k in range(14) if (n_t15 - 1 - 15 * k) >= 0)
    hl_range_210 = max(max(highs[-min(210, n_bars):]) - min(lows[-min(210, n_bars):]), 0.10)
    chop = 100.0 * math.log10(max(tr15_sum / hl_range_210, 1.0)) / math.log10(14.0) if n_bars >= 210 else 48.0

    # Exact ADX(14) on 15M = 210-bar rolling mean of DX (matches precompute_features L116-120)
    dx_vals: List[float] = []
    if n_t15 >= 30:
        for k_end in range(max(30, n_t15 - 209), n_t15 + 1):
            k_beg = max(0, k_end - 210)
            w_k = k_end - k_beg
            atr_k = sum(tr15_tail[k_beg:k_end]) / w_k
            atr_k = 0.10 if atr_k == 0.0 else atr_k
            pdi_k = 100.0 * (sum(pdm_tail[k_beg:k_end]) / w_k) / atr_k
            mdi_k = 100.0 * (sum(mdm_tail[k_beg:k_end]) / w_k) / atr_k
            denom_k = pdi_k + mdi_k
            denom_k = 1.0 if denom_k == 0.0 else denom_k
            dx_vals.append(100.0 * abs(pdi_k - mdi_k) / denom_k)
    adx = (sum(dx_vals) / len(dx_vals)) if dx_vals else 20.0

    p_60 = sum(abs(closes[i] - closes[i - 1]) for i in range(max(1, n_bars - 60), n_bars))
    ker_1h = abs(c - closes[-min(61, n_bars)]) / (p_60 + 1e-6) if n_bars >= 10 else 0.35
    p_15 = sum(abs(closes[i] - closes[i - 1]) for i in range(max(1, n_bars - 15), n_bars))
    ker_15m = abs(c - closes[-min(16, n_bars)]) / (p_15 + 1e-6) if n_bars >= 8 else 0.35
    p_10 = sum(abs(closes[i] - closes[i - 1]) for i in range(max(1, n_bars - 10), n_bars))
    ker_1m = abs(c - closes[-min(11, n_bars)]) / (p_10 + 1e-6) if n_bars >= 5 else 0.35
    min_atr_ter = 5.0 if symbol == "BTCUSD" else 0.25
    ter_ratio = max(rh60 - rl60, min_box60) / max(atr_1m, min_atr_ter)

    g5, l5 = [], []
    for i in range(max(5, n_bars - 70), n_bars):
        df5 = closes[i] - closes[i - 5]
        g5.append(max(df5, 0.0))
        l5.append(max(-df5, 0.0))
    rsi5 = (100.0 - 100.0 / (1.0 + (sum(g5) / len(g5)) / max(sum(l5) / len(l5), 0.01))) if g5 else 50.0

    g14, l14 = [], []
    for i in range(max(1, n_bars - 14), n_bars):
        df1 = closes[i] - closes[i - 1]
        g14.append(max(df1, 0.0))
        l14.append(max(-df1, 0.0))
    rsi14 = (100.0 - 100.0 / (1.0 + (sum(g14) / len(g14)) / max(sum(l14) / len(l14), 1e-6))) if g14 else 50.0

    roc_5m = c - closes[-min(6, n_bars)]
    roc_15m = c - closes[-min(16, n_bars)]
    cvd_min_15 = min(cvd_vals[-min(16, n_bars):-1]) if n_bars >= 2 else cvd_vals[-1]
    cvd_max_15 = max(cvd_vals[-min(16, n_bars):-1]) if n_bars >= 2 else cvd_vals[-1]
    cvd_tol15 = 0.30 if symbol == "XAUTUSD" else (0.35 * atr_1m)
    cvd_div_bull = (l <= sl15 + cvd_tol15) and (cvd_vals[-1] > cvd_min_15)
    cvd_div_bear = (h >= sh15 - cvd_tol15) and (cvd_vals[-1] < cvd_max_15)

    cvd_min_20 = min(cvd_vals[-min(21, n_bars):-1]) if n_bars >= 2 else cvd_vals[-1]
    cvd_max_20 = max(cvd_vals[-min(21, n_bars):-1]) if n_bars >= 2 else cvd_vals[-1]
    cvd_tol20 = 0.35 * atr_1m
    cvd_div_20_bull = (l <= sl20 + cvd_tol20) and (cvd_vals[-1] > cvd_min_20)
    cvd_div_20_bear = (h >= sh20 - cvd_tol20) and (cvd_vals[-1] < cvd_max_20)

    sh_8 = max(h_src[-min(8, len(h_src)):])
    sl_8 = min(l_src[-min(8, len(l_src)):])
    bos_bull = (c > sh_8) and (c > o)
    bos_bear = (c < sl_8) and (c < o)
    gate_fvg_tol = 0.60 if symbol == "XAUTUSD" else (0.50 * atr_1m)
    fvg_bull_gate = (n_bars >= 4) and (l <= highs[-4] + gate_fvg_tol) and (c > o) and (c >= ema15)
    fvg_bear_gate = (n_bars >= 4) and (h >= lows[-4] - gate_fvg_tol) and (c < o) and (c <= ema15)

    win_fvg_tol = 10.0 if symbol == "BTCUSD" else 0.30
    bull_fvg_win = False
    bear_fvg_win = False
    stop_b_win = False
    stop_s_win = False
    u_grab_win = False
    l_grab_win = False
    b_abs_win = False
    s_abs_win = False

    for j in range(max(2, n_bars - 10), n_bars):
        bo_j, bh_j, bl_j, bc_j = opens[j], highs[j], lows[j], closes[j]
        body_j = max(abs(bc_j - bo_j), eps_body)
        lw_j = max(0.0, min(bo_j, bc_j) - bl_j) / body_j
        uw_j = max(0.0, bh_j - max(bo_j, bc_j)) / body_j
        vw_j = norm_vols[max(0, j - 59):j + 1]
        mw_j = sum(vw_j) / len(vw_j)
        sw_j = _sample_std(vw_j, mw_j)
        vz_j = (norm_vols[j] - mw_j) / (1.0 if sw_j == 0.0 else sw_j)
        d5_j = sum(bar_deltas[max(0, j - 4):j + 1])
        if (bl_j - highs[j - 2]) >= win_fvg_tol:
            bull_fvg_win = True
        if (lows[j - 2] - bh_j) >= win_fvg_tol:
            bear_fvg_win = True
        if vz_j >= 1.3 and lw_j >= 1.3:
            stop_b_win = True
        if vz_j >= 1.3 and uw_j >= 1.3:
            stop_s_win = True
        rh60_j = max(highs[max(0, j - 60):j]) if j > 0 else highs[0]
        rl60_j = min(lows[max(0, j - 60):j]) if j > 0 else lows[0]
        rh60_j1 = max(highs[max(0, j - 61):max(1, j - 1)]) if j > 1 else highs[0]
        rl60_j1 = min(lows[max(0, j - 61):max(1, j - 1)]) if j > 1 else lows[0]
        if (bh_j >= rh60_j) or (highs[j - 1] >= rh60_j1):
            u_grab_win = True
        if (bl_j <= rl60_j) or (lows[j - 1] <= rl60_j1):
            l_grab_win = True
        if j >= n_bars - 8:
            if d5_j < 0 and (bc_j > bo_j or lw_j >= 1.5):
                b_abs_win = True
            if d5_j > 0 and (bc_j < bo_j or uw_j >= 1.5):
                s_abs_win = True

    ts_val = int(bar_ts if bar_ts is not None else bars[-1].get("time", time.time()))
    brng_last = max(h - l, min_clv_rng)
    eff_bias_last = max(
        -1.0,
        min(1.0, 0.7 * ((c - o) / brng_last) + 0.3 * (((min(o, c) - l) - (h - max(o, c))) / brng_last)),
    )
    ratio_native_last = max(0.45, min(2.2, 1.0 + 0.65 * eff_bias_last))

    # 1m Mean-L2 Entry Resolution (eliminates single-second :02s L2 flicker while keeping 100% replay parity)
    b_l2 = m.bar_l2_map.get(ts_val)
    if b_l2 is None and m.cur_l2_samples and m.l2_depth.get("bids"):
        n_s = len(m.cur_l2_samples)
        r5_m = sum(x[0] for x in m.cur_l2_samples) / n_s
        r10_m = sum(x[1] for x in m.cur_l2_samples) / n_s
        b_l2 = {
            "ratio": r5_m,
            "ratio_10": r10_m,
            "l2_max": max(max(x[0], x[1], 1.0 / max(x[0], 0.1), 1.0 / max(x[1], 0.1)) for x in m.cur_l2_samples),
            "imbalance": sum(x[2] for x in m.cur_l2_samples) / n_s,
            "spread": sum(x[3] for x in m.cur_l2_samples) / n_s,
            "from_live_1s": True,
        }

    if b_l2 is not None:
        raw_r5 = float(b_l2.get("ratio", 1.0))
        if symbol == "XAUTUSD" and b_l2.get("from_live_1s"):
            # Matches _extend_gold_df 50% MBP-10 + 50% native CLV ratio calibration
            l2_ratio = round(0.5 * raw_r5 + 0.5 * ratio_native_last, 4)
            l2_max = round(max(1.1, abs(l2_ratio) * 1.15), 3)
        else:
            l2_ratio = round(raw_r5, 4)
            l2_max = round(float(b_l2.get("l2_max", max(1.1, abs(l2_ratio) * 1.15))), 3)
        db_imb = round(float(b_l2.get("imbalance", 0.0)), 4)
        spread = round(float(b_l2.get("spread", PAIR_SPECS[symbol]["default_spread"])), 3)
        l2_ts_val = float(ts_val)
    elif m.l2_depth.get("bids"):
        raw_r5 = float(m.l2_depth.get("ratio", 1.0))
        l2_ratio = round(0.5 * raw_r5 + 0.5 * ratio_native_last, 4) if symbol == "XAUTUSD" else raw_r5
        l2_max = round(max(1.1, abs(l2_ratio) * 1.15), 3) if symbol == "XAUTUSD" else float(m.l2_depth.get("l2_max", max(1.1, abs(l2_ratio) * 1.15)))
        db_imb = float(m.l2_depth.get("imbalance", 0.0))
        spread = float(m.l2_depth.get("spread", PAIR_SPECS[symbol]["default_spread"]))
        l2_ts_val = float(m.l2_depth.get("timestamp", 0.0))
    else:
        l2_ratio = float(m.l2_depth.get("ratio", 1.0))
        l2_max = float(m.l2_depth.get("l2_max", max(1.1, abs(l2_ratio) * 1.15)))
        db_imb = float(m.l2_depth.get("imbalance", 0.0))
        spread = float(m.l2_depth.get("spread", PAIR_SPECS[symbol]["default_spread"]))
        l2_ts_val = float(m.l2_depth.get("timestamp", 0.0))
    eq_tol = 40.0 if symbol == "BTCUSD" else 1.20
    htf_tol = 85.0 if symbol == "BTCUSD" else 2.50
    near_eqh_60 = sum(1.0 for x in highs[-min(61, n_bars):-1] if abs(rh60 - x) <= eq_tol)
    near_eql_60 = sum(1.0 for x in lows[-min(61, n_bars):-1] if abs(x - rl60) <= eq_tol)
    vol_cluster = min(4.0, max(0.0, vz))
    l2_ask_wall = min(2.0, max(0.5, 2.0 - l2_ratio)) * 18.0
    l2_bid_wall = min(2.0, max(0.5, l2_ratio)) * 18.0
    htf_align_hi = 16.0 if abs(rh240 - rh60) <= htf_tol else 6.0
    htf_align_lo = 16.0 if abs(rl60 - rl240) <= htf_tol else 6.0
    u_liq = max(10.0, min(99.0, 15.0 + near_eqh_60 * 6.5 + vol_cluster * 7.5 + l2_ask_wall + htf_align_hi))
    l_liq = max(10.0, min(99.0, 15.0 + near_eql_60 * 6.5 + vol_cluster * 7.5 + l2_bid_wall + htf_align_lo))

    if symbol == "XAUTUSD":
        l2_abs_bull = (lw_rng >= 0.30 and l2_ratio >= 1.04) or (l2_ratio >= 1.16)
        l2_abs_bear = (uw_rng >= 0.30 and l2_ratio <= 0.96) or (l2_ratio <= 0.86)
    else:
        l2_abs_bull = (lw_rng >= 0.28 and l2_ratio >= 1.03) or (l2_ratio >= 1.15) or (db_imb >= 0.08)
        l2_abs_bear = (uw_rng >= 0.28 and l2_ratio <= 0.97) or (l2_ratio <= 0.87) or (db_imb <= -0.08)

    # Exact Session-Reset Cumulative VWAP (22:00 UTC Gold COMEX rollover, 00:00 UTC Crypto)
    vwap_reset_hr = 22 if symbol == "XAUTUSD" else 0
    vwap_start = max(0, n_bars - 1440)
    for i in range(n_bars - 1, max(0, n_bars - 1500), -1):
        b_t = int(bars[i].get("time", 0))
        if b_t > 0:
            hr_i = datetime.fromtimestamp(b_t, tz=timezone.utc).hour
            if i > 0:
                b_tp = int(bars[i - 1].get("time", 0))
                hr_p = datetime.fromtimestamp(b_tp, tz=timezone.utc).hour if b_tp > 0 else hr_i
                if hr_i == vwap_reset_hr and hr_p != vwap_reset_hr:
                    vwap_start = i
                    break
            elif hr_i == vwap_reset_hr:
                vwap_start = 0
                break
    tpv_sum = sum(((highs[i] + lows[i] + closes[i]) / 3.0) * norm_vols[i] for i in range(vwap_start, n_bars))
    v_sum_vw = sum(norm_vols[vwap_start:]) + 1e-6
    vwap = tpv_sum / v_sum_vw
    c60 = closes[-min(60, n_bars):]
    vwap_std = _sample_std(c60) + 1e-6
    vwap_z = max(-4.0, min(4.0, (c - vwap) / vwap_std))

    dt_utc = datetime.fromtimestamp(ts_val, tz=timezone.utc)
    utc_hr = dt_utc.hour
    est_hr = (utc_hr - 5) % 24
    mins = dt_utc.minute
    dow = dt_utc.weekday()

    news_mult = 3.2 if symbol == "XAUTUSD" else 3.5
    news_lock = ((est_hr == 8) and (25 <= mins <= 36) and ((h - l) >= news_mult * atr_1m)) or ((h - l) >= 5.5 * atr_1m)
    in_session = est_hr in (2, 3, 4, 5, 7, 8, 10, 11, 12, 15, 16, 17, 23)
    kz_win = est_hr in (2, 3, 4, 5, 7, 8, 10, 11, 12)
    sb_win = (est_hr in (3, 10, 15)) or (mins >= 50) or (mins <= 10)
    sess = "LONDON" if (2 <= est_hr <= 7) else ("NEW_YORK" if (8 <= est_hr <= 12) else "ASIA")

    if symbol == "XAUTUSD":
        david_vec = [
            ker_1h,
            ker_15m,
            ker_1m,
            min(2.0, max(0.0, ter_ratio / 15.0)),
            rpos60,
            bpos240,
            vwap_z / 3.0,
            min(3.0, max(-2.0, vol_z_30 / 3.0)),
            min(3.0, max(-2.0, delta_z / 3.0)),
            min(1.0, max(-1.0, l2_ratio - 1.0)),
        ]
    else:
        atr_pct = (atr_1m / max(c, 1.0)) * 100.0
        david_vec = [
            ker_1h,
            ker_15m,
            rpos60,
            bpos240,
            vol_z_30,
            delta_z,
            l2_ratio,
            rsi14,
            roc_15m,
            atr_pct,
        ]

    ctx = {
        "symbol": symbol,
        "o": o, "h": h, "l": l, "c": c, "v": v,
        "ema9": ema9, "ema15": ema15, "ema50": ema50, "ema60": ema60,
        "ema200": ema200, "ema240": ema240, "emad": emad, "emam": emam,
        "macd_up": macd_up, "macd_dn": macd_dn,
        "lw": lw, "uw": uw, "lw_rng": lw_rng, "uw_rng": uw_rng,
        "bull_disp": bull_disp, "bear_disp": bear_disp,
        "b_mss": b_mss, "s_mss": s_mss, "b_mss_5b": b_mss_5b, "s_mss_5b": s_mss_5b,
        "d5": d5, "d15": d15, "delta_z": delta_z, "vz": vz, "vol_z_30": vol_z_30,
        "atr_1m": atr_1m, "atr_5m": atr_5m, "atr_15m": atr_15m_f, "atr_60m": atr_60m,
        "chop": chop, "adx": adx,
        "ker_1h": ker_1h, "ker_15m": ker_15m, "ker_1m": ker_1m, "ter_ratio": ter_ratio,
        "rsi5": rsi5, "rsi14": rsi14, "roc_5m": roc_5m, "roc_15m": roc_15m,
        "cvd_div_bull": cvd_div_bull, "cvd_div_bear": cvd_div_bear,
        "cvd_div_20_bull": cvd_div_20_bull, "cvd_div_20_bear": cvd_div_20_bear,
        "bos_bull": bos_bull, "bos_bear": bos_bear,
        "fvg_bull_gate": fvg_bull_gate, "fvg_bear_gate": fvg_bear_gate,
        "bull_fvg": bull_fvg_win, "bear_fvg": bear_fvg_win,
        "stop_b": stop_b_win, "stop_s": stop_s_win,
        "u_grab": u_grab_win, "l_grab": l_grab_win,
        "b_abs": b_abs_win, "s_abs": s_abs_win,
        "u_liq": u_liq, "l_liq": l_liq,
        "l2_ratio": l2_ratio, "l2_max": l2_max, "db_imb": db_imb, "spread": spread, "l2_ts": l2_ts_val,
        "l2_abs_bull": l2_abs_bull, "l2_abs_bear": l2_abs_bear, "vwap_z": vwap_z,
        "sh20": sh20, "sl20": sl20,
        "rh60": rh60, "rl60": rl60, "box_w60": rh60 - rl60, "poc60": poc60, "rpos60": rpos60,
        "fib60_l": fib60_l, "fib60_s": fib60_s, "room_h60": room_h60, "room_l60": room_l60,
        "rh120": rh120, "rl120": rl120,
        "rh240": rh240, "rl240": rl240, "bpos240": bpos240,
        "fib240_l": fib240_l, "fib240_s": fib240_s, "room_h240": room_h240, "room_l240": room_l240,
        "rh360": rh360, "rl360": rl360,
        "swing_10_low": swing_10_low, "swing_10_high": swing_10_high,
        "swing_12_low": swing_12_low, "swing_12_high": swing_12_high,
        "swing_20_low": swing_20_low, "swing_20_high": swing_20_high,
        "bar_ts": ts_val,
        "allow_1m": bool(HUB.pair_allow_1m.get(symbol, True)),
        "specialist_cd_until": dict(m.specialist_cd_until),
        "utc_hr": utc_hr, "est_hr": est_hr, "mins": mins, "dow": dow, "sess": sess,
        "news_lock": news_lock, "in_session": in_session, "kz_win": kz_win, "sb_win": sb_win,
        "david_vec": david_vec,
    }
    m.last_ctx = ctx
    return ctx


def compute_sr_targets(entry_px: float, ref_dist: float, direction: str, ctx: Dict[str, Any], max_wall_r: float = 3.80) -> Dict[str, Any]:
    if ref_dist <= 0:
        ref_dist = 1.0
    if direction == "LONG":
        sr1h = (ctx["rh60"] - entry_px) / ref_dist
        sr2h = (ctx["rh120"] - entry_px) / ref_dist
        sr4h = (ctx["rh240"] - entry_px) / ref_dist
        sr6h = (ctx["rh360"] - entry_px) / ref_dist
    else:
        sr1h = (entry_px - ctx["rl60"]) / ref_dist
        sr2h = (entry_px - ctx["rl120"]) / ref_dist
        sr4h = (entry_px - ctx["rl240"]) / ref_dist
        sr6h = (entry_px - ctx["rl360"]) / ref_dist

    sr_walls = sorted(list({round(w, 2) for w in (sr1h, sr2h, sr4h, sr6h) if 1.15 <= w <= max_wall_r}))
    outer_tp_r = 3.50
    for w in reversed(sr_walls):
        if 2.60 <= w <= 3.65:
            outer_tp_r = round(w * 0.98, 2)
            break
    tp_price = round(entry_px + outer_tp_r * ref_dist if direction == "LONG" else entry_px - outer_tp_r * ref_dist, 2)
    return {
        "sr1h": round(sr1h, 2),
        "sr_walls": sr_walls,
        "outer_tp_r": outer_tp_r,
        "tp_price": tp_price,
    }


# ==============================================================================
# 1. GOLD (XAUTUSD) — MERGE_1_VPS_EXACT_MASTER (Mon-Fri MERGE_3 + 5m Judas + 0.60x SL)
# ==============================================================================
def _eval_gold_16_agents(side: str, initiator: str, tf: str, ctx: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Evaluate all 16 Syndicate Floor Agents on a Gold candidate bar using 30-bar vol_z."""
    c, o, h, l = ctx["c"], ctx["o"], ctx["h"], ctx["l"]
    ema15, ema50, ema200 = ctx["ema15"], ctx["ema50"], ctx["ema200"]
    ker_1h, ter, bpos60 = ctx["ker_1h"], ctx["ter_ratio"], ctx["rpos60"]
    l2_5, l2_max = ctx["l2_ratio"], ctx["l2_max"]
    vol_z, delta_z = ctx["vol_z_30"], ctx["delta_z"]
    lw_rng, uw_rng = ctx["lw_rng"], ctx["uw_rng"]
    vwap_z, rsi = ctx["vwap_z"], ctx["rsi14"]
    est_hr = ctx["est_hr"]

    hard_vetoes = []
    confirming = [initiator]
    dom_vol, dom_l2 = False, False

    if ker_1h < 0.14 and ter < 3.5 and tf == "1m":
        hard_vetoes.append("1:MARCUS")
    elif (side == "BUY" and c >= ema50) or (side == "SELL" and c <= ema50) or ker_1h >= 0.24:
        confirming.append("1:MARCUS")

    if side == "BUY" and bpos60 > 0.96 and ker_1h < 0.22 and initiator != "8:MAX":
        hard_vetoes.append("2:ARTHUR")
    elif side == "SELL" and bpos60 < 0.04 and ker_1h < 0.22 and initiator != "8:MAX":
        hard_vetoes.append("2:ARTHUR")
    elif (side == "BUY" and (bpos60 <= 0.84 or c > ema200)) or (side == "SELL" and (bpos60 >= 0.16 or c < ema200)):
        confirming.append("2:ARTHUR")

    if ctx["news_lock"]:
        hard_vetoes.append("3:BEN")

    if (side == "BUY" and (ctx["roc_15m"] >= -3.0 or ctx["cvd_div_bull"])) or (side == "SELL" and (ctx["roc_15m"] <= 3.0 or ctx["cvd_div_bear"])):
        confirming.append("4:OWEN")

    opposing = False
    if side == "BUY":
        if ctx["bos_bull"] or ctx["fvg_bull_gate"] or c >= ema15:
            confirming.append("6:KAI")
        if bpos60 <= 0.52 or lw_rng >= 0.18:
            confirming.append("5:LEO")
        if vwap_z <= 1.95:
            confirming.append("9:ELENA")
        if c < ctx["rl60"] - 3.5 and delta_z < -2.8 and initiator != "5:LEO":
            opposing = True
    else:
        if ctx["bos_bear"] or ctx["fvg_bear_gate"] or c <= ema15:
            confirming.append("6:KAI")
        if bpos60 >= 0.48 or uw_rng >= 0.18:
            confirming.append("5:LEO")
        if vwap_z >= -1.95:
            confirming.append("9:ELENA")
        if c > ctx["rh60"] + 3.5 and delta_z > 2.8 and initiator != "5:LEO":
            opposing = True

    in_kz = not (13 <= est_hr <= 16)
    if in_kz:
        confirming.append("10:ZOE")
    else:
        hard_vetoes.append("10:ZOE")

    if ctx["spread"] > 0.65:
        hard_vetoes.append("11:SAM")
    elif side == "BUY" and l2_5 < 0.45 and lw_rng < 0.25:
        hard_vetoes.append("11:SAM")
    elif side == "SELL" and l2_5 > 1.85 and uw_rng < 0.25:
        hard_vetoes.append("11:SAM")
    else:
        if side == "BUY" and (l2_5 >= 0.72 or ctx["l2_abs_bull"] or l2_max >= 1.08 or lw_rng >= 0.16):
            dom_l2 = True
            confirming.append("11:SAM")
        elif side == "SELL" and (l2_5 <= 1.28 or ctx["l2_abs_bear"] or l2_max >= 1.08 or uw_rng >= 0.16):
            dom_l2 = True
            confirming.append("11:SAM")

    if vol_z < -1.65:
        hard_vetoes.append("12:JACK")
    else:
        if side == "BUY" and (lw_rng >= 0.16 or delta_z >= -0.95 or vol_z >= -0.65 or c >= o):
            dom_vol = True
            confirming.append("12:JACK")
        elif side == "SELL" and (uw_rng >= 0.16 or delta_z <= 0.95 or vol_z >= -0.65 or c <= o):
            dom_vol = True
            confirming.append("12:JACK")

    if (side == "BUY" and rsi >= 85.0) or (side == "SELL" and rsi <= 15.0):
        hard_vetoes.append("13:CLARA")
    else:
        confirming.append("13:CLARA")

    passed = (not hard_vetoes) and (not opposing) and in_kz and dom_vol and dom_l2
    return passed, sorted(list(set(confirming)))


def evaluate_gold_signal(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Evaluates XAUTUSD MERGE_1_VPS_EXACT_MASTER (Strictly Causal Mon-Fri Only):
    - Engine A: MERGE_3_DAILY_32D_MEMORY_MASTER (11 Specialists + 6 Pods + Frozen dsa_pf + DAVID k-NN)
    - Engine B (Fallback): 5m 4H Discount/Premium Judas Liquidity Sweep (CVD/Abs + L2)
    - Decoupled 0.60x ATR SL (3.00 floor, 9.35 cap) + Decoupled 0.85x Exit Breathing Scale
    """
    est_hr, mins, dow = ctx["est_hr"], ctx["mins"], ctx["dow"]
    if dow >= 5:
        return None

    c, o, h, l = ctx["c"], ctx["o"], ctx["h"], ctx["l"]
    ema9, ema15, ema50, ema200 = ctx["ema9"], ctx["ema15"], ctx["ema50"], ctx["ema200"]
    emad, emam = ctx["emad"], ctx["emam"]
    rsi5, vz, lw, uw = ctx["rsi5"], ctx["vz"], ctx["lw"], ctx["uw"]
    b_mss, s_mss = ctx["b_mss"], ctx["s_mss"]
    bull_disp, bear_disp = ctx["bull_disp"], ctx["bear_disp"]
    fib60_l, fib60_s = ctx["fib60_l"], ctx["fib60_s"]
    fib240_l, fib240_s = ctx["fib240_l"], ctx["fib240_s"]
    chop, adx, d5, d15 = ctx["chop"], ctx["adx"], ctx["d5"], ctx["d15"]
    poc60, rpos60, bpos240 = ctx["poc60"], ctx["rpos60"], ctx["bpos240"]
    sh20, sl20 = ctx["sh20"], ctx["sl20"]
    macd_up, macd_dn = ctx["macd_up"], ctx["macd_dn"]
    bull_fvg, bear_fvg = ctx["bull_fvg"], ctx["bear_fvg"]
    u_liq, l_liq = ctx["u_liq"], ctx["l_liq"]
    u_grab, l_grab = ctx["u_grab"], ctx["l_grab"]
    b_abs, s_abs = ctx["b_abs"], ctx["s_abs"]
    stop_b, stop_s = ctx["stop_b"], ctx["stop_s"]
    sb_win, kz_win = ctx["sb_win"], ctx["kz_win"]
    is_5m = (mins % 5) == 0
    is_15m = (mins % 15) == 0
    is_1h = (mins == 0)

    candidates: List[Dict[str, Any]] = []
    if True:
        prime_hrs = est_hr in (2, 3, 4, 5, 8, 10, 11, 12, 18, 20, 21)
        base_gate = (not ctx["news_lock"]) and prime_hrs and (ctx["spread"] <= 0.65)
        rk_base = max(3.8, min(11.0, 1.25 * ctx["atr_5m"]))
        rm_l = (ctx["room_h240"] >= 1.10 * rk_base) or (ctx["room_h60"] >= 1.00 * rk_base)
        rm_s = (ctx["room_l240"] >= 1.10 * rk_base) or (ctx["room_l60"] >= 1.00 * rk_base)
        is_london = (ctx["sess"] == "LONDON")

        htf_bull = (
            ((c > emam) and (ema200 > emad))
            or ((c > emad) and (ema50 > ema200) and (chop < 52.5))
            or ((c > ema200) and (ema15 > ema50) and (chop < 50.0) and (d5 > 0))
        )
        htf_bear = (
            ((c < emam) and (ema50 < ema200))
            or ((c < emad) and (ema50 < ema200) and (chop < 53.5))
            or ((c < ema200) and (ema15 < ema50) and (chop < 51.0) and (d5 < 0))
        )
        sarah_l = (not is_london) and (chop < 53.5) and (35 <= rsi5 <= 66) and rm_l
        sarah_s = (32 <= rsi5 <= 66) and rm_s
        disc_l = (fib240_l >= 0.50) or (fib60_l >= 0.54)
        prem_s = (fib240_s >= 0.50) or (fib60_s >= 0.54)
        htf_disc_l = (fib240_l >= 0.57) or (fib60_l >= 0.61)
        htf_prem_s = (fib240_s >= 0.57) or (fib60_s >= 0.61)

        gate_l = base_gate and htf_bull and sarah_l and (c > ema15) and disc_l
        gate_s = base_gate and htf_bear and sarah_s and (c < ema15) and prem_s
        gate_htf_l = base_gate and htf_bull and sarah_l and (c > ema15) and htf_disc_l
        gate_htf_s = base_gate and htf_bear and sarah_s and (c < ema15) and htf_prem_s
        macro_bull_1h = (
            (not ctx["news_lock"]) and ctx["in_session"] and (not is_london)
            and (c > emam) and (ema200 > emad) and (c > ema15) and (chop < 53.2) and (rsi5 <= 65) and rm_l
        )
        macro_bear_1h = (
            (not ctx["news_lock"]) and ctx["in_session"]
            and (c < emam) and (ema50 < ema200) and (c < ema15) and (rsi5 >= 33) and rm_s
        )

        specialists = [
            ("1:ALEX", "8:MAX", "MAX_TREND_PULLBACK", 3,
             (macd_up or (rsi5 >= 43)) and ((c > ema9) or (d5 > 0)) and (vz >= 0.50),
             (macd_dn or (rsi5 <= 57)) and ((c < ema9) or (d5 < 0)) and (vz >= 0.50),
             (fib240_l >= 0.56) and (macd_up or (rsi5 >= 40) or (d5 > 0)),
             (fib240_s >= 0.56) and (macd_dn or (rsi5 <= 60) or (d5 < 0)), 1.35),
            ("2:MAYA", "5:LEO", "LEO_SWEEP_TRAP", 5,
             ((lw >= 1.15) or b_mss or bull_disp) and (vz >= 0.48),
             ((uw >= 1.15) or s_mss or bear_disp) and (vz >= 0.48),
             (fib240_l >= 0.56) and ((lw >= 1.12) or b_mss or (d5 > 0)),
             (fib240_s >= 0.56) and ((uw >= 1.12) or s_mss or (d5 < 0)), 1.35),
            ("3:MARCUS", "8:MAX", "MAX_BOX_BREAKOUT", 4,
             (chop < 52.8) and ((rpos60 <= 0.64) or (fib240_l >= 0.50)) and ((lw >= 1.05) or b_mss or bull_disp or (d5 > 0)) and (vz >= 0.44),
             (chop < 53.4) and ((rpos60 >= 0.36) or (fib240_s >= 0.50)) and ((uw >= 1.05) or s_mss or bear_disp or (d5 < 0)) and (vz >= 0.44),
             (fib240_l >= 0.53) and (chop < 52.5) and ((rpos60 <= 0.58) or (d5 > 0)),
             (fib240_s >= 0.53) and (chop < 53.0) and ((rpos60 >= 0.42) or (d5 < 0)), 1.36),
            ("4:NINA", "6:KAI", "KAI_FVG_BOS_RETEST", 4,
             (bull_fvg or (l <= sl20 * 1.0018) or (lw >= 1.18) or b_mss) and (vz >= 0.58),
             (bear_fvg or (h >= sh20 * 0.9982) or (uw >= 1.18) or s_mss) and (vz >= 0.58),
             (fib240_l >= 0.55) and (bull_fvg or (l <= sl20 * 1.002) or b_mss or (d5 > 0)),
             (fib240_s >= 0.55) and (bear_fvg or (h >= sh20 * 0.998) or s_mss or (d5 < 0)), 1.34),
            ("5:MAX", "7:NINA", "NINA_RANGE_BOX_FADE", 4,
             ((rpos60 <= 0.58) or l_grab or (fib240_l >= 0.52)) and ((lw >= 1.10) or b_mss or (d5 > 0)) and (vz >= 0.64),
             ((rpos60 >= 0.42) or u_grab or (fib240_s >= 0.52)) and ((uw >= 1.10) or s_mss or (d5 < 0)) and (vz >= 0.64),
             (fib240_l >= 0.55) and ((rpos60 <= 0.52) or l_grab or (lw >= 1.10) or (d5 > 0)),
             (fib240_s >= 0.55) and ((rpos60 >= 0.48) or u_grab or (uw >= 1.10) or (d5 < 0)), 1.35),
            ("6:SOPHIA", "5:LEO", "LEO_KILLZONE_SWEEP", 4,
             (kz_win or sb_win or (est_hr >= 18)) and (adx >= 14.5) and (vz >= 0.42),
             (kz_win or sb_win or (est_hr >= 18)) and (adx >= 14.5) and (vz >= 0.42),
             (fib240_l >= 0.57) and (kz_win or sb_win or (est_hr >= 18)) and ((lw >= 1.08) or b_mss or (d5 > 0)),
             (fib240_s >= 0.57) and (kz_win or sb_win or (est_hr >= 18)) and ((uw >= 1.08) or s_mss or (d5 < 0)), 1.35),
            ("7:DEREK", "6:KAI", "KAI_SMC_OTE_TREND", 5,
             ((fib240_l >= 0.53) or (fib60_l >= 0.57)) and (fib240_l <= 0.86) and (vz >= 0.52),
             ((fib240_s >= 0.53) or (fib60_s >= 0.57)) and (fib240_s <= 0.86) and (vz >= 0.52),
             (fib240_l >= 0.58) and (fib240_l <= 0.85) and ((lw >= 1.05) or (d5 > 0)),
             (fib240_s >= 0.58) and (fib240_s <= 0.85) and ((uw >= 1.05) or (d5 < 0)), 1.36),
            ("8:ELENA", "9:ELENA", "ELENA_VWAP_WICK_REV", 5,
             (c > ema9) and (b_mss or (lw >= 1.12)) and (vz >= 0.32),
             (c < ema9) and (s_mss or (uw >= 1.12)) and (vz >= 0.32),
             (fib240_l >= 0.57) and (c > ema9) and (b_mss or (lw >= 1.12) or (d5 > 0)),
             (fib240_s >= 0.57) and (c < ema9) and (s_mss or (uw >= 1.12) or (d5 < 0)), 1.32),
            ("9:SAM", "5:LEO", "LEO_LIQUIDITY_TRAP", 3,
             ((l_liq >= 35.5) or (u_liq >= 37.5) or l_grab) and (vz >= 0.56),
             ((u_liq >= 35.5) or (l_liq >= 37.5) or u_grab) and (vz >= 0.56),
             (fib240_l >= 0.54) and ((l_liq >= 36.0) or (u_liq >= 38.0) or l_grab) and (d5 > 0),
             (fib240_s >= 0.54) and ((u_liq >= 36.0) or (l_liq >= 38.0) or u_grab) and (d5 < 0), 1.35),
            ("10:OMAR", "8:MAX", "MAX_DELTA_MOMENTUM", 4,
             (((d5 > 0) and (d15 > 0)) or b_abs) and (vz >= 0.62),
             (((d5 < 0) and (d15 < 0)) or s_abs) and (vz >= 0.62),
             (fib240_l >= 0.56) and (((d5 > 0) and (d15 > 0)) or b_abs or (lw >= 1.15)),
             (fib240_s >= 0.56) and (((d5 < 0) and (d15 < 0)) or s_abs or (uw >= 1.15)), 1.35),
            ("11:LIAM", "9:ELENA", "ELENA_VWAP_VSA_POCKET", 4,
             (stop_b or (vz >= 0.80)) and (vz >= 0.70),
             (stop_s or (vz >= 0.80)) and (vz >= 0.70),
             (fib240_l >= 0.55) and (stop_b or (vz >= 0.45) or (c <= poc60)) and ((lw >= 1.10) or b_mss or (d5 > 0)),
             (fib240_s >= 0.55) and (stop_s or (vz >= 0.45) or (c >= poc60)) and ((uw >= 1.10) or s_mss or (d5 < 0)), 1.35),
        ]

        allow_1m = bool(ctx.get("allow_1m", True))
        spec_cds = ctx.get("specialist_cd_until") or {}
        cur_bar_ts = int(ctx.get("bar_ts", 0))

        spec_masks: Dict[str, Tuple[bool, bool]] = {}
        for aid, init_16, pb_style, prio, tl, ts_c, tl1h, ts1h, sl_m1 in specialists:
            m1_l = bool(gate_l and tl)
            m1_s = bool(gate_s and ts_c)
            spec_masks[aid] = (m1_l, m1_s)
            for tf_lbl, tf_rk, cond_l, cond_s, sl_m in [
                ("1h", 4, is_1h and macro_bull_1h and tl1h, is_1h and macro_bear_1h and ts1h, 1.20),
                ("15m", 3, is_15m and gate_htf_l and tl, is_15m and gate_htf_s and ts_c, 1.22),
                ("5m", 2, is_5m and gate_htf_l and tl, is_5m and gate_htf_s and ts_c, 1.22),
                ("1m", 1, m1_l, m1_s, sl_m1),
            ]:
                if tf_lbl == "1m" and not allow_1m:
                    continue
                if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{aid}|{tf_lbl}", 0)):
                    continue
                if cond_l:
                    candidates.append({"orig_agent": aid, "initiator": init_16, "playbook": pb_style, "pb_prio": prio, "direction": "LONG", "tf": tf_lbl, "tf_rank": tf_rk, "sl_mult": sl_m, "is_pod": False})
                if cond_s:
                    candidates.append({"orig_agent": aid, "initiator": init_16, "playbook": pb_style, "pb_prio": prio, "direction": "SHORT", "tf": tf_lbl, "tf_rank": tf_rk, "sl_mult": sl_m, "is_pod": False})

        pod_specs = [
            ("POD_MARCUS_SOPHIA", "3:MARCUS", "6:SOPHIA", "8:MAX", "MAX_BOX_BREAKOUT", 4),
            ("POD_MAYA_SAM", "2:MAYA", "9:SAM", "5:LEO", "LEO_LIQUIDITY_TRAP", 5),
            ("POD_MAYA_LIAM", "2:MAYA", "11:LIAM", "9:ELENA", "ELENA_VWAP_VSA_POCKET", 5),
            ("POD_SOPHIA_LIAM", "6:SOPHIA", "11:LIAM", "9:ELENA", "ELENA_VWAP_VSA_POCKET", 4),
            ("POD_DEREK_OMAR", "7:DEREK", "10:OMAR", "6:KAI", "KAI_SMC_OTE_TREND", 5),
            ("POD_NINA_MARCUS", "4:NINA", "3:MARCUS", "6:KAI", "KAI_FVG_BOS_RETEST", 4),
        ]
        for pod_name, ag_a, ag_b, init_16, pb_style, prio in pod_specs:
            p1_l = spec_masks[ag_a][0] and spec_masks[ag_b][0]
            p1_s = spec_masks[ag_a][1] and spec_masks[ag_b][1]
            for tf_lbl, tf_rk, cond_l, cond_s, sl_m in [
                ("15m", 3, is_15m and p1_l, is_15m and p1_s, 1.22),
                ("5m", 2, is_5m and p1_l, is_5m and p1_s, 1.22),
                ("1m", 1, p1_l, p1_s, 1.35),
            ]:
                if tf_lbl == "1m" and not allow_1m:
                    continue
                if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{pod_name}|{tf_lbl}", 0)):
                    continue
                if cond_l:
                    candidates.append({"orig_agent": pod_name, "initiator": init_16, "playbook": pb_style, "pb_prio": prio, "direction": "LONG", "tf": tf_lbl, "tf_rank": tf_rk, "sl_mult": sl_m, "is_pod": True})
                if cond_s:
                    candidates.append({"orig_agent": pod_name, "initiator": init_16, "playbook": pb_style, "pb_prio": prio, "direction": "SHORT", "tf": tf_lbl, "tf_rank": tf_rk, "sl_mult": sl_m, "is_pod": True})
    # Engine A verification: 16-Agent Gate (using vol_z_30), frozen dsa_pf, and frozen DAVID k-NN
    gold_meta = FROZEN_MODELS.get("XAUTUSD", {})
    dsa_map = gold_meta.get("dsa_pf", {})
    default_dsa_pf = float(gold_meta.get("default_dsa_pf", 1.35))
    sess_key = ctx["sess"]

    verified_cands: List[Dict[str, Any]] = []
    david_cache: Dict[str, Tuple[float, float]] = {}
    for cand in candidates:
        side = "BUY" if cand["direction"] == "LONG" else "SELL"
        _, conf_agents = _eval_gold_16_agents(side, cand["initiator"], cand["tf"], ctx)
        if side not in david_cache:
            david_cache[side] = query_david_memory("XAUTUSD", ctx["david_vec"], side)
        tw_wr, tw_pf = david_cache[side]
        cand["confirming_agents"] = conf_agents
        cand["dsa_pf"] = float(dsa_map.get(f"{dow}|{sess_key}|{cand['orig_agent']}", default_dsa_pf))
        cand["david_twins_wr"] = tw_wr
        cand["david_twins_pf"] = tw_pf
        verified_cands.append(cand)

    R8_ELITE = {
        "2:MAYA", "6:SOPHIA", "11:LIAM", "9:SAM",
        "POD_MARCUS_SOPHIA", "POD_MAYA_SAM", "POD_MAYA_LIAM",
    }
    PRIO_HTF = [
        "CHAMP_4_MTF_80WR_SNIPER",
        "CHAMP_3_LOW_DD_ELITE",
        "CHAMP_2_DAILY_100PCT_MEMORY",
        "CHAMP_5_MTF_DAILY_SNIPER",
        "CHAMP_1_MAX_PROFIT",
        "CHAMP_6_HIGH_CADENCE_4TF",
    ]

    chosen_best = None
    chosen_votes: List[str] = []
    chosen_from = ""
    best_vote_score = (-1, -1)

    for direc in ("LONG", "SHORT"):
        d_cands = [x for x in verified_cands if x["direction"] == direc]
        if not d_cands:
            continue
        d_cands.sort(key=lambda x: (-x["tf_rank"], -x["pb_prio"], -(1 if x["is_pod"] else 0), -x["dsa_pf"], x["orig_agent"]))

        champ_matches: Dict[str, List[Dict[str, Any]]] = {}
        for x in d_cands:
            is_r9 = x["dsa_pf"] >= 1.05
            is_r8 = (x["orig_agent"] in R8_ELITE) and (x["dsa_pf"] >= 1.25)
            eff_wr = 54.0 if x["tf"] == "1m" else 48.0
            eff_pf = 1.25 if x["tf"] == "1m" else 1.00
            david_ok = (x["david_twins_wr"] >= eff_wr) and (x["david_twins_pf"] >= eff_pf)

            if (x["tf"] in ("1m", "5m")) and is_r9:
                champ_matches.setdefault("CHAMP_1_MAX_PROFIT", []).append(x)
            if (x["tf"] in ("1m", "5m")) and is_r9 and david_ok:
                champ_matches.setdefault("CHAMP_2_DAILY_100PCT_MEMORY", []).append(x)
            if (x["tf"] in ("1m", "5m")) and is_r8 and david_ok:
                champ_matches.setdefault("CHAMP_3_LOW_DD_ELITE", []).append(x)
            if (x["tf"] in ("5m", "15m", "1h")) and is_r9:
                champ_matches.setdefault("CHAMP_4_MTF_80WR_SNIPER", []).append(x)
                champ_matches.setdefault("CHAMP_5_MTF_DAILY_SNIPER", []).append(x)
            if is_r9:
                champ_matches.setdefault("CHAMP_6_HIGH_CADENCE_4TF", []).append(x)

        member_profiles = [cid for cid in PRIO_HTF if cid in champ_matches]
        c4_15m = any(x["tf"] == "15m" for x in champ_matches.get("CHAMP_4_MTF_80WR_SNIPER", ()))
        is_m3 = (len(member_profiles) >= 4) or c4_15m or ("CHAMP_2_DAILY_100PCT_MEMORY" in champ_matches)
        if not is_m3:
            continue

        top_cid = member_profiles[0]
        picked = champ_matches[top_cid][0]
        cand_score = (len(member_profiles), picked["tf_rank"])
        if cand_score > best_vote_score:
            chosen_best = picked
            chosen_votes = member_profiles
            chosen_from = top_cid
            best_vote_score = cand_score

    # Engine B Fallback: 5m 4H Discount/Premium Liquidity Sweep Judas Trap (Mon-Fri, CVD/Abs + L2)
    if chosen_best is None and is_5m and (not ctx["news_lock"]) and (ctx["spread"] <= 0.65) and (est_hr not in (13, 14, 15, 16, 17)):
        rk_base = max(3.8, min(11.0, 1.25 * ctx["atr_5m"]))
        rm_l = (ctx["room_h240"] >= 1.10 * rk_base) or (ctx["room_h60"] >= 1.00 * rk_base)
        rm_s = (ctx["room_l240"] >= 1.10 * rk_base) or (ctx["room_l60"] >= 1.00 * rk_base)
        l2_r = ctx["l2_ratio"]

        judas_l = (
            ((l <= sl20) or l_grab) and (c > sl20) and (c > o) and (c >= ema9)
            and ((lw >= 1.15) or b_mss) and (d5 > 0) and (l2_r >= 1.02) and (vz >= 0.60)
            and (rpos60 <= 0.40) and (bpos240 <= 0.50) and (chop < 55.0)
            and (ctx["cvd_div_20_bull"] or b_abs) and rm_l
        )
        judas_s = (
            ((h >= sh20) or u_grab) and (c < sh20) and (c < o) and (c <= ema9)
            and ((uw >= 1.15) or s_mss) and (d5 < 0) and (l2_r <= 0.98) and (vz >= 0.60)
            and (rpos60 >= 0.60) and (bpos240 >= 0.50) and (chop < 55.0)
            and (ctx["cvd_div_20_bear"] or s_abs) and rm_s
        )
        judas_dir = "LONG" if judas_l else ("SHORT" if judas_s else None)
        if judas_dir is not None:
            side = "BUY" if judas_dir == "LONG" else "SELL"
            _, conf_agents = _eval_gold_16_agents(side, "5:LEO", "5m", ctx)
            tw_wr, tw_pf = david_cache.get(side) or query_david_memory("XAUTUSD", ctx["david_vec"], side)
            chosen_best = {
                "orig_agent": "L2_JUDAS_4H_SWEEP",
                "initiator": "5:LEO",
                "playbook": "LEO_JUDAS_4H_SWEEP",
                "direction": judas_dir,
                "tf": "5m",
                "sl_mult": 1.35,
                "confirming_agents": conf_agents,
                "david_twins_wr": tw_wr,
                "david_twins_pf": tw_pf,
            }
            chosen_votes = ["JUDAS_4H_SWEEP"]
            chosen_from = "L2_JUDAS_4H_SWEEP"

    if not chosen_best:
        return None

    # Decoupled 0.60x ATR SL (3.00 floor, 9.35 cap) + Decoupled 0.85x Exit Breathing Scale
    tf = chosen_best["tf"]
    orig_sl = max(3.8, min(11.0, chosen_best["sl_mult"] * ctx["atr_5m"]))
    base_sl = round(max(3.23, min(9.35, 0.85 * orig_sl)), 2)
    sl_dist = round(max(3.00, min(9.35, (base_sl / 0.85) * 0.60)), 2)
    sl_tier = "UNIF_0.6"
    exit_sd = base_sl
    sr_info = compute_sr_targets(c, exit_sd, chosen_best["direction"], ctx, max_wall_r=3.60)
    max_hold = 270 if tf in ("15m", "1h") else 180
    v_cnt = 5 if chosen_from == "L2_JUDAS_4H_SWEEP" else len(chosen_votes)

    return {
        "symbol": "XAUTUSD",
        "direction": chosen_best["direction"],
        "tf": tf,
        "setup_agent": chosen_best["orig_agent"],
        "initiator": chosen_best["initiator"],
        "orig_agent": chosen_best["orig_agent"],
        "playbook": chosen_best["playbook"],
        "chosen_from": chosen_from,
        "member_profiles": chosen_votes,
        "vote_count": v_cnt,
        "confirming_count": len(chosen_best["confirming_agents"]),
        "david_twins_wr": chosen_best["david_twins_wr"],
        "david_twins_pf": chosen_best["david_twins_pf"],
        "sl_dist": sl_dist,
        "exit_sd": exit_sd,
        "sl_tier": sl_tier,
        "max_hold_mins": max_hold,
        "partials_spec": [],  # Gold uses dynamic Chart 1H-6H S/R Wall & Ceiling TP
        "sr_info": sr_info,
    }


# ==============================================================================
# 2. BITCOIN (BTCUSD) — MERGE_1_SUPER_CONFLUENCE_OR_15M (M1+M2+M5 + 3-Tier SL)
# ==============================================================================
def compute_btc_adaptive_sl_live(ctx: Dict[str, Any], direction: str, tf_label: str, raw_sl_dist: float, sl_mode: str) -> Tuple[float, float, str]:
    sk = ctx["sess"]
    bw = float(ctx["box_w60"])
    is_trend = bool(ctx["ker_1h"] >= 0.18)
    ref = max(360.0, min(960.0, raw_sl_dist))

    if sl_mode == "TF_SESS_NYCAP":
        tm = {"1h": 0.45, "5m": 0.60, "1m": 0.78, "15m": 0.82}.get(tf_label, 0.70)
        sm = {"LONDON": 0.78, "ASIA": 0.90, "NEW_YORK": 0.88}[sk]
        fl = {"1h": 130.0, "5m": 175.0, "1m": 230.0, "15m": 250.0}.get(tf_label, 195.0)
        return round(max(fl, min(520.0, ref * tm * sm)), 2), round(ref, 2), "MULTI_SL_TF_SESS_NYCAP"
    if sl_mode == "FOUR_TIER_SAFE":
        coil_t = 250.0 if bw < 320.0 else (340.0 if bw < 520.0 else 430.0)
        atr_t = ref * (0.62 if bw < 320.0 else (0.76 if bw < 520.0 else 0.82))
        base = 0.55 * coil_t + 0.45 * atr_t
        tm = {"1h": 0.78, "5m": 0.88, "15m": 1.08, "1m": 1.12}.get(tf_label, 1.0)
        sm = {"LONDON": 0.88, "ASIA": 0.99, "NEW_YORK": 1.02}[sk]
        rg = 1.06 if is_trend else 0.97
        return round(max(170.0, min(520.0, base * tm * sm * rg)), 2), round(ref, 2), "MULTI_SL_4TIER_SAFE"
    if sl_mode == "FOUR_TIER_RR":
        base = 195.0 if bw < 320.0 else (285.0 if bw < 520.0 else 360.0)
        tm = {"1h": 0.75, "5m": 0.84, "15m": 1.12, "1m": 1.15}.get(tf_label, 1.0)
        sm = {"LONDON": 0.86, "ASIA": 0.98, "NEW_YORK": 1.00}[sk]
        rg = 1.08 if is_trend else 0.95
        return round(max(135.0, min(440.0, base * tm * sm * rg)), 2), round(ref, 2), "MULTI_SL_4TIER_RR"
    if sl_mode == "TF_SESS_V2":
        tm = {"1h": 0.45, "5m": 0.62, "1m": 0.80, "15m": 0.85}.get(tf_label, 0.72)
        sm = {"LONDON": 0.80, "ASIA": 0.92, "NEW_YORK": 1.02}[sk]
        fl = {"1h": 130.0, "5m": 180.0, "1m": 240.0, "15m": 260.0}.get(tf_label, 200.0)
        return round(max(fl, min(650.0, ref * tm * sm)), 2), round(ref, 2), "MULTI_SL_TF_SESS_V2"
    return round(ref, 2), round(ref, 2), "BASE_STRUCT_360"


def evaluate_btc_signal(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Evaluates BTCUSD MERGE_1_SUPER_CONFLUENCE_OR_15M:
    - 11 BTC Volatility-Coil Specialists + 6 Pods + 5 L2 Coil Judas Trap Agents
    - evaluate_16_agents_btc 3-Domain Confirmation
    - Trained 21-Slot Day x Session Router + Cross-Roster M1+M2+M5 Quorum
    - route_final_master 3-Tier Multi-SL & Maker Multi-TP Lifecycle
    """
    c, o, h, l = ctx["c"], ctx["o"], ctx["h"], ctx["l"]
    ema50, ema60, ema240, emad, emam = ctx["ema50"], ctx["ema60"], ctx["ema240"], ctx["emad"], ctx["emam"]
    rsi5, vz, lw, uw = ctx["rsi5"], ctx["vz"], ctx["lw_rng"], ctx["uw_rng"]
    d5, d15, db_imb = ctx["d5"], ctx["d15"], ctx["db_imb"]
    fib240_l, fib240_s = ctx["fib240_l"], ctx["fib240_s"]
    u_liq, l_liq = ctx["u_liq"], ctx["l_liq"]
    est_hr, mins, dow = ctx["est_hr"], ctx["mins"], ctx["dow"]
    is_5m = (mins % 5) == 0
    is_15m = (mins % 15) == 0
    is_1h = (mins == 0)

    atr60 = ctx["atr_60m"]
    h60, l60 = ctx["rh60"], ctx["rl60"]
    h240, l240 = ctx["rh240"], ctx["rl240"]
    sq60 = (h60 - l60) < 1.00 * atr60
    sq60_92 = (h60 - l60) < 0.92 * atr60
    sq60_85 = (h60 - l60) < 0.85 * atr60
    sq240 = (h240 - l240) < 2.60 * atr60
    mac_up_4h = (ema60 > emad) and (ema240 > emam)
    mac_dn_4h = (ema60 < emad) and (ema240 < emam)
    kz_active = bool(ctx["kz_win"] or (est_hr >= 18) or (est_hr <= 11))
    base_gate = (not ctx["news_lock"]) and (ctx["spread"] <= 20.0)

    g5_l = base_gate and sq60 and (c < l60) and (d5 < 0) and (d15 < 0)
    g5_s = base_gate and sq60 and (c > h60) and (d5 > 0) and (d15 > 0)
    g15_l = base_gate and sq60 and (c < l60) and (d5 < 0)
    g15_s = base_gate and sq60 and (c > h60) and (d5 > 0)

    specialists = [
        ("1:ALEX", g5_l and (rsi5 <= 48), g5_s and (rsi5 >= 52), g15_l and (rsi5 <= 48), g15_s and (rsi5 >= 52)),
        ("2:MAYA", g5_l and ((lw >= 0.12) or (c < o)), g5_s and ((uw >= 0.12) or (c > o)), g15_l and ((lw >= 0.12) or (c < o)), g15_s and ((uw >= 0.12) or (c > o))),
        ("3:MARCUS", g5_l, g5_s, g15_l and sq60_92, g15_s and sq60_92),
        ("4:NINA", g5_l and sq240, g5_s and sq240, g15_l and (sq240 or (d15 < 0)), g15_s and (sq240 or (d15 > 0))),
        ("5:MAX", base_gate and sq240 and (c < l240) and (d5 < 0) and (d15 < 0), base_gate and sq240 and (c > h240) and (d5 > 0) and (d15 > 0), base_gate and sq240 and (c < l240) and (d5 < 0), base_gate and sq240 and (c > h240) and (d5 > 0)),
        ("6:SOPHIA", g5_l and kz_active, g5_s and kz_active, g15_l and kz_active, g15_s and kz_active),
        ("7:DEREK", g5_l and (fib240_l >= 0.42), g5_s and (fib240_s >= 0.42), g15_l and (fib240_l >= 0.42), g15_s and (fib240_s >= 0.42)),
        ("8:ELENA", g5_l and (c < ema50) and (rsi5 <= 50), g5_s and (c > ema50) and (rsi5 >= 50), g15_l and (c < ema50) and (rsi5 <= 50), g15_s and (c > ema50) and (rsi5 >= 50)),
        ("9:SAM", g5_l and ((l_liq >= 24.0) or (u_liq >= 24.0)), g5_s and ((u_liq >= 24.0) or (l_liq >= 24.0)), g15_l and ((l_liq >= 24.0) or (u_liq >= 24.0)), g15_s and ((u_liq >= 24.0) or (l_liq >= 24.0))),
        ("10:OMAR", g5_l and ((d15 < 0) or (db_imb <= -0.02)), g5_s and ((d15 > 0) or (db_imb >= 0.02)), g15_l and ((d15 < 0) or (db_imb <= -0.02)), g15_s and ((d15 > 0) or (db_imb >= 0.02))),
        ("11:LIAM", g5_l and (sq60_85 or (vz >= 0.20)), g5_s and (sq60_85 or (vz >= 0.20)), g15_l and (sq60_85 or (vz >= 0.20)), g15_s and (sq60_85 or (vz >= 0.20))),
    ]

    raw_hits: List[Dict[str, Any]] = []
    spec_masks: Dict[str, Tuple[bool, bool, bool, bool]] = {}
    raw_sl_btc = max(360.0, min(1200.0, 1.80 * ctx["atr_5m"]))
    allow_1m = bool(ctx.get("allow_1m", True))
    spec_cds = ctx.get("specialist_cd_until") or {}
    cur_bar_ts = int(ctx.get("bar_ts", 0))

    for aid, c5_l, c5_s, c15_l, c15_s in specialists:
        spec_masks[aid] = (bool(c5_l), bool(c5_s), bool(c15_l), bool(c15_s))
        m1_l = bool(c5_l and sq240 and (c < l240))
        m1_s = bool(c5_s and sq240 and (c > h240))
        for tf_lbl, tf_rk, f_l, f_s in [
            ("1h", 4, is_1h and c15_l, is_1h and c15_s),
            ("15m", 3, is_15m and c15_l, is_15m and c15_s),
            ("5m", 2, is_5m and c5_l, is_5m and c5_s),
            ("1m", 1, m1_l, m1_s),
        ]:
            if tf_lbl == "1m" and not allow_1m:
                continue
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{aid}|{tf_lbl}", 0)):
                continue
            pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[aid]
            if f_l:
                raw_hits.append({"orig_agent": aid, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})
            if f_s:
                raw_hits.append({"orig_agent": aid, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})

    pod_specs = [
        ("POD_MARCUS_SOPHIA", "3:MARCUS", "6:SOPHIA"),
        ("POD_MAYA_SAM", "2:MAYA", "9:SAM"),
        ("POD_MAYA_LIAM", "2:MAYA", "11:LIAM"),
        ("POD_SOPHIA_LIAM", "6:SOPHIA", "11:LIAM"),
        ("POD_DEREK_OMAR", "7:DEREK", "10:OMAR"),
        ("POD_NINA_MARCUS", "4:NINA", "3:MARCUS"),
    ]
    for pod_name, ag_a, ag_b in pod_specs:
        c5_l = spec_masks[ag_a][0] and spec_masks[ag_b][0]
        c5_s = spec_masks[ag_a][1] and spec_masks[ag_b][1]
        c15_l = spec_masks[ag_a][2] and spec_masks[ag_b][2]
        c15_s = spec_masks[ag_a][3] and spec_masks[ag_b][3]
        p1_l = c5_l and sq240 and (c < l240)
        p1_s = c5_s and sq240 and (c > h240)
        pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[pod_name]
        for tf_lbl, tf_rk, f_l, f_s in [
            ("1h", 4, is_1h and c15_l, is_1h and c15_s),
            ("15m", 3, is_15m and c15_l, is_15m and c15_s),
            ("5m", 2, is_5m and c5_l, is_5m and c5_s),
            ("1m", 1, p1_l, p1_s),
        ]:
            if tf_lbl == "1m" and not allow_1m:
                continue
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{pod_name}|{tf_lbl}", 0)):
                continue
            if f_l:
                raw_hits.append({"orig_agent": pod_name, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})
            if f_s:
                raw_hits.append({"orig_agent": pod_name, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})

    has_live_l2 = bool(ctx.get("l2_ts", 0.0) > 0.0)
    l2_agents = [
        ("L2_COIL60_JUDAS_TRAP", g5_l and sq240 and (c < l240), g5_s and sq240 and (c > h240), g15_l, g15_s),
        ("L2_COIL_MACRO_SUPPLY_TRAP", base_gate and mac_up_4h and sq240 and (c < l240) and (d5 < 0), base_gate and mac_dn_4h and sq240 and (c > h240) and (d5 > 0), base_gate and mac_up_4h and sq60 and (c < l60) and (d5 < 0), base_gate and mac_dn_4h and sq60 and (c > h60) and (d5 > 0)),
        ("L2_COIL240_LIQ_TRAP", base_gate and sq240 and (c < l240) and (d5 < 0) and (d15 < 0), base_gate and sq240 and (c > h240) and (d5 > 0) and (d15 > 0), base_gate and sq240 and (c < l240) and (d5 < 0), base_gate and sq240 and (c > h240) and (d5 > 0)),
        ("L2_DELTA_COIL_ABSORPTION", base_gate and sq60_85 and sq240 and (c < l240) and (d5 < 0) and (d15 < 0), base_gate and sq60_85 and sq240 and (c > h240) and (d5 > 0) and (d15 > 0), base_gate and sq60_85 and (c < l60) and (d5 < 0) and ((db_imb <= -0.03) or (vz >= 0.45) or has_live_l2), base_gate and sq60_85 and (c > h60) and (d5 > 0) and ((db_imb >= 0.03) or (vz >= 0.45) or has_live_l2)),
        ("L2_COIL120_JUDAS_TRAP", g5_l, g5_s, base_gate and sq60_92 and (c < l60) and (d5 < 0) and (d15 < 0), base_gate and sq60_92 and (c > h60) and (d5 > 0) and (d15 > 0)),
    ]
    for l2_id, c5_l, c5_s, c15_l, c15_s in l2_agents:
        pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[l2_id]
        for tf_lbl, tf_rk, f_l, f_s in [
            ("1h", 4, is_1h and c15_l, is_1h and c15_s),
            ("15m", 3, is_15m and c15_l, is_15m and c15_s),
            ("5m", 2, is_5m and c5_l, is_5m and c5_s),
        ]:
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{l2_id}|{tf_lbl}", 0)):
                continue
            if f_l:
                raw_hits.append({"orig_agent": l2_id, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})
            if f_s:
                raw_hits.append({"orig_agent": l2_id, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl_btc})

    if not raw_hits:
        return None

    # Embargoed Frozen 21-Slot Day x Session Router lookup (evaluated across ALL co-firing candidates!)
    slot_key = f"{dow}|{ctx['sess']}"
    btc_router = FROZEN_MODELS.get("BTCUSD", {}).get("router", {})
    if slot_key in btc_router.get("top2", {}):
        top2_set = set(btc_router["top2"][slot_key])
        pf1_set = set(btc_router.get("top2_pf1", {}).get(slot_key, []))
    else:
        ds_slot = BTC_DAY_SESSION_ROUTER.get((dow, ctx["sess"]))
        top2_set = {ds_slot[0][0], ds_slot[1][0]} if ds_slot else set()
        pf1_set = {ag for ag, pf_v in (ds_slot or ()) if pf_v >= 1.05}

    for side in ("BUY", "SELL"):
        grp = [x for x in raw_hits if x["direction"] == side]
        if not grp:
            continue
        co_orig = list(dict.fromkeys(g["orig_agent"] for g in grp))
        co_fire_cnt = len(co_orig)

        def _btc_router_rank(item: Dict[str, Any]) -> Tuple[int, int, int, str]:
            ag = item["orig_agent"]
            r_rk = 3 if ag in pf1_set else (2 if ag == "L2_COIL_MACRO_SUPPLY_TRAP" else (1 if (ag in top2_set or ag in BTC_COIL_CORE_AGENTS) else 0))
            return (-item["tf_rank"], -r_rk, -item["pb_prio"], ag)

        grp.sort(key=_btc_router_rank)
        tw_wr, tw_pf = query_david_memory("BTCUSD", ctx["david_vec"], side)
        d_60 = (tw_wr >= 60.0) and (tw_pf >= 1.45)
        d_55 = (tw_wr >= 55.0) and (tw_pf >= 1.25)
        d_50 = (tw_wr >= 50.0) and (tw_pf >= 1.05)
        ema240_ok = ((ema240 >= emam) or (d15 < 0)) if side == "BUY" else ((ema240 <= emam) or (d15 > 0))

        winning_pack = None
        winning_score = (-1, -1, -1, -1)

        for cand_item in grp:
            best = dict(cand_item)
            best["co_fire_count"] = co_fire_cnt

            # evaluate_16_agents_btc
            initiator = best["initiator"]
            ker1h, ter, bpos60, bpos240 = ctx["ker_1h"], ctx["ter_ratio"], ctx["rpos60"], ctx["bpos240"]
            l2_5, l2_max = ctx["l2_ratio"], ctx["l2_max"]
            vol_z, delta_z, rsi = ctx["vol_z_30"], ctx["delta_z"], ctx["rsi14"]
            atr = max(ctx["atr_1m"], 10.0)
            confirming = [initiator, "16:RYAN"]
            hard_vetoes = []
            dom_vol, dom_l2 = False, False

            if ker1h < 0.12 and ter < 3.0 and best["tf"] == "1m":
                hard_vetoes.append("1:MARCUS")
            elif (side == "BUY" and c >= ema60 * 0.998) or (side == "SELL" and c <= ema60 * 1.002):
                confirming.append("1:MARCUS")

            if side == "BUY" and bpos60 > 0.96 and ctx["roc_5m"] < 0.8 * atr:
                hard_vetoes.append("2:ARTHUR")
            elif side == "SELL" and bpos60 < 0.04 and ctx["roc_5m"] > -0.8 * atr:
                hard_vetoes.append("2:ARTHUR")
            else:
                confirming.append("2:ARTHUR")

            if ctx["news_lock"]:
                hard_vetoes.append("3:BEN")
            else:
                confirming.append("3:BEN")

            if (side == "BUY" and (ctx["roc_15m"] > 0 or ctx["cvd_div_bull"])) or (side == "SELL" and (ctx["roc_15m"] < 0 or ctx["cvd_div_bear"])):
                confirming.append("4:OWEN")
            if (side == "BUY" and (l <= ctx["sl20"] + 0.5 * atr or ctx["cvd_div_bull"])) or (side == "SELL" and (h >= ctx["sh20"] - 0.5 * atr or ctx["cvd_div_bear"])):
                if "5:LEO" not in confirming:
                    confirming.append("5:LEO")
            if (side == "BUY" and (ctx["bull_fvg"] or ctx["bos_bull"] or bpos240 <= 0.45)) or (side == "SELL" and (ctx["bear_fvg"] or ctx["bos_bear"] or bpos240 >= 0.55)):
                if "6:KAI" not in confirming:
                    confirming.append("6:KAI")
            if (side == "BUY" and bpos60 <= 0.35) or (side == "SELL" and bpos60 >= 0.65):
                if "7:NINA" not in confirming:
                    confirming.append("7:NINA")
            if (side == "BUY" and (ctx["bos_bull"] or ctx["roc_15m"] > 0)) or (side == "SELL" and (ctx["bos_bear"] or ctx["roc_15m"] < 0)):
                if "8:MAX" not in confirming:
                    confirming.append("8:MAX")
            if (side == "BUY" and (ctx["vwap_z"] <= -0.30 or lw >= 0.25)) or (side == "SELL" and (ctx["vwap_z"] >= 0.30 or uw >= 0.25)):
                if "9:ELENA" not in confirming:
                    confirming.append("9:ELENA")

            in_kz = not (14 <= est_hr <= 16)
            if in_kz:
                confirming.append("10:ZOE")

            is_coil_trap = best["orig_agent"] in BTC_COIL_TRAP_AGENTS
            if ctx["spread"] > 20.0:
                hard_vetoes.append("11:SAM")
            elif is_coil_trap:
                dom_l2 = True
                if "11:SAM" not in confirming:
                    confirming.append("11:SAM")
            elif side == "BUY" and l2_5 < 0.38 and db_imb < -0.25 and lw < 0.25:
                hard_vetoes.append("11:SAM")
            elif side == "SELL" and l2_5 > 2.20 and db_imb > 0.25 and uw < 0.25:
                hard_vetoes.append("11:SAM")
            else:
                if side == "BUY" and (l2_5 >= 0.78 or db_imb >= -0.05 or ctx["l2_abs_bull"] or l2_max >= 1.06 or lw >= 0.16):
                    dom_l2 = True
                    if "11:SAM" not in confirming:
                        confirming.append("11:SAM")
                elif side == "SELL" and (l2_5 <= 1.25 or db_imb <= 0.05 or ctx["l2_abs_bear"] or l2_max >= 1.06 or uw >= 0.16):
                    dom_l2 = True
                    if "11:SAM" not in confirming:
                        confirming.append("11:SAM")

            if vol_z < -2.1:
                hard_vetoes.append("12:JACK")
            elif is_coil_trap:
                if vol_z >= -1.2:
                    dom_vol = True
                    confirming.append("12:JACK")
            else:
                if side == "BUY" and (lw >= 0.14 or delta_z >= -0.90 or db_imb >= -0.05 or vol_z >= -0.60 or c >= o):
                    dom_vol = True
                    confirming.append("12:JACK")
                elif side == "SELL" and (uw >= 0.14 or delta_z <= 0.90 or db_imb <= 0.05 or vol_z >= -0.60 or c <= o):
                    dom_vol = True
                    confirming.append("12:JACK")

            if (not is_coil_trap) and ((side == "BUY" and rsi >= 85.0) or (side == "SELL" and rsi <= 15.0)):
                hard_vetoes.append("13:CLARA")
            else:
                confirming.append("13:CLARA")

            if hard_vetoes or not (dom_vol and dom_l2):
                continue

            is_ds_top2 = best["orig_agent"] in top2_set
            is_ds_pf1 = best["orig_agent"] in pf1_set
            is_coil_core = best["orig_agent"] in BTC_COIL_CORE_AGENTS

            member_profiles: List[str] = []
            distinct_rosters = set()
            if is_ds_pf1 and d_60:
                member_profiles.extend(["CHAMP_1_MAX_PROFIT", "CHAMP_2_DAILY_100PCT_MEMORY", "CHAMP_5_MTF_DAILY_SNIPER"])
                distinct_rosters.add("R6_DAY_TIME_PROFITABLE_PF100")
            if best["tf"] in ("5m", "15m", "1h") and d_55 and ((is_ds_pf1 and ema240_ok) or best["orig_agent"] == "L2_COIL_MACRO_SUPPLY_TRAP"):
                member_profiles.append("CHAMP_3_LOW_DD_ELITE")
                distinct_rosters.add("R8_DAY_TIME_EMA240_GUARDIAN")
            if is_ds_pf1 and best["co_fire_count"] >= 3 and d_60:
                member_profiles.append("CHAMP_4_MTF_80WR_SNIPER")
                distinct_rosters.add("R9_DAY_TIME_COFIRE_SNIPER")
            if (is_ds_top2 or is_coil_core) and d_50:
                member_profiles.append("CHAMP_6_HIGH_CADENCE_4TF")
                distinct_rosters.add("R0_DAY_TIME_ROUTER_PLUS_COIL")

            vc = len(member_profiles)
            rc = len(distinct_rosters)
            conf_cnt = len(confirming)

            passed_master = (
                (vc >= 4)
                or (vc == 3 and (rc >= 2 or (conf_cnt >= 11 and (side == "SELL" or tw_wr >= 75.0))))
                or (vc == 2 and "CHAMP_1_MAX_PROFIT" in member_profiles and "CHAMP_3_LOW_DD_ELITE" in member_profiles)
            )
            if not passed_master:
                continue

            sc = (vc, rc, best["tf_rank"], best["pb_prio"])
            if sc > winning_score:
                winning_score = sc
                winning_pack = (best, member_profiles, vc, conf_cnt, ker1h)

        if winning_pack is None:
            continue

        best, member_profiles, vc, conf_cnt, ker1h = winning_pack
        direc_str = "LONG" if side == "BUY" else "SHORT"
        regime_str = "TRENDING" if ker1h >= 0.18 else "SIDEWAYS"

        # route_final_master exact priority order & lifecycle mapping
        PRIO_HTF = ["CHAMP_1_MAX_PROFIT", "CHAMP_2_DAILY_100PCT_MEMORY", "CHAMP_5_MTF_DAILY_SNIPER", "CHAMP_3_LOW_DD_ELITE", "CHAMP_4_MTF_80WR_SNIPER", "CHAMP_6_HIGH_CADENCE_4TF"]
        PRIO_LOWDD = ["CHAMP_3_LOW_DD_ELITE", "CHAMP_5_MTF_DAILY_SNIPER", "CHAMP_2_DAILY_100PCT_MEMORY", "CHAMP_1_MAX_PROFIT", "CHAMP_4_MTF_80WR_SNIPER", "CHAMP_6_HIGH_CADENCE_4TF"]
        PRIO_MTP = ["CHAMP_2_DAILY_100PCT_MEMORY", "CHAMP_5_MTF_DAILY_SNIPER", "CHAMP_3_LOW_DD_ELITE", "CHAMP_4_MTF_80WR_SNIPER", "CHAMP_6_HIGH_CADENCE_4TF", "CHAMP_1_MAX_PROFIT"]

        if vc >= 5 and (direc_str == "LONG" or regime_str == "TRENDING" or best["tf"] == "15m"):
            prio_order = PRIO_HTF
        elif ctx["sess"] == "NEW_YORK" and "CHAMP_3_LOW_DD_ELITE" in member_profiles:
            prio_order = PRIO_LOWDD
        else:
            prio_order = PRIO_MTP

        chosen_from = next(p for p in prio_order if p in member_profiles)
        if chosen_from == "CHAMP_1_MAX_PROFIT":
            sl_mode = "FOUR_TIER_RR"
            partials_spec = [
                {"name": "TP1", "r": 1.80, "frac": 0.20, "stop_r": 0.35},
                {"name": "TP2", "r": 3.20, "frac": 0.20, "stop_r": 1.10},
            ]
            max_hold_btc = 600
        elif chosen_from == "CHAMP_2_DAILY_100PCT_MEMORY":
            sl_mode = "FOUR_TIER_SAFE"
            partials_spec = [
                {"name": "TP1", "r": 2.45, "frac": 0.20, "stop_r": 0.80},
            ]
            max_hold_btc = 600
        elif chosen_from in ("CHAMP_3_LOW_DD_ELITE", "CHAMP_5_MTF_DAILY_SNIPER"):
            sl_mode = "TF_SESS_NYCAP"
            partials_spec = [
                {"name": "TP1", "r": 2.85, "frac": 0.20, "stop_r": 0.90},
            ]
            max_hold_btc = 600
        else:
            sl_mode = "TF_SESS_V2"
            partials_spec = [
                {"name": "TP1", "r": 2.00, "frac": 0.20, "stop_r": 0.40},
                {"name": "TP2", "r": 3.40, "frac": 0.20, "stop_r": 1.20},
            ]
            max_hold_btc = 720

        sl_dist, trail_ref_dist, sl_tier = compute_btc_adaptive_sl_live(ctx, direc_str, best["tf"], best["orig_sl_dist"], sl_mode)
        sr_info = compute_sr_targets(c, trail_ref_dist, direc_str, ctx, max_wall_r=3.80)

        return {
            "symbol": "BTCUSD",
            "direction": direc_str,
            "tf": best["tf"],
            "setup_agent": best["orig_agent"],
            "initiator": best["initiator"],
            "orig_agent": best["orig_agent"],
            "playbook": best["playbook"],
            "chosen_from": chosen_from,
            "member_profiles": member_profiles,
            "david_twins_wr": tw_wr,
            "david_twins_pf": tw_pf,
            "vote_count": vc,
            "confirming_count": conf_cnt,
            "sl_dist": sl_dist,
            "exit_sd": trail_ref_dist,
            "sl_tier": sl_tier,
            "max_hold_mins": max_hold_btc,
            "partials_spec": partials_spec,
            "sr_info": sr_info,
        }

    return None


# ==============================================================================
# 3. ETHEREUM (ETHUSD) — MERGE_1_SUPER_CONFLUENCE_OR_15M (G3P + Wick Shield + Co-Fire)
# ==============================================================================
def evaluate_eth_signal(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Evaluates ETHUSD MERGE_1_SUPER_CONFLUENCE_OR_15M:
    - 11 ETH Specialists + 6 Pods + 4 Real Delta L2 Order Flow Agents
    - evaluate_16_agents_eth 3-Domain Confirmation
    - Embargoed G3P Top-1/Top-2 Day x Session Router + UTC Dead-Zone Shield (08-11 & 20-21 UTC)
    - 6-Champion Quorum (vote_count >= 3 or CHAMP_1 or CHAMP_4) + Adaptive Multi-SL & Maker Multi-TP
    """
    c, o, h, l = ctx["c"], ctx["o"], ctx["h"], ctx["l"]
    ema9, ema15, ema50, ema60, ema200, ema240, emad, emam = (
        ctx["ema9"], ctx["ema15"], ctx["ema50"], ctx["ema60"], ctx["ema200"], ctx["ema240"], ctx["emad"], ctx["emam"]
    )
    rsi5, vz, lw, uw = ctx["rsi5"], ctx["vz"], ctx["lw"], ctx["uw"]
    lw_rng, uw_rng = ctx["lw_rng"], ctx["uw_rng"]
    b_mss, s_mss = ctx["b_mss"], ctx["s_mss"]
    bull_disp, bear_disp = ctx["bull_disp"], ctx["bear_disp"]
    fib60_l, fib60_s = ctx["fib60_l"], ctx["fib60_s"]
    fib240_l, fib240_s = ctx["fib240_l"], ctx["fib240_s"]
    chop, adx, d5, d15 = ctx["chop"], ctx["adx"], ctx["d5"], ctx["d15"]
    poc60, rpos60, bpos240 = ctx["poc60"], ctx["rpos60"], ctx["bpos240"]
    sh20, sl20 = ctx["sh20"], ctx["sl20"]
    macd_up, macd_dn = ctx["macd_up"], ctx["macd_dn"]
    bull_fvg, bear_fvg = ctx["bull_fvg"], ctx["bear_fvg"]
    u_liq, l_liq = ctx["u_liq"], ctx["l_liq"]
    u_grab, l_grab = ctx["u_grab"], ctx["l_grab"]
    b_abs, s_abs = ctx["b_abs"], ctx["s_abs"]
    stop_b, stop_s = ctx["stop_b"], ctx["stop_s"]
    sb_win, kz_win = ctx["sb_win"], ctx["kz_win"]
    db_imb, l2_r5, vwap_z = ctx["db_imb"], ctx["l2_ratio"], ctx["vwap_z"]
    est_hr, utc_hr, mins, dow = ctx["est_hr"], ctx["utc_hr"], ctx["mins"], ctx["dow"]
    is_5m = (mins % 5) == 0
    is_15m = (mins % 15) == 0
    is_1h = (mins == 0)

    base_gate = (not ctx["news_lock"]) and (ctx["spread"] <= 0.75)
    rk_base = max(9.0, min(28.0, 1.25 * ctx["atr_5m"]))
    rm_l = (ctx["room_h240"] >= 1.05 * rk_base) or (ctx["room_h60"] >= 0.95 * rk_base)
    rm_s = (ctx["room_l240"] >= 1.05 * rk_base) or (ctx["room_l60"] >= 0.95 * rk_base)

    htf_bull = ((c > emam) and (ema200 > emad)) or ((c > emad) and (ema50 > ema200) and (chop < 55.0)) or ((c > ema200) and (ema15 > ema50) and (chop < 52.5) and (d5 > 0))
    htf_bear = ((c < emam) and (ema50 < ema200)) or ((c < emad) and (ema50 < ema200) and (chop < 55.5)) or ((c < ema200) and (ema15 < ema50) and (chop < 53.0) and (d5 < 0))

    sarah_l = (chop < 56.0) and (28 <= rsi5 <= 70) and rm_l
    sarah_s = (chop < 56.0) and (28 <= rsi5 <= 72) and rm_s
    disc_l = (fib240_l >= 0.45) or (fib60_l >= 0.50)
    prem_s = (fib240_s >= 0.45) or (fib60_s >= 0.50)
    htf_disc_l = (fib240_l >= 0.52) or (fib60_l >= 0.56)
    htf_prem_s = (fib240_s >= 0.52) or (fib60_s >= 0.56)

    gate_l = base_gate and htf_bull and sarah_l and (c > ema15) and disc_l
    gate_s = base_gate and htf_bear and sarah_s and (c < ema15) and prem_s
    gate_htf_l = base_gate and htf_bull and sarah_l and (c > ema15) and htf_disc_l
    gate_htf_s = base_gate and htf_bear and sarah_s and (c < ema15) and htf_prem_s
    macro_bull_1h = base_gate and (c > emam) and (ema200 > emad) and (c > ema15) and (chop < 55.0) and (rsi5 <= 68) and rm_l
    macro_bear_1h = base_gate and (c < emam) and (ema50 < ema200) and (c < ema15) and (rsi5 >= 30) and rm_s

    specialists = [
        ("1:ALEX", (macd_up or (rsi5 >= 42)) and ((c > ema9) or (d5 > 0)) and (vz >= 0.40), (macd_dn or (rsi5 <= 58)) and ((c < ema9) or (d5 < 0)) and (vz >= 0.40), (fib240_l >= 0.52) and (macd_up or (rsi5 >= 40) or (d5 > 0)), (fib240_s >= 0.52) and (macd_dn or (rsi5 <= 60) or (d5 < 0))),
        ("2:MAYA", ((lw >= 1.08) or b_mss or bull_disp) and (vz >= 0.40), ((uw >= 1.08) or s_mss or bear_disp) and (vz >= 0.40), (fib240_l >= 0.52) and ((lw >= 1.05) or b_mss or (d5 > 0)), (fib240_s >= 0.52) and ((uw >= 1.05) or s_mss or (d5 < 0))),
        ("3:MARCUS", (chop < 54.0) and ((rpos60 <= 0.68) or (fib240_l >= 0.48)) and ((lw >= 1.02) or b_mss or bull_disp or (d5 > 0)) and (vz >= 0.38), (chop < 54.5) and ((rpos60 >= 0.32) or (fib240_s >= 0.48)) and ((uw >= 1.02) or s_mss or bear_disp or (d5 < 0)) and (vz >= 0.38), (fib240_l >= 0.50) and (chop < 54.0) and ((rpos60 <= 0.62) or (d5 > 0)), (fib240_s >= 0.50) and (chop < 54.5) and ((rpos60 >= 0.38) or (d5 < 0))),
        ("4:NINA", (bull_fvg or (l <= sl20 * 1.0025) or (lw >= 1.12) or b_mss) and (vz >= 0.48), (bear_fvg or (h >= sh20 * 0.9975) or (uw >= 1.12) or s_mss) and (vz >= 0.48), (fib240_l >= 0.52) and (bull_fvg or (l <= sl20 * 1.003) or b_mss or (d5 > 0)), (fib240_s >= 0.52) and (bear_fvg or (h >= sh20 * 0.997) or s_mss or (d5 < 0))),
        ("5:MAX", ((rpos60 <= 0.62) or l_grab or (fib240_l >= 0.48)) and ((lw >= 1.05) or b_mss or (d5 > 0)) and (vz >= 0.50), ((rpos60 >= 0.38) or u_grab or (fib240_s >= 0.48)) and ((uw >= 1.05) or s_mss or (d5 < 0)) and (vz >= 0.50), (fib240_l >= 0.52) and ((rpos60 <= 0.55) or l_grab or (lw >= 1.05) or (d5 > 0)), (fib240_s >= 0.52) and ((rpos60 >= 0.45) or u_grab or (uw >= 1.05) or (d5 < 0))),
        ("6:SOPHIA", (kz_win or sb_win or (est_hr >= 18) or (est_hr <= 5)) and (adx >= 13.5) and (vz >= 0.36), (kz_win or sb_win or (est_hr >= 18) or (est_hr <= 5)) and (adx >= 13.5) and (vz >= 0.36), (fib240_l >= 0.52) and ((lw >= 1.04) or b_mss or (d5 > 0)), (fib240_s >= 0.52) and ((uw >= 1.04) or s_mss or (d5 < 0))),
        ("7:DEREK", ((fib240_l >= 0.50) or (fib60_l >= 0.54)) and (fib240_l <= 0.90) and (vz >= 0.42), ((fib240_s >= 0.50) or (fib60_s >= 0.54)) and (fib240_s <= 0.90) and (vz >= 0.42), (fib240_l >= 0.55) and (fib240_l <= 0.88) and ((lw >= 1.02) or (d5 > 0)), (fib240_s >= 0.55) and (fib240_s <= 0.88) and ((uw >= 1.02) or (d5 < 0))),
        ("8:ELENA", (c > ema9) and (b_mss or (lw >= 1.08)) and (vz >= 0.28), (c < ema9) and (s_mss or (uw >= 1.08)) and (vz >= 0.28), (fib240_l >= 0.52) and (c > ema9) and (b_mss or (lw >= 1.08) or (d5 > 0)), (fib240_s >= 0.52) and (c < ema9) and (s_mss or (uw >= 1.08) or (d5 < 0))),
        ("9:SAM", ((l_liq >= 32.0) or (u_liq >= 34.0) or l_grab) and (vz >= 0.46), ((u_liq >= 32.0) or (l_liq >= 34.0) or u_grab) and (vz >= 0.46), (fib240_l >= 0.50) and ((l_liq >= 33.0) or (u_liq >= 35.0) or l_grab) and (d5 > 0), (fib240_s >= 0.50) and ((u_liq >= 33.0) or (l_liq >= 35.0) or u_grab) and (d5 < 0)),
        ("10:OMAR", (((d5 > 0) and (d15 > 0)) or b_abs or (db_imb >= 0.04)) and (vz >= 0.50), (((d5 < 0) and (d15 < 0)) or s_abs or (db_imb <= -0.04)) and (vz >= 0.50), (fib240_l >= 0.52) and (((d5 > 0) and (d15 > 0)) or b_abs or (db_imb >= 0.04)), (fib240_s >= 0.52) and (((d5 < 0) and (d15 < 0)) or s_abs or (db_imb <= -0.04))),
        ("11:LIAM", (stop_b or (vz >= 0.65) or (vwap_z <= -1.2)) and (vz >= 0.55), (stop_s or (vz >= 0.65) or (vwap_z >= 1.2)) and (vz >= 0.55), (fib240_l >= 0.50) and (stop_b or (vz >= 0.38) or (c <= poc60)) and ((lw >= 1.05) or b_mss or (d5 > 0)), (fib240_s >= 0.50) and (stop_s or (vz >= 0.38) or (c >= poc60)) and ((uw >= 1.05) or s_mss or (d5 < 0))),
    ]

    raw_hits: List[Dict[str, Any]] = []
    spec_masks: Dict[str, Tuple[bool, bool]] = {}
    allow_1m = bool(ctx.get("allow_1m", True))
    spec_cds = ctx.get("specialist_cd_until") or {}
    cur_bar_ts = int(ctx.get("bar_ts", 0))

    for aid, tl, ts_cond, tl1h, ts1h in specialists:
        m1_l = bool(gate_l and tl)
        m1_s = bool(gate_s and ts_cond)
        spec_masks[aid] = (m1_l, m1_s)
        pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[aid]
        for tf_lbl, tf_rk, f_l, f_s, sl_m in [
            ("1h", 4, is_1h and macro_bull_1h and tl1h, is_1h and macro_bear_1h and ts1h, 1.40),
            ("15m", 3, is_15m and gate_htf_l and tl, is_15m and gate_htf_s and ts_cond, 1.35),
            ("5m", 2, is_5m and gate_htf_l and tl, is_5m and gate_htf_s and ts_cond, 1.30),
            ("1m", 1, m1_l, m1_s, 1.25),
        ]:
            if tf_lbl == "1m" and not allow_1m:
                continue
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{aid}|{tf_lbl}", 0)):
                continue
            raw_sl = max(9.0, min(28.0, sl_m * ctx["atr_5m"]))
            if f_l:
                raw_hits.append({"orig_agent": aid, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})
            if f_s:
                raw_hits.append({"orig_agent": aid, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})

    pod_specs = [
        ("POD_MARCUS_SOPHIA", "3:MARCUS", "6:SOPHIA"),
        ("POD_MAYA_SAM", "2:MAYA", "9:SAM"),
        ("POD_MAYA_LIAM", "2:MAYA", "11:LIAM"),
        ("POD_SOPHIA_LIAM", "6:SOPHIA", "11:LIAM"),
        ("POD_DEREK_OMAR", "7:DEREK", "10:OMAR"),
        ("POD_NINA_MARCUS", "4:NINA", "3:MARCUS"),
    ]
    for pod_name, ag_a, ag_b in pod_specs:
        p1_l = spec_masks[ag_a][0] and spec_masks[ag_b][0]
        p1_s = spec_masks[ag_a][1] and spec_masks[ag_b][1]
        pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[pod_name]
        for tf_lbl, tf_rk, f_l, f_s, sl_m in [
            ("1h", 4, is_1h and p1_l, is_1h and p1_s, 1.40),
            ("15m", 3, is_15m and p1_l, is_15m and p1_s, 1.35),
            ("5m", 2, is_5m and p1_l, is_5m and p1_s, 1.30),
            ("1m", 1, p1_l, p1_s, 1.25),
        ]:
            if tf_lbl == "1m" and not allow_1m:
                continue
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{pod_name}|{tf_lbl}", 0)):
                continue
            raw_sl = max(9.0, min(28.0, sl_m * ctx["atr_5m"]))
            if f_l:
                raw_hits.append({"orig_agent": pod_name, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})
            if f_s:
                raw_hits.append({"orig_agent": pod_name, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})

    has_live_l2 = bool(ctx.get("l2_ts", 0.0) > 0.0)
    l2_ok_l = base_gate and sarah_l and (has_live_l2 or (vz >= 0.55))
    l2_ok_s = base_gate and sarah_s and (has_live_l2 or (vz >= 0.55))
    l2_agents = [
        ("L2_DELTA_SWEEP_TRAP", l2_ok_l and disc_l and (l_grab or (l <= sl20 * 1.0015)) and ((c > sl20) or (lw >= 0.25)) and ((db_imb >= 0.05) or (l2_r5 >= 1.10)) and (d5 > 0), l2_ok_s and prem_s and (u_grab or (h >= sh20 * 0.9985)) and ((c < sh20) or (uw >= 0.25)) and ((db_imb <= -0.05) or (l2_r5 <= 0.90)) and (d5 < 0)),
        ("L2_CVD_DIVERGENCE_REV", l2_ok_l and htf_bull and ctx["cvd_div_bull"] and (rpos60 <= 0.42) and ((lw >= 0.22) or b_mss) and ((db_imb >= 0.02) or (d5 > 0)), l2_ok_s and htf_bear and ctx["cvd_div_bear"] and (rpos60 >= 0.58) and ((uw >= 0.22) or s_mss) and ((db_imb <= -0.02) or (d5 < 0))),
        ("L2_ABSORPTION_WALL_BOUNCE", l2_ok_l and htf_bull and disc_l and (b_abs or (l2_r5 >= 1.16) or (db_imb >= 0.10)) and (rpos60 <= 0.50) and (c > o) and (c > ema15) and rm_l, l2_ok_s and htf_bear and prem_s and (s_abs or (l2_r5 <= 0.86) or (db_imb <= -0.10)) and (rpos60 >= 0.50) and (c < o) and (c < ema15) and rm_s),
        ("L2_DELTA_DISPLACEMENT_MOM", l2_ok_l and htf_bull and (chop < 52.0) and (b_mss or bull_disp) and ((db_imb >= 0.08) or (l2_r5 >= 1.14)) and (d5 > 0) and (d15 > 0) and (vz >= 0.50), l2_ok_s and htf_bear and (chop < 52.0) and (s_mss or bear_disp) and ((db_imb <= -0.08) or (l2_r5 <= 0.88)) and (d5 < 0) and (d15 < 0) and (vz >= 0.50)),
    ]
    for l2_id, cond_l, cond_s in l2_agents:
        pb_ag, pb_st, pb_pr = PLAYBOOK_MAP[l2_id]
        for tf_lbl, tf_rk, f_l, f_s, sl_m in [
            ("1h", 4, is_1h and cond_l, is_1h and cond_s, 1.45),
            ("15m", 3, is_15m and cond_l, is_15m and cond_s, 1.35),
            ("5m", 2, is_5m and cond_l, is_5m and cond_s, 1.30),
        ]:
            if l2_id == "L2_DELTA_SWEEP_TRAP" and tf_lbl == "15m":
                continue
            if cur_bar_ts and cur_bar_ts < int(spec_cds.get(f"{l2_id}|{tf_lbl}", 0)):
                continue
            raw_sl = max(9.0, min(28.0, sl_m * ctx["atr_5m"]))
            if f_l:
                raw_hits.append({"orig_agent": l2_id, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "BUY", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})
            if f_s:
                raw_hits.append({"orig_agent": l2_id, "initiator": pb_ag, "playbook": pb_st, "pb_prio": pb_pr, "direction": "SELL", "tf": tf_lbl, "tf_rank": tf_rk, "orig_sl_dist": raw_sl})

    if not raw_hits:
        return None

    # Embargoed Frozen 21-Slot G3P Top-1/Top-2 Router (evaluated across ALL co-firing candidates!)
    slot_key = f"{dow}|{ctx['sess']}"
    eth_router = FROZEN_MODELS.get("ETHUSD", {}).get("router", {})
    if slot_key in eth_router.get("top2", {}):
        top2_set = set(eth_router["top2"][slot_key])
        g3p_t1 = set(eth_router.get("top1_pf1", {}).get(slot_key, []))
        g3p_t2 = set(eth_router.get("top2_pf1", {}).get(slot_key, []))
    else:
        ds_slot = ETH_DAY_SESSION_ROUTER.get((dow, ctx["sess"]))
        top2_set = {ds_slot[0][0], ds_slot[1][0]} if ds_slot else set()
        g3p_t1 = {ds_slot[0][0]} if (ds_slot and ds_slot[0][1] >= 1.00) else set()
        g3p_t2 = {ag for ag, pf_v in (ds_slot or ()) if pf_v >= 1.00}

    for side in ("BUY", "SELL"):
        grp = [x for x in raw_hits if x["direction"] == side]
        if not grp:
            continue
        co_orig = list(dict.fromkeys(g["orig_agent"] for g in grp))
        co_fire_cnt = len(co_orig)

        def _eth_router_rank(item: Dict[str, Any]) -> Tuple[int, int, int, str]:
            ag = item["orig_agent"]
            r_rk = 3 if ag in g3p_t1 else (2 if ag in g3p_t2 else (1 if ag in top2_set else 0))
            return (-item["tf_rank"], -r_rk, -item["pb_prio"], ag)

        grp.sort(key=_eth_router_rank)
        tw_wr, tw_pf = query_david_memory("ETHUSD", ctx["david_vec"], side)
        deadzone_ok = utc_hr not in (8, 9, 10, 11, 20, 21)

        winning_pack = None
        winning_score = (-1, -1, -1, -1)

        for cand_item in grp:
            best = dict(cand_item)
            best["co_fire_count"] = co_fire_cnt

            # evaluate_16_agents_eth
            initiator = best["initiator"]
            ker1h, ter, bpos60 = ctx["ker_1h"], ctx["ter_ratio"], ctx["rpos60"]
            l2_max = ctx["l2_max"]
            vol_z, delta_z, rsi = ctx["vol_z_30"], ctx["delta_z"], ctx["rsi14"]
            atr = max(ctx["atr_1m"], 0.5)
            confirming = [initiator, "16:RYAN"]
            hard_vetoes = []
            dom_vol, dom_l2 = False, False

            if ker1h < 0.12 and ter < 3.0 and best["tf"] == "1m":
                hard_vetoes.append("1:MARCUS")
            elif (side == "BUY" and c >= ema60 * 0.998) or (side == "SELL" and c <= ema60 * 1.002):
                confirming.append("1:MARCUS")

            if side == "BUY" and bpos60 > 0.96 and ctx["roc_5m"] < 0.8 * atr:
                hard_vetoes.append("2:ARTHUR")
            elif side == "SELL" and bpos60 < 0.04 and ctx["roc_5m"] > -0.8 * atr:
                hard_vetoes.append("2:ARTHUR")
            else:
                confirming.append("2:ARTHUR")

            if ctx["news_lock"]:
                hard_vetoes.append("3:BEN")
            else:
                confirming.append("3:BEN")

            if (side == "BUY" and (ctx["roc_15m"] > 0 or ctx["cvd_div_bull"])) or (side == "SELL" and (ctx["roc_15m"] < 0 or ctx["cvd_div_bear"])):
                confirming.append("4:OWEN")
            if (side == "BUY" and (l <= ctx["sl20"] + 0.5 * atr or ctx["cvd_div_bull"])) or (side == "SELL" and (h >= ctx["sh20"] - 0.5 * atr or ctx["cvd_div_bear"])):
                if "5:LEO" not in confirming:
                    confirming.append("5:LEO")
            if (side == "BUY" and (ctx["bull_fvg"] or ctx["bos_bull"] or bpos240 <= 0.45)) or (side == "SELL" and (ctx["bear_fvg"] or ctx["bos_bear"] or bpos240 >= 0.55)):
                if "6:KAI" not in confirming:
                    confirming.append("6:KAI")
            if (side == "BUY" and bpos60 <= 0.35) or (side == "SELL" and bpos60 >= 0.65):
                if "7:NINA" not in confirming:
                    confirming.append("7:NINA")
            if (side == "BUY" and (ctx["bos_bull"] or ctx["roc_15m"] > 0)) or (side == "SELL" and (ctx["bos_bear"] or ctx["roc_15m"] < 0)):
                if "8:MAX" not in confirming:
                    confirming.append("8:MAX")
            if (side == "BUY" and (vwap_z <= -0.30 or lw_rng >= 0.25)) or (side == "SELL" and (vwap_z >= 0.30 or uw_rng >= 0.25)):
                if "9:ELENA" not in confirming:
                    confirming.append("9:ELENA")

            in_kz = not (14 <= est_hr <= 16)
            if in_kz:
                confirming.append("10:ZOE")

            if ctx["spread"] > 0.75:
                hard_vetoes.append("11:SAM")
            elif side == "BUY" and l2_r5 < 0.38 and db_imb < -0.25 and lw_rng < 0.25:
                hard_vetoes.append("11:SAM")
            elif side == "SELL" and l2_r5 > 2.20 and db_imb > 0.25 and uw_rng < 0.25:
                hard_vetoes.append("11:SAM")
            else:
                if side == "BUY" and (l2_r5 >= 0.78 or db_imb >= -0.05 or ctx["l2_abs_bull"] or l2_max >= 1.06 or lw_rng >= 0.16):
                    dom_l2 = True
                    if "11:SAM" not in confirming:
                        confirming.append("11:SAM")
                elif side == "SELL" and (l2_r5 <= 1.25 or db_imb <= 0.05 or ctx["l2_abs_bear"] or l2_max >= 1.06 or uw_rng >= 0.16):
                    dom_l2 = True
                    if "11:SAM" not in confirming:
                        confirming.append("11:SAM")

            if vol_z < -2.1:
                hard_vetoes.append("12:JACK")
            else:
                if side == "BUY" and (lw_rng >= 0.14 or delta_z >= -0.90 or db_imb >= -0.05 or vol_z >= -0.60 or c >= o):
                    dom_vol = True
                    confirming.append("12:JACK")
                elif side == "SELL" and (uw_rng >= 0.14 or delta_z <= 0.90 or db_imb <= 0.05 or vol_z >= -0.60 or c <= o):
                    dom_vol = True
                    confirming.append("12:JACK")

            if (side == "BUY" and rsi >= 85.0) or (side == "SELL" and rsi <= 15.0):
                hard_vetoes.append("13:CLARA")
            else:
                confirming.append("13:CLARA")

            if hard_vetoes or not (dom_vol and dom_l2):
                continue

            no_15m_sw = not (best["orig_agent"] == "L2_DELTA_SWEEP_TRAP" and best["tf"] == "15m")
            is_g3p_top1 = (best["orig_agent"] in g3p_t1) and no_15m_sw
            is_g3p_top2 = (best["orig_agent"] in g3p_t2) and no_15m_sw
            is_ds_top2 = best["orig_agent"] in top2_set
            is_ds_maya = best["orig_agent"] in ("2:MAYA", "L2_DELTA_SWEEP_TRAP")

            member_profiles: List[str] = []
            if is_g3p_top1 and deadzone_ok:
                member_profiles.extend(["CHAMP_1_MAX_PROFIT", "CHAMP_3_LOW_DD_ELITE"])
            if is_g3p_top2:
                member_profiles.append("CHAMP_2_DAILY_100PCT_MEMORY")
            if best["tf"] in ("5m", "15m", "1h") and is_g3p_top2 and best["co_fire_count"] >= 2:
                member_profiles.append("CHAMP_4_MTF_80WR_SNIPER")
            if is_g3p_top1:
                member_profiles.append("CHAMP_5_MTF_DAILY_SNIPER")
            if best["tf"] in ("5m", "15m", "1h") and (is_ds_top2 or is_ds_maya) and (tw_wr >= 55.0) and (tw_pf >= 1.25):
                member_profiles.append("CHAMP_6_HIGH_CADENCE_4TF")

            vc = len(member_profiles)
            passed_master = (vc >= 3) or ("CHAMP_1_MAX_PROFIT" in member_profiles) or ("CHAMP_4_MTF_80WR_SNIPER" in member_profiles)
            if not passed_master:
                continue

            c1_bonus = 2 if "CHAMP_1_MAX_PROFIT" in member_profiles else (1 if "CHAMP_4_MTF_80WR_SNIPER" in member_profiles else 0)
            sc = (c1_bonus, vc, best["tf_rank"], best["pb_prio"])
            if sc > winning_score:
                winning_score = sc
                winning_pack = (best, member_profiles, vc, confirming, is_g3p_top1)

        if winning_pack is None:
            continue

        best, member_profiles, vc, confirming, is_g3p_top1 = winning_pack

        direc_str = "LONG" if side == "BUY" else "SHORT"
        is_long = direc_str == "LONG"
        raw_sl_d = best["orig_sl_dist"]
        sw12_d = (c - ctx["swing_12_low"]) if is_long else (ctx["swing_12_high"] - c)
        sw20_d = (c - ctx["swing_20_low"]) if is_long else (ctx["swing_20_high"] - c)

        def _eth_tiered_hybrid_sl() -> Tuple[float, str]:
            if is_g3p_top1:
                if "SWEEP" in best["orig_agent"] or "MAYA" in best["orig_agent"]:
                    return round(max(11.5, min(26.0, max(min(sw12_d + 2.5, raw_sl_d * 1.25), 11.5))), 2), "TOP1_SW12_SWEEP"
                if "ABSORPTION" in best["orig_agent"] or "CVD" in best["orig_agent"]:
                    return round(max(12.2, min(28.0, max(min(sw20_d + 2.0, raw_sl_d * 1.32), 12.2))), 2), "TOP1_SW20_WALL"
                return round(max(13.0, min(29.0, raw_sl_d * 1.38)), 2), "TOP1_MOM_ATR"
            return round(max(14.0, min(34.0, raw_sl_d * 1.45)), 2), "TOP2_BUFFER_ATR"

        # Exact prio_htf lifecycle precedence: CHAMP_1 -> CHAMP_3 -> CHAMP_4 -> CHAMP_5 -> CHAMP_2 -> CHAMP_6
        ETH_PRIO_HTF = [
            "CHAMP_1_MAX_PROFIT",
            "CHAMP_3_LOW_DD_ELITE",
            "CHAMP_4_MTF_80WR_SNIPER",
            "CHAMP_5_MTF_DAILY_SNIPER",
            "CHAMP_2_DAILY_100PCT_MEMORY",
            "CHAMP_6_HIGH_CADENCE_4TF",
        ]
        chosen_from = next(p for p in ETH_PRIO_HTF if p in member_profiles)
        eth_pre_sl_cut = False

        if chosen_from == "CHAMP_1_MAX_PROFIT":
            sl_dist = round(max(14.0, min(34.0, raw_sl_d * 1.45)), 2)
            sl_tier = "ETH_WICK_SHIELD_145X"
            partials_spec = []
        elif chosen_from == "CHAMP_3_LOW_DD_ELITE":
            cf = best["co_fire_count"]
            if cf >= 4:
                sl_dist = round(max(11.0, min(26.0, raw_sl_d * 1.15)), 2)
                sl_tier = f"COFIRE_{cf}AG_TIGHT"
            elif cf >= 2:
                sl_dist = round(max(12.2, min(28.0, raw_sl_d * 1.28)), 2)
                sl_tier = f"COFIRE_{cf}AG_MID"
            else:
                sl_dist = round(max(14.0, min(34.0, raw_sl_d * 1.45)), 2)
                sl_tier = "COFIRE_1AG_SAFE"
            partials_spec = [
                {"name": "TP1", "r": 1.60, "frac": 0.15, "stop_r": None},
                {"name": "TP2", "r": 3.00, "frac": 0.15, "stop_r": None},
            ]
        elif chosen_from == "CHAMP_4_MTF_80WR_SNIPER":
            sl_dist = round(max(14.0, min(34.0, raw_sl_d * 1.45)), 2)
            sl_tier = "G3P_PROBE_145X"
            partials_spec = []
            eth_pre_sl_cut = True
        elif chosen_from in ("CHAMP_5_MTF_DAILY_SNIPER", "CHAMP_2_DAILY_100PCT_MEMORY"):
            sl_dist, sl_tier = _eth_tiered_hybrid_sl()
            partials_spec = [
                {"name": "TP1", "r": 1.60, "frac": 0.15, "stop_r": None},
                {"name": "TP2", "r": 3.00, "frac": 0.15, "stop_r": None},
            ]
        else:
            sl_dist, sl_tier = _eth_tiered_hybrid_sl()
            partials_spec = [
                {"name": "TP1", "r": 2.20, "frac": 0.20, "stop_r": None},
            ]

        sr_info = compute_sr_targets(c, sl_dist, direc_str, ctx, max_wall_r=3.80)
        max_hold = 480

        return {
            "symbol": "ETHUSD",
            "direction": direc_str,
            "tf": best["tf"],
            "setup_agent": best["orig_agent"],
            "initiator": best["initiator"],
            "orig_agent": best["orig_agent"],
            "playbook": best["playbook"],
            "chosen_from": chosen_from,
            "member_profiles": member_profiles,
            "david_twins_wr": tw_wr,
            "david_twins_pf": tw_pf,
            "eth_pre_sl_cut": eth_pre_sl_cut,
            "vote_count": vc,
            "confirming_count": len(confirming),
            "sl_dist": sl_dist,
            "exit_sd": sl_dist,
            "sl_tier": sl_tier,
            "max_hold_mins": max_hold,
            "partials_spec": partials_spec,
            "sr_info": sr_info,
        }

    return None


# ==============================================================================
# MULTI-BROKER 3-ASSET EXECUTION & LIFECYCLE CONTROLLER
# ==============================================================================
class UnifiedStrategyEngine:
    def compute_account_lots(
        self, acct: MultiBrokerAccountClient, symbol: str, entry_px: float, sl_dist: float
    ) -> Tuple[Any, float, float]:
        spec = PAIR_SPECS[symbol]
        pcfg = acct.get_pair_cfg(symbol)

        if acct.broker_type == "DELTA":
            lot_size = spec["lot_size"]
            if acct.sizing_mode == "FIXED_LOT":
                lots = max(1, int(round(float(pcfg.get("lots", 100)))))
            else:
                if symbol == "XAUTUSD":
                    qty = acct.risk_usd / max(sl_dist, 0.5)
                else:
                    eff_sl = sl_dist + (spec["maker_fee"] + spec["taker_fee"]) * entry_px
                    qty = acct.risk_usd / max(eff_sl, 0.5)
                lots = max(1, int(round(qty / lot_size)))
                if acct.liq_guard and entry_px > 0 and acct.leverage > 1 and acct.free_margin_usd > 0:
                    safe_notional = acct.free_margin_usd * float(acct.leverage) * 0.85
                    max_safe_lots = max(1, int(safe_notional / max(entry_px * lot_size, 1e-6)))
                    lots = min(lots, max_safe_lots)
            unit_qty = round(lots * lot_size, 5)
            margin_usd = round((unit_qty * entry_px) / float(max(acct.leverage, 1)), 2)
            return lots, unit_qty, margin_usd

        # Non-Delta Multi-Broker Sizing (FIXED_LOT default or RISK_USD auto-calc)
        pv = float(pcfg.get("point_value", 1.0))
        if acct.sizing_mode == "FIXED_LOT":
            raw_lots = float(pcfg.get("lots", 1.0))
            if acct.broker_type == "MT5_API":
                lots: Any = max(0.01, round(raw_lots, 2))
            elif acct.broker_type in ("PICKMYTRADE", "TRADOVATE_API"):
                lots = max(1, int(round(raw_lots)))
            else:
                lots = max(1, int(round(raw_lots))) if raw_lots >= 1.0 and abs(raw_lots - round(raw_lots)) < 1e-6 else max(0.01, round(raw_lots, 2))
        else:
            raw = acct.risk_usd / max(sl_dist * pv, 1e-6)
            if acct.broker_type == "MT5_API":
                lots = max(0.01, round(raw, 2))
            else:
                lots = max(1, int(round(raw)))

        unit_qty = round(float(lots) * pv, 5)
        margin_usd = round(max(1.0, (unit_qty * entry_px) / float(max(acct.leverage, 1))), 2)
        return lots, unit_qty, margin_usd

    def _est_fee(self, acct: MultiBrokerAccountClient, symbol: str, qty_units: float, px: float, is_maker: bool = False, sides: int = 1) -> float:
        if acct.broker_type == "DELTA":
            spec = PAIR_SPECS[symbol]
            rate = spec["maker_fee"] if is_maker else spec["taker_fee"]
            return round(qty_units * px * rate * DELTA_GST_MULT * sides, 4)
        if acct.broker_type in ("PICKMYTRADE", "TRADOVATE_API"):
            return round(0.87 * abs(float(qty_units or 1.0)) * sides, 4)
        return 0.0

    def _est_roundtrip_fee(
        self,
        acct: MultiBrokerAccountClient,
        symbol: str,
        qty_units_or_lots: float,
        entry_px: float,
        exit_px: float,
        entry_is_maker: bool,
        exit_is_maker: bool,
    ) -> float:
        f_in = self._est_fee(acct, symbol, qty_units_or_lots, entry_px, is_maker=entry_is_maker, sides=1)
        f_out = self._est_fee(acct, symbol, qty_units_or_lots, exit_px, is_maker=exit_is_maker, sides=1)
        return round(f_in + f_out, 4)

    def on_tick(self, symbol: str, mid_px: float):
        with HUB.lock:
            if HUB.engine_paused or HUB.pair_states.get(symbol) == "PAUSED":
                return
            m = HUB.markets.get(symbol)
            if m is not None:
                cur_sp = float(m.l2_depth.get("spread", PAIR_SPECS[symbol]["default_spread"]))
                if cur_sp > float(PAIR_SPECS[symbol]["max_spread"]):
                    return
            active_keys = [k for k in HUB.open_positions.keys() if k[1] == symbol]
            for k in active_keys:
                self.manage_position(k[0], symbol, mid_px, mid_px, mid_px, is_bar_close=False)

    def on_bar_close(self, symbol: str, bar: Dict[str, Any]):
        with HUB.lock:
            m = HUB.markets[symbol]
            b_ts = int(bar.get("time", time.time()))
            if m.cur_l2_samples and (m.cur_l2_min_ts == b_ts or m.cur_l2_min_ts == 0):
                m.finalize_l2_minute(b_ts)
            m.append_bar(bar)
            if HUB.engine_paused or HUB.pair_states.get(symbol) == "PAUSED":
                return

            c_px, h_px, l_px = float(bar["close"]), float(bar["high"]), float(bar["low"])
            ctx = compute_symbol_context(symbol, bar_ts=b_ts)

            active_keys = [k for k in HUB.open_positions.keys() if k[1] == symbol]
            for k in active_keys:
                self.manage_position(k[0], symbol, h_px, l_px, c_px, is_bar_close=True, ctx=ctx)

            if not ctx or int(time.time()) < m.cooldown_until_ts:
                return
            flat_accounts = [
                a for a in HUB.accounts
                if a.mode != "PAUSED"
                and a.get_pair_cfg(symbol).get("enabled", False)
                and (a.id, symbol) not in HUB.open_positions
            ]
            if not flat_accounts:
                return

            sig = None
            if symbol == "XAUTUSD":
                sig = evaluate_gold_signal(ctx)
            elif symbol == "BTCUSD":
                sig = evaluate_btc_signal(ctx)
            elif symbol == "ETHUSD":
                sig = evaluate_eth_signal(ctx)

            if sig:
                self.dispatch_entry_parallel(sig, flat_accounts, c_px)

    def dispatch_entry_parallel(self, sig: Dict[str, Any], accounts: List[MultiBrokerAccountClient], ref_px: float):
        symbol = sig["symbol"]
        direction = sig["direction"]
        is_long = direction == "LONG"
        s_mult = 1.0 if is_long else -1.0
        sl_dist = float(sig["sl_dist"])
        exit_sd = float(sig["exit_sd"])
        sr_info = sig["sr_info"]

        def _open_for_account(acct: MultiBrokerAccountClient):
            pcfg = acct.get_pair_cfg(symbol)
            if not pcfg.get("enabled", True):
                return
            lots, unit_qty, margin_usd = self.compute_account_lots(acct, symbol, ref_px, sl_dist)
            b_sym = pcfg["symbol"]

            entry_is_maker = False
            if acct.broker_type == "DELTA":
                acct.ensure_leverage(symbol)
                res = acct.place_entry_order(symbol, direction, lots, ref_px)
                if not res.get("success"):
                    HUB.record_dispatch_log(acct, "OPEN", b_sym, float(lots), False, 0, str(res.get("error", "Unknown")))
                    HUB.send_telegram_async(
                        f"⚠️ <b>ORDER REJECTED ({symbol})</b>\nAccount: {acct.label}\nError: {res.get('error', 'Unknown')}",
                        kind="error",
                    )
                    return
                if res.get("partial_entry_lots"):
                    lots = int(res["partial_entry_lots"])
                    unit_qty = round(lots * PAIR_SPECS[symbol]["lot_size"], 5)
                entry_is_maker = bool(res.get("entry_is_maker", False))
                fill_raw = (res.get("result") or {}).get("average_fill_price") if isinstance(res.get("result"), dict) else None
                fill_px = round(float(fill_raw), 2) if (fill_raw and float(fill_raw) > 0) else round(ref_px, 2)
                role_tag = "MAKER" if entry_is_maker else "TAKER"
                HUB.record_dispatch_log(
                    acct, "OPEN", b_sym, float(lots), True, 200 if acct.is_live_authenticated else 0,
                    f"{'[LIVE]' if acct.is_live_authenticated else '[PAPER]'} DELTA {role_tag} {direction} {lots} @ ${fill_px:.2f}"
                )
            else:
                tp_dist_open = float(sr_info.get("tp_dist") or 0.0)
                if sig.get("partials_spec"):
                    tp_dist_open = round(float(sig["partials_spec"][-1]["r"]) * exit_sd, 2)
                elif tp_dist_open <= 0 and sr_info.get("tp_price"):
                    tp_dist_open = round(abs(float(sr_info["tp_price"]) - ref_px), 2)
                elif tp_dist_open <= 0:
                    tp_dist_open = round(exit_sd * 2.5, 2)

                res = acct.dispatch_broker_action(
                    "OPEN", symbol, direction, float(lots), ref_px, sl_dist, tp_dist_open,
                    trail_offset=-sl_dist, reason=sig.get("setup_agent", "SIGNAL"),
                )
                if acct.is_live_authenticated and not res.get("success"):
                    return
                fill_px = round(ref_px, 2)

            init_sl = round(fill_px - s_mult * sl_dist, 2)

            # Build partial TP tranches (skip splitting when position is at minimum 1 contract / 0.01 lot)
            active_partials = []
            tp_order_ids = []
            is_mt5 = acct.broker_type == "MT5_API"
            min_unit = 0.01 if is_mt5 else 1.0
            can_split = (float(lots) >= 0.02) if is_mt5 else (float(lots) > 1.0)

            if can_split:
                rem_alloc = float(lots)
                for p in sig.get("partials_spec", []):
                    if is_mt5:
                        p_lots = round(float(lots) * float(p["frac"]), 2)
                        max_take = round(max(0.0, rem_alloc - min_unit), 2)
                        p_lots = min(p_lots, max_take) if rem_alloc > min_unit else 0.0
                    else:
                        p_lots = float(int(round(float(lots) * float(p["frac"]))))
                        p_lots = min(p_lots, max(0.0, rem_alloc - 1.0)) if rem_alloc > 1.0 else 0.0
                    if p_lots >= min_unit:
                        rem_alloc = round(rem_alloc - p_lots, 4)
                        tp_px_i = round(fill_px + s_mult * float(p["r"]) * exit_sd, 2)
                        oid = acct.place_maker_tp_order(symbol, direction, p_lots, tp_px_i) if acct.broker_type == "DELTA" else None
                        if oid:
                            tp_order_ids.append(oid)
                        active_partials.append({
                            "name": p["name"],
                            "r": float(p["r"]),
                            "lots": fmt_qty_json(p_lots),
                            "px": tp_px_i,
                            "stop_r": p.get("stop_r"),
                            "done": False,
                            "order_id": oid,
                        })

            tp_display_px = active_partials[0]["px"] if active_partials else (sr_info["tp_price"] if symbol == "XAUTUSD" else 0.0)
            tp_dist_saved = round(abs(float(tp_display_px) - fill_px), 2) if tp_display_px else round(exit_sd * 2.5, 2)

            if acct.broker_type == "DELTA" and symbol == "XAUTUSD" and not active_partials and sr_info.get("tp_price"):
                oid = acct.place_maker_tp_order(symbol, direction, lots, sr_info["tp_price"])
                if oid:
                    tp_order_ids.append(oid)

            spec = PAIR_SPECS[symbol]
            in_rate = spec["maker_fee"] if entry_is_maker else spec["taker_fee"]
            fee_be_pts = ((in_rate + spec["taker_fee"]) * DELTA_GST_MULT * fill_px) + (0.25 / max(unit_qty, 0.0001)) if (acct.broker_type == "DELTA" and symbol == "XAUTUSD") else 0.0
            entry_fee = self._est_fee(acct, symbol, unit_qty if acct.broker_type == "DELTA" else float(lots), fill_px, is_maker=entry_is_maker, sides=1)

            pos_obj: Dict[str, Any] = {
                "id": int(time.time() * 1000) % 1000000,
                "account_id": acct.id,
                "account_label": acct.label,
                "broker_type": acct.broker_type,
                "broker_symbol": b_sym,
                "sizing_mode": acct.sizing_mode,
                "entry_is_maker": entry_is_maker,
                "point_value": float(pcfg["point_value"]),
                "tick_size": float(pcfg["tick_size"]),
                "symbol": symbol,
                "direction": direction,
                "tf": sig["tf"],
                "setup_agent": sig["setup_agent"],
                "initiator": sig.get("initiator", sig["setup_agent"]),
                "orig_agent": sig["orig_agent"],
                "playbook": sig["playbook"],
                "chosen_from": sig.get("chosen_from", ""),
                "member_profiles": list(sig.get("member_profiles", [])),
                "david_twins_wr": float(sig.get("david_twins_wr", 0.0)),
                "david_twins_pf": float(sig.get("david_twins_pf", 0.0)),
                "eth_pre_sl_cut": bool(sig.get("eth_pre_sl_cut", False)),
                "sl_tier": sig["sl_tier"],
                "entry_price": fill_px,
                "current_price": fill_px,
                "init_sl": init_sl,
                "cur_sl": init_sl,
                "sl_price": init_sl,
                "tp_price": tp_display_px,
                "sl_dist": sl_dist,
                "tp_dist": tp_dist_saved,
                "exit_sd": exit_sd,
                "fee_be_pts": fee_be_pts,
                "lots": fmt_qty_json(lots),
                "initial_lots": fmt_qty_json(lots),
                "unit_qty": unit_qty,
                "leverage": acct.leverage,
                "margin_usd": margin_usd,
                "entry_ts": int(time.time()),
                "entry_time_ist": fmt_ist_datetime(),
                "date_ist": fmt_ist_date(),
                "hold_mins": 0,
                "max_hold_mins": sig["max_hold_mins"],
                "peak_fav": 0.0,
                "peak_bar": 0,
                "stage": 0,
                "stall_locked": False,
                "stage_reason": "STOP_LOSS",
                "sr_partial_done": False,
                "rev_partial_done": False,
                "active_partials": active_partials,
                "realized_gross_usd": 0.0,
                "realized_fee_usd": 0.0,
                "partial_tags": [],
                "fee_usd": round(entry_fee, 2),
                "unrealized_pnl_usd": 0.0,
                "r_multiple": 0.0,
                "sl_order_id": None,
                "exchange_sl_price": None,
                "sl_order_lots": lots,
                "tp_order_ids": tp_order_ids,
                **sr_info,
            }

            if acct.broker_type == "DELTA":
                acct.sync_exchange_sl(symbol, direction, lots, init_sl, pos_obj)
            else:
                pos_obj["exchange_sl_price"] = init_sl

            with HUB.lock:
                HUB.open_positions[(acct.id, symbol)] = pos_obj

            print(
                f"[👑 ENTRY {symbol} -> {acct.broker_type}:{b_sym}] Account={acct.label} | {direction} {lots} Lots @ ${fill_px:.2f} "
                f"| SL_Dist=${sl_dist:.2f} ({sig['sl_tier']}) | Agent={sig['setup_agent']}"
            )
            HUB.send_telegram_async(
                f"🚀 <b>TRADE OPENED · {symbol} ({direction})</b>\n"
                f"Broker: <b>{acct.broker_type}</b> ({b_sym}) · <b>{acct.label}</b> [{acct.mode}]\n"
                f"Agent: <b>{sig['setup_agent']}</b> ({sig['tf']} · {sig['playbook']})\n"
                f"Ref Price: <b>${fill_px:.2f}</b> | Size: <b>{lots}</b>\n"
                f"SL Distance: <b>{sl_dist:.2f} pts</b> ({sig['sl_tier']})",
                kind="open",
            )

        for acct in accounts:
            HUB.executor.submit(_open_for_account, acct)

    def _record_partial_scale_out(self, acct: MultiBrokerAccountClient, pos: Dict[str, Any], take_lots: float, fill_px: float, fee_rate: float, tag: str):
        symbol = pos["symbol"]
        spec = PAIR_SPECS[symbol]
        is_long = pos["direction"] == "LONG"
        s_mult = 1.0 if is_long else -1.0
        take_f = float(take_lots)
        cur_lots = float(pos["lots"])
        if take_f <= 0 or take_f >= cur_lots:
            return
        if not pos.get("already_filled_on_exchange"):
            if acct.broker_type == "DELTA":
                acct.place_market_order(symbol, pos["direction"], take_f, fill_px, reduce_only=True)
            else:
                acct.dispatch_broker_action("PARTIAL_CLOSE", symbol, pos["direction"], take_f, fill_px, pos["sl_dist"], 0.0, reason=tag)

        pv = spec["lot_size"] if acct.broker_type == "DELTA" else float(pos.get("point_value", 1.0))
        q_part = take_f * pv
        g_part = s_mult * (fill_px - float(pos["entry_price"])) * q_part
        ex_comm = float(pos.pop("exchange_fill_commission", 0.0) or 0.0)
        if ex_comm > 0 and acct.broker_type == "DELTA":
            in_leg = self._est_fee(acct, symbol, q_part, float(pos["entry_price"]), is_maker=bool(pos.get("entry_is_maker", False)), sides=1)
            f_part = round(in_leg + ex_comm, 4)
        else:
            f_part = self._est_roundtrip_fee(
                acct,
                symbol,
                q_part if acct.broker_type == "DELTA" else take_f,
                float(pos["entry_price"]),
                fill_px,
                entry_is_maker=bool(pos.get("entry_is_maker", False)),
                exit_is_maker=(fee_rate == spec["maker_fee"]),
            )

        pos["realized_gross_usd"] = round(pos.get("realized_gross_usd", 0.0) + g_part, 4)
        pos["realized_fee_usd"] = round(pos.get("realized_fee_usd", 0.0) + f_part, 4)
        pos["lots"] = fmt_qty_json(round(cur_lots - take_f, 4))
        pos["unit_qty"] = round(float(pos["lots"]) * pv, 5)
        if tag not in pos["partial_tags"]:
            pos["partial_tags"].append(tag)

        if acct.broker_type == "DELTA":
            acct.sync_exchange_sl(symbol, pos["direction"], pos["lots"], pos["cur_sl"], pos)
        else:
            trail_offset = s_mult * (float(pos["cur_sl"]) - float(pos["entry_price"]))
            acct.dispatch_broker_action("MODIFY_SL", symbol, pos["direction"], float(pos["lots"]), fill_px, pos["sl_dist"], float(pos.get("tp_dist", 0.0)), trail_offset=trail_offset, reason=tag)

        HUB.send_telegram_async(
            f"🎯 <b>PARTIAL TP ({tag}) · {symbol}</b>\n"
            f"Broker: <b>{acct.broker_type}</b> · <b>{acct.label}</b>\n"
            f"Scaled Out: <b>{take_lots} @ ${fill_px:.2f}</b>\n"
            f"Remaining Runner: <b>{pos['lots']}</b> (SL @ ${pos['cur_sl']:.2f})",
            kind="partial",
        )

    def manage_position(self, account_id: str, symbol: str, h_px: float, l_px: float, c_px: float, is_bar_close: bool = False, ctx: Optional[Dict[str, Any]] = None):
        pos = HUB.open_positions.get((account_id, symbol))
        if not pos:
            return
        acct = next((a for a in HUB.accounts if a.id == account_id), None) or HUB.get_default_account()
        if not acct:
            return
        spec = PAIR_SPECS[symbol]

        now_ts = int(time.time())
        pos["hold_mins"] = max(0, int((now_ts - pos["entry_ts"]) // 60))
        is_long = pos["direction"] == "LONG"
        s_mult = 1.0 if is_long else -1.0
        e_px = float(pos["entry_price"])
        sl_dist = float(pos["sl_dist"])
        exit_sd = float(pos["exit_sd"])
        cur_sl = float(pos["cur_sl"])

        pos["current_price"] = round(c_px, 2)
        live_pts = s_mult * (c_px - e_px)
        pos["r_multiple"] = round(live_pts / sl_dist, 2) if sl_dist > 0 else 0.0
        open_gross = live_pts * float(pos["unit_qty"])
        open_fee = self._est_roundtrip_fee(
            acct,
            symbol,
            float(pos["unit_qty"]) if acct.broker_type == "DELTA" else float(pos["lots"]),
            e_px,
            c_px,
            entry_is_maker=bool(pos.get("entry_is_maker", False)),
            exit_is_maker=False,
        )
        tot_gross = pos.get("realized_gross_usd", 0.0) + open_gross
        tot_fee = pos.get("realized_fee_usd", 0.0) + open_fee
        pos["fee_usd"] = round(tot_fee, 2)
        pos["unrealized_pnl_usd"] = round(tot_gross - tot_fee, 2)

        fav = (h_px - e_px) if is_long else (e_px - l_px)
        if fav > pos["peak_fav"]:
            pos["peak_fav"] = fav
            pos["peak_bar"] = pos["hold_mins"]

        mfe_r = pos["peak_fav"] / exit_sd if exit_sd > 0 else 0.0
        cur_close_r = s_mult * (c_px - e_px) / exit_sd if exit_sd > 0 else 0.0
        sl_close_r = s_mult * (c_px - e_px) / sl_dist if sl_dist > 0 else 0.0

        # 1. Active 1-Second Failsafe Stop-Loss / Trailing-Stop Cut
        stop_hit = (l_px <= cur_sl) if is_long else (h_px >= cur_sl)
        if stop_hit:
            self.close_position(acct, pos, cur_sl, pos.get("stage_reason") or "STOP_LOSS")
            return

        # 2. Multi-TP Partial Limit Fills
        sl_changed = False
        for p in pos.get("active_partials", []):
            if p["done"] or float(pos["lots"]) <= 0:
                continue
            if acct.broker_type == "DELTA" and acct.is_live_authenticated and p.get("order_id"):
                continue
            tp_hit = (h_px >= p["px"]) if is_long else (l_px <= p["px"])
            if tp_hit:
                p["done"] = True
                take_l = min(float(p["lots"]), float(pos["lots"]))
                if take_l >= float(pos["lots"]):
                    self.close_position(acct, pos, p["px"], p["name"], is_maker=True)
                    return
                self._record_partial_scale_out(acct, pos, take_l, p["px"], spec["maker_fee"], p["name"])
                if p.get("stop_r") is not None:
                    ns = round(e_px + s_mult * float(p["stop_r"]) * exit_sd, 2)
                    if (ns > pos["cur_sl"] and ns < c_px) if is_long else (ns < pos["cur_sl"] and ns > c_px):
                        pos["cur_sl"] = ns
                        pos["sl_price"] = ns
                        pos["stage"] = max(pos["stage"], 2)
                        pos["stage_reason"] = f"{p['name']}_FLOOR"
                        sl_changed = True

        # 3. Gold Outer Chart S/R Ceiling TP
        if symbol == "XAUTUSD" and pos.get("tp_price"):
            if not (acct.broker_type == "DELTA" and acct.is_live_authenticated and pos.get("tp_order_ids")):
                tp_px = float(pos["tp_price"])
                if (is_long and h_px >= tp_px) or ((not is_long) and l_px <= tp_px):
                    self.close_position(acct, pos, tp_px, "SR_CEILING_TP", is_maker=True)
                    return

        sr_walls = pos.get("sr_walls") or []
        sr1h = float(pos.get("sr1h", 1.0))

        if ctx:
            exit_bmss = ctx["b_mss"] if symbol == "XAUTUSD" else ctx["b_mss_5b"]
            exit_smss = ctx["s_mss"] if symbol == "XAUTUSD" else ctx["s_mss_5b"]
            exit_vz = ctx["vz"] if symbol == "XAUTUSD" else ctx["vol_z_30"]
        else:
            exit_bmss = False
            exit_smss = False
            exit_vz = 0.0

        is_mt5 = acct.broker_type == "MT5_API"
        can_split_dyn = (float(pos["lots"]) >= 0.02) if is_mt5 else (float(pos["lots"]) > 1.0)

        # 4. Pillar 1: Chart 1H-6H S/R Wall Rejection TP & S/R-Flip Stop Floor
        if symbol in ("XAUTUSD", "ETHUSD") and mfe_r >= 1.25:
            for w in sr_walls:
                if is_bar_close and ctx and (0.95 * w <= mfe_r <= 1.08 * w) and cur_close_r >= 1.10:
                    if is_long:
                        rej = (c_px < ctx["o"] and (ctx["d5"] < 0 or ctx["uw"] >= 1.05 or c_px < ctx["ema9"])) or (
                            (pos["hold_mins"] - pos["peak_bar"] >= 2) and cur_close_r >= 0.88 * w and (ctx["d5"] < 0 or exit_smss)
                        )
                    else:
                        rej = (c_px > ctx["o"] and (ctx["d5"] > 0 or ctx["lw"] >= 1.05 or c_px > ctx["ema9"])) or (
                            (pos["hold_mins"] - pos["peak_bar"] >= 2) and cur_close_r >= 0.88 * w and (ctx["d5"] > 0 or exit_bmss)
                        )
                    if rej:
                        if symbol == "XAUTUSD":
                            self.close_position(acct, pos, round(c_px, 2), "SR_WALL_TP")
                            return
                        elif not pos.get("sr_partial_done") and can_split_dyn:
                            pos["sr_partial_done"] = True
                            if is_mt5:
                                take_l = max(0.01, min(round(float(pos["lots"]) - 0.01, 2), round(float(pos["lots"]) * 0.50, 2)))
                            else:
                                take_l = float(max(1, min(int(float(pos["lots"])) - 1, int(round(float(pos["lots"]) * 0.50)))))
                            self._record_partial_scale_out(acct, pos, take_l, round(c_px, 2), spec["taker_fee"], "TP_SR_WALL")
                            ns = round(e_px + s_mult * max(0.45, cur_close_r * 0.55) * exit_sd, 2)
                            if (ns > pos["cur_sl"] and ns < c_px) if is_long else (ns < pos["cur_sl"] and ns > c_px):
                                pos["cur_sl"] = ns
                                pos["sl_price"] = ns
                                pos["stage"] = max(pos["stage"], 4)
                                pos["stage_reason"] = "SR_WALL_RUNNER_FLOOR"
                                sl_changed = True
                        break
                elif mfe_r > 1.08 * w:
                    flip_mult = 0.86 if symbol == "XAUTUSD" else 0.82
                    flip_sl = round(e_px + s_mult * (w * flip_mult) * exit_sd, 2)
                    if (flip_sl > pos["cur_sl"] and flip_sl < c_px) if is_long else (flip_sl < pos["cur_sl"] and flip_sl > c_px):
                        pos["cur_sl"] = flip_sl
                        pos["sl_price"] = flip_sl
                        pos["stage"] = 4
                        pos["stall_locked"] = False
                        pos["stage_reason"] = "SR_FLIP_FLOOR"
                        sl_changed = True

        # 5. Pillar 2: Proactive Multi-TF Reversal Cuts (Bar Close)
        if is_bar_close and ctx:
            opp_mss = exit_smss if is_long else exit_bmss
            opp_d5 = (ctx["d5"] < 0) if is_long else (ctx["d5"] > 0)
            opp_d15 = (ctx["d15"] < 0) if is_long else (ctx["d15"] > 0)
            ema15_lost = (c_px < ctx["ema15"]) if is_long else (c_px > ctx["ema15"])
            sw20_brk = (c_px < ctx["sl20"]) if is_long else (c_px > ctx["sh20"])

            if symbol == "XAUTUSD":
                if pos["stage"] == 0 and pos["hold_mins"] >= 6 and (pos["peak_fav"] / sl_dist) <= 0.32 and (-0.86 <= sl_close_r <= -0.68):
                    if opp_mss and sw20_brk and opp_d5 and exit_vz >= 1.15:
                        self.close_position(acct, pos, round(c_px, 2), "INSTANT_PRE_SL_CUT")
                        return
                if pos["stage"] == 0 and pos.get("sl_tier") != "T2_L2_COUNTERFLOW_SHIELD_085" and pos["hold_mins"] >= 8 and (pos["peak_fav"] / sl_dist) <= 0.18 and (-0.85 <= sl_close_r <= -0.58):
                    if opp_mss and ema15_lost and opp_d5 and opp_d15:
                        self.close_position(acct, pos, round(c_px, 2), "INSTANT_PRE_SL_CUT")
                        return
                if pos["stage"] == 0 and sr1h <= 0.15 and pos["hold_mins"] >= 5 and mfe_r >= 0.50 and cur_close_r <= -0.25:
                    if opp_mss and ema15_lost:
                        self.close_position(acct, pos, round(c_px, 2), "FAILED_SR_BREAK_CUT")
                        return
                if pos["hold_mins"] >= 7 and (0.75 <= mfe_r <= 1.75) and cur_close_r >= 0.48 and ((mfe_r - cur_close_r) >= 0.22):
                    rpos_ext = (ctx["rpos60"] >= 0.85) if is_long else (ctx["rpos60"] <= 0.15)
                    if opp_mss and opp_d5 and opp_d15 and (ema15_lost or rpos_ext):
                        self.close_position(acct, pos, round(c_px, 2), "FLOW_REVERSAL_CUT")
                        return

            if symbol == "ETHUSD" and pos.get("eth_pre_sl_cut"):
                if pos["stage"] == 0 and pos["hold_mins"] >= 5 and mfe_r <= 0.32 and (-0.88 <= cur_close_r <= -0.60):
                    if opp_mss and sw20_brk and opp_d5 and exit_vz >= 0.90:
                        self.close_position(acct, pos, round(c_px, 2), "PRE_SL_REV_CUT")
                        return
                if pos["stage"] == 0 and sr1h <= 0.20 and pos["hold_mins"] >= 5 and mfe_r >= 0.50 and cur_close_r <= -0.25:
                    if opp_mss and ema15_lost:
                        self.close_position(acct, pos, round(c_px, 2), "FAILED_SR_BREAK_CUT")
                        return

            if symbol in ("XAUTUSD", "ETHUSD"):
                if pos["tf"] in ("1m", "5m") and pos["stage"] <= 1 and mfe_r >= 0.75 and (0.18 <= cur_close_r <= 0.26):
                    if opp_mss and opp_d5:
                        self.close_position(acct, pos, round(c_px, 2), "FAST_REVERSAL_SAVE")
                        return
                rev_vz_min = 1.25 if symbol == "XAUTUSD" else 1.20
                if pos["hold_mins"] >= 10 and mfe_r >= 1.15 and cur_close_r >= 0.45 and ((mfe_r - cur_close_r) >= 0.55):
                    if opp_mss and opp_d5 and exit_vz >= rev_vz_min:
                        if symbol == "ETHUSD":
                            if not pos.get("rev_partial_done") and can_split_dyn:
                                pos["rev_partial_done"] = True
                                if is_mt5:
                                    take_l = max(0.01, min(round(float(pos["lots"]) - 0.01, 2), round(float(pos["lots"]) * 0.50, 2)))
                                else:
                                    take_l = float(max(1, min(int(float(pos["lots"])) - 1, int(round(float(pos["lots"]) * 0.50)))))
                                self._record_partial_scale_out(acct, pos, take_l, round(c_px, 2), spec["taker_fee"], "TP_REV_LOCK")
                                ns = round(e_px + s_mult * max(0.35, cur_close_r * 0.55) * exit_sd, 2)
                                if (ns > pos["cur_sl"] and ns < c_px) if is_long else (ns < pos["cur_sl"] and ns > c_px):
                                    pos["cur_sl"] = ns
                                    pos["sl_price"] = ns
                                    pos["stage"] = max(pos["stage"], 4)
                                    pos["stage_reason"] = "REV_RUNNER_FLOOR"
                                    sl_changed = True
                        else:
                            self.close_position(acct, pos, round(c_px, 2), "REVERSAL_PROFIT_LOCK")
                            return

        # 6. Pillar 3A: Smart Stall Lock & Fee-Positive Breathing Floor (Strictly Bar Close Only)
        stall_trig = 0.54 if symbol == "XAUTUSD" else 0.65
        stall_bars = 6 if symbol == "XAUTUSD" else 8
        stall_lock_r = 0.33 if symbol == "XAUTUSD" else 0.35
        if is_bar_close and ctx and pos["stage"] <= 1 and mfe_r >= stall_trig:
            stalled = (pos["hold_mins"] - pos["peak_bar"]) >= stall_bars
            flow_flip = (exit_smss and ctx["d5"] < 0) if is_long else (exit_bmss and ctx["d5"] > 0)
            ema15_lost = (c_px < ctx["ema15"]) if is_long else (c_px > ctx["ema15"])
            if flow_flip or (stalled and ema15_lost):
                lock_dist = max(stall_lock_r * exit_sd, pos.get("fee_be_pts", 0.0))
                lock_px = round(e_px + s_mult * lock_dist, 2)
                if (c_px >= lock_px and lock_px > pos["cur_sl"]) if is_long else (c_px <= lock_px and lock_px < pos["cur_sl"]):
                    pos["cur_sl"] = lock_px
                    pos["sl_price"] = lock_px
                    pos["stage"] = max(pos["stage"], 1)
                    pos["stall_locked"] = True
                    pos["stage_reason"] = "SMART_STALL_LOCK"
                    sl_changed = True
            elif symbol == "XAUTUSD" and stalled and (not ema15_lost) and (not pos.get("stall_locked")):
                breath_r = min(0.40, max(0.12, mfe_r - 0.52))
                lock_dist = max(breath_r * exit_sd, pos.get("fee_be_pts", 0.0))
                lock_px = round(e_px + s_mult * lock_dist, 2)
                if (c_px >= lock_px and lock_px > pos["cur_sl"]) if is_long else (c_px <= lock_px and lock_px < pos["cur_sl"]):
                    pos["cur_sl"] = lock_px
                    pos["sl_price"] = lock_px
                    pos["stage"] = max(pos["stage"], 1)
                    pos["stage_reason"] = "CONTINUOUS_RATCHET_TRAIL"
                    sl_changed = True

        # 7. Pillar 3B: Breakeven Lock (1-Second + Bar Close)
        be_trig = 1.05 if symbol == "XAUTUSD" else (1.10 if symbol == "BTCUSD" else 1.15)
        be_lock = 0.45 if symbol == "XAUTUSD" else (0.40 if symbol == "BTCUSD" else 0.42)
        if pos["stage"] <= 1 and mfe_r >= be_trig:
            be_px = round(e_px + s_mult * be_lock * exit_sd, 2)
            if (be_px > pos["cur_sl"] and be_px < c_px) if is_long else (be_px < pos["cur_sl"] and be_px > c_px):
                pos["cur_sl"] = be_px
                pos["sl_price"] = be_px
                pos["stage_reason"] = "BREAKEVEN"
                sl_changed = True
            pos["stage"] = 2
            pos["stall_locked"] = False

        # 8. Pillar 3C: Continuous S/R-Aware Ratchet Trailing SL + Swing Floor
        r_trig = 1.10 if symbol == "XAUTUSD" else (1.40 if symbol == "BTCUSD" else 1.35)
        if mfe_r >= r_trig:
            trail_ctx = ctx or HUB.markets[symbol].last_ctx
            in_sr = bool(trail_ctx and ((trail_ctx["rpos60"] >= 0.88) if is_long else (trail_ctx["rpos60"] <= 0.12))) or any(0.92 * w <= mfe_r <= 1.05 * w for w in sr_walls)
            if symbol == "XAUTUSD":
                keep_pct = 0.72 if in_sr else 0.62
                floor_r = 0.45
            elif symbol == "BTCUSD":
                keep_pct = 0.65 if in_sr else 0.55
                floor_r = 0.40
            else:
                keep_pct = 0.62 if in_sr else 0.52
                floor_r = 0.40
            ratchet_r = max(floor_r, mfe_r * keep_pct)
            cand_sl = e_px + s_mult * ratchet_r * exit_sd
            if trail_ctx and mfe_r >= (1.50 if symbol == "XAUTUSD" else 1.60):
                if symbol == "XAUTUSD":
                    sw_px = (trail_ctx["swing_12_low"] - 0.15 * exit_sd) if is_long else (trail_ctx["swing_12_high"] + 0.15 * exit_sd)
                else:
                    sw_px = (trail_ctx["swing_20_low"] - 0.18 * exit_sd) if is_long else (trail_ctx["swing_20_high"] + 0.18 * exit_sd)
                cand_sl = max(cand_sl, sw_px) if is_long else min(cand_sl, sw_px)
            cand_sl = round(cand_sl, 2)
            if (cand_sl > pos["cur_sl"] and cand_sl < c_px) if is_long else (cand_sl < pos["cur_sl"] and cand_sl > c_px):
                pos["cur_sl"] = cand_sl
                pos["sl_price"] = cand_sl
                pos["stage"] = 4
                pos["stall_locked"] = False
                pos["stage_reason"] = "CONTINUOUS_RATCHET_TRAIL" if symbol == "XAUTUSD" else "RATCHET_TRAIL"
                sl_changed = True

        if sl_changed:
            if acct.broker_type == "DELTA":
                acct.sync_exchange_sl(symbol, pos["direction"], pos["lots"], pos["cur_sl"], pos)
            else:
                trail_offset = s_mult * (float(pos["cur_sl"]) - e_px)
                acct.dispatch_broker_action(
                    "MODIFY_SL", symbol, pos["direction"], float(pos["lots"]), c_px,
                    sl_dist, float(pos.get("tp_dist", 0.0)), trail_offset=trail_offset,
                    reason=pos.get("stage_reason") or "TRAIL_SL",
                )

        # 9. Time Cycle Exit
        if pos["hold_mins"] >= int(pos.get("max_hold_mins", 270)):
            self.close_position(acct, pos, round(c_px, 2), f"TIME_{pos.get('max_hold_mins', 270)}M")

    def close_position(self, acct: MultiBrokerAccountClient, pos: Dict[str, Any], exit_px: float, reason: str, is_maker: bool = False, already_flat_on_exchange: bool = False):
        symbol = pos["symbol"]
        key = (acct.id, symbol)
        now_ts = int(time.time())
        if now_ts - pos.get("last_close_attempt_ts", 0) < 4:
            return
        pos["last_close_attempt_ts"] = now_ts

        if acct.broker_type == "DELTA":
            acct.cancel_all_symbol_orders(symbol, pos)
            if not already_flat_on_exchange and float(pos["lots"]) > 0:
                res = acct.place_market_order(symbol, pos["direction"], pos["lots"], exit_px, reduce_only=True)
                if acct.is_live_authenticated and not res.get("success"):
                    print(f"[CLOSE DEFERRED {symbol}] {acct.label}: {res.get('error')}")
                    return
                fill_raw = (res.get("result") or {}).get("average_fill_price") if isinstance(res.get("result"), dict) else None
                if fill_raw:
                    try:
                        exit_px = round(float(fill_raw), 2)
                    except Exception:
                        pass
                HUB.record_dispatch_log(
                    acct, "CLOSE", pos.get("broker_symbol", symbol), float(pos["lots"]),
                    True, 200 if acct.is_live_authenticated else 0, f"Closed @ ${exit_px:.2f} ({reason})"
                )
        else:
            if not already_flat_on_exchange and float(pos["lots"]) > 0:
                acct.dispatch_broker_action(
                    "CLOSE", symbol, pos["direction"], float(pos["lots"]), exit_px,
                    float(pos["sl_dist"]), 0.0, reason=reason,
                )

        spec = PAIR_SPECS[symbol]
        is_long = pos["direction"] == "LONG"
        s_mult = 1.0 if is_long else -1.0
        e_px = float(pos["entry_price"])
        pv = spec["lot_size"] if acct.broker_type == "DELTA" else float(pos.get("point_value", 1.0))
        rem_q = float(pos["lots"]) * pv

        rem_gross = s_mult * (exit_px - e_px) * rem_q
        ex_comm = float(pos.pop("exchange_fill_commission", 0.0) or 0.0)
        if ex_comm > 0 and acct.broker_type == "DELTA":
            in_leg = self._est_fee(acct, symbol, rem_q, e_px, is_maker=bool(pos.get("entry_is_maker", False)), sides=1)
            rem_fee = round(in_leg + ex_comm, 4)
        else:
            rem_fee = self._est_roundtrip_fee(
                acct,
                symbol,
                rem_q if acct.broker_type == "DELTA" else float(pos["lots"]),
                e_px,
                exit_px,
                entry_is_maker=bool(pos.get("entry_is_maker", False)),
                exit_is_maker=is_maker,
            )

        gross_usd = round(pos.get("realized_gross_usd", 0.0) + rem_gross, 2)
        fee_usd = round(pos.get("realized_fee_usd", 0.0) + rem_fee, 2)
        net_usd = round(gross_usd - fee_usd, 2)

        pts = s_mult * (exit_px - e_px)
        r_mult = round(pts / float(pos["sl_dist"]), 2) if float(pos["sl_dist"]) > 0 else 0.0
        tags = list(pos.get("partial_tags", []))
        if reason not in tags:
            tags.append(reason)
        full_reason = "+".join(tags)

        closed_row = {
            "account_id": acct.id,
            "account_label": acct.label,
            "broker_type": acct.broker_type,
            "broker_symbol": pos.get("broker_symbol", symbol),
            "symbol": symbol,
            "direction": pos["direction"],
            "tf": pos["tf"],
            "setup_agent": pos["setup_agent"],
            "initiator": pos.get("initiator", pos["setup_agent"]),
            "orig_agent": pos.get("orig_agent", pos["setup_agent"]),
            "playbook": pos.get("playbook", ""),
            "entry_price": round(e_px, 2),
            "exit_price": round(float(exit_px), 2),
            "sl_price": round(float(pos["init_sl"]), 2),
            "lots": pos.get("initial_lots", pos["lots"]),
            "leverage": int(pos.get("leverage", acct.leverage)),
            "gross_pnl_usd": gross_usd,
            "fee_usd": fee_usd,
            "net_pnl_usd": net_usd,
            "r_multiple": r_mult,
            "hold_mins": int(pos.get("hold_mins", 0)),
            "date_ist": fmt_ist_date(),
            "entry_time_ist": pos.get("entry_time_ist", fmt_ist_datetime()),
            "exit_time_ist": fmt_ist_datetime(),
            "exit_reason": full_reason,
        }

        with HUB.lock:
            HUB.open_positions.pop(key, None)
            if not (acct.broker_type == "DELTA" and acct.is_live_authenticated):
                acct.wallet_balance_usd = round(float(acct.wallet_balance_usd) + net_usd, 2)
                acct.free_margin_usd = round(float(acct.free_margin_usd) + net_usd, 2)
            HUB.save_trade(closed_row)
            HUB.save_runtime_config()
            def_acct = HUB.get_default_account()
            is_primary_acct = (def_acct is None) or (acct.id == def_acct.id) or (acct.broker_type == "DELTA")
            if is_primary_acct:
                orig_ag = str(pos.get("orig_agent") or pos.get("setup_agent") or "")
                tf_lbl = str(pos.get("tf") or "5m")
                spec_cd = int(SPECIALIST_COOLDOWN_SECS.get(symbol, {}).get(tf_lbl, 0))
                if orig_ag and spec_cd > 0 and orig_ag != "INBOUND_WEBHOOK":
                    cd_key = f"{orig_ag}|{tf_lbl}"
                    HUB.markets[symbol].specialist_cd_until[cd_key] = max(
                        int(HUB.markets[symbol].specialist_cd_until.get(cd_key, 0)),
                        int(time.time()) + spec_cd,
                    )
                if net_usd < 0:
                    cd_secs = int(PAIR_SPECS[symbol].get("post_loss_cd_secs", 0))
                    if cd_secs > 0:
                        HUB.markets[symbol].cooldown_until_ts = max(
                            HUB.markets[symbol].cooldown_until_ts, int(time.time()) + cd_secs
                        )

        print(
            f"[✅ CLOSED {symbol} ({acct.broker_type})] Account={acct.label} | {pos['direction']} Exit @ ${exit_px:.2f} ({full_reason}) "
            f"| Gross=${gross_usd:+.2f} | Net=${net_usd:+.2f} ({r_mult:+.2f}R)"
        )
        icon = "🟢" if net_usd >= 0 else "🔴"
        HUB.send_telegram_async(
            f"{icon} <b>TRADE CLOSED · {symbol} ({pos['direction']})</b>\n"
            f"Broker: <b>{acct.broker_type}</b> · <b>{acct.label}</b>\n"
            f"Reason: <b>{full_reason}</b>\n"
            f"Ref Entry: <b>${e_px:.2f}</b> &rarr; Exit: <b>${exit_px:.2f}</b> ({r_mult:+.2f}R)\n"
            f"Net Realized P&amp;L: <b>${net_usd:+.2f}</b>",
            kind="close",
        )

    def sync_all_accounts_positions(self):
        """Every 5 seconds, reconciles RAM open_positions and resting exchange SL/TP across LIVE Delta accounts."""
        with HUB.lock:
            accts = [a for a in HUB.accounts if a.broker_type == "DELTA" and a.is_live_authenticated]
        for acct in accts:
            acct.fetch_balance(force=False)
            pos_map = acct.fetch_open_positions_map()
            if pos_map is None:
                continue

            fill_queries: List[Tuple[str, int, int, int]] = []
            with HUB.lock:
                for sym in PAIRS:
                    ex_pos = pos_map.get(sym, {"size": 0})
                    sz = int(ex_pos.get("size", 0))
                    ram_pos = HUB.open_positions.get((acct.id, sym))
                    if ram_pos is not None:
                        if sz == 0 or (abs(sz) < int(round(float(ram_pos["lots"])))):
                            fill_queries.append((sym, sz, int(ram_pos.get("entry_ts", 0)), int(round(float(ram_pos["lots"])))))

            fills_by_sym: Dict[str, Optional[Dict[str, Any]]] = {}
            for sym, _sz, e_ts, _lots in fill_queries:
                fills_by_sym[sym] = acct.fetch_latest_fill(sym, since_ts=e_ts)

            with HUB.lock:
                for sym in PAIRS:
                    ex_pos = pos_map.get(sym, {"size": 0})
                    sz = int(ex_pos.get("size", 0))
                    key = (acct.id, sym)
                    ram_pos = HUB.open_positions.get(key)
                    if ram_pos is None:
                        continue
                    fill_info = fills_by_sym.get(sym)

                    if sz == 0:
                        px = (fill_info or {}).get("price") or HUB.markets[sym].last_price or ram_pos["entry_price"]
                        is_mk = (fill_info or {}).get("role") == "maker"
                        if fill_info and float(fill_info.get("commission", 0.0) or 0.0) > 0:
                            ram_pos["exchange_fill_commission"] = float(fill_info["commission"])
                        if is_mk and sym == "XAUTUSD":
                            reason = "SR_CEILING_TP"
                        elif is_mk:
                            reason = "EXCHANGE_MAKER_TP"
                        else:
                            reason = ram_pos.get("stage_reason") or "EXCHANGE_SL"
                        self.close_position(
                            acct, ram_pos, round(float(px), 2), reason,
                            is_maker=is_mk, already_flat_on_exchange=True,
                        )
                    else:
                        ex_lots = abs(sz)
                        ram_lots_i = int(round(float(ram_pos["lots"])))
                        if ex_lots < ram_lots_i:
                            diff_lots = ram_lots_i - ex_lots
                            is_long = ram_pos["direction"] == "LONG"
                            s_mult = 1.0 if is_long else -1.0
                            e_px = float(ram_pos["entry_price"])
                            exit_sd = float(ram_pos["exit_sd"])
                            cur_px = HUB.markets[sym].last_price or e_px
                            fill_px = (fill_info or {}).get("price")

                            matched_any = False
                            rem_diff = diff_lots
                            for p in ram_pos.get("active_partials", []):
                                if p["done"] or rem_diff <= 0:
                                    continue
                                take_i = min(int(round(float(p["lots"]))), rem_diff)
                                if take_i <= 0:
                                    continue
                                matched_any = True
                                p["done"] = True
                                rem_diff -= take_i
                                p_px = float(fill_px or p["px"])
                                ram_pos["already_filled_on_exchange"] = True
                                self._record_partial_scale_out(
                                    acct, ram_pos, float(take_i), p_px, PAIR_SPECS[sym]["maker_fee"], p["name"]
                                )
                                ram_pos.pop("already_filled_on_exchange", None)
                                if p.get("stop_r") is not None:
                                    ns = round(e_px + s_mult * float(p["stop_r"]) * exit_sd, 2)
                                    if (ns > ram_pos["cur_sl"] and ns < cur_px) if is_long else (ns < ram_pos["cur_sl"] and ns > cur_px):
                                        ram_pos["cur_sl"] = ns
                                        ram_pos["sl_price"] = ns
                                        ram_pos["stage"] = max(ram_pos["stage"], 2)
                                        ram_pos["stage_reason"] = f"{p['name']}_FLOOR"
                                        acct.sync_exchange_sl(sym, ram_pos["direction"], ram_pos["lots"], ns, ram_pos)

                            if rem_diff > 0:
                                fallback_px = float(fill_px or ram_pos.get("tp_price") or cur_px)
                                ram_pos["already_filled_on_exchange"] = True
                                self._record_partial_scale_out(
                                    acct, ram_pos, float(rem_diff), fallback_px, PAIR_SPECS[sym]["maker_fee"],
                                    "MAKER_PARTIAL_TP" if not matched_any else "EXCHANGE_PARTIAL_FILL",
                                )
                                ram_pos.pop("already_filled_on_exchange", None)


ENGINE = UnifiedStrategyEngine()


# ==============================================================================
# DELTA EXCHANGE PUBLIC MARKET STREAMER (1s L2 + 1m CANDLE PRIMER FOR 3 ASSETS)
# ==============================================================================
def refresh_product_ids():
    url = f"{ENV_CFG['DELTA_BASE_URL']}/v2/products"
    req = urllib.request.Request(url, headers={"User-Agent": "SyndicateMultiBroker/4.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            res = json.loads(r.read().decode("utf-8"))
            for p in res.get("result", []):
                sym = str(p.get("symbol", ""))
                if sym in PAIR_SPECS and p.get("id"):
                    PAIR_SPECS[sym]["product_id"] = int(p["id"])
            print("[PRODUCTS] Verified Delta Exchange Product IDs:", {s: PAIR_SPECS[s]["product_id"] for s in PAIRS})
    except Exception as e:
        print(f"[PRODUCTS] Using default Product IDs ({e})")


def fetch_l2_orderbook_public(symbol: str, depth: int = 20) -> Dict[str, Any]:
    url = f"{ENV_CFG['DELTA_BASE_URL']}/v2/l2orderbook/{symbol}"
    req = urllib.request.Request(url, headers={"User-Agent": "SyndicateMultiBroker/4.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=2.5) as r:
            res = json.loads(r.read().decode("utf-8"))
            if res.get("success") and "result" in res:
                book = res["result"]
                bids = [
                    [float(b["price"]), float(b["size"])] if isinstance(b, dict) else [float(b[0]), float(b[1])]
                    for b in book.get("buy", [])[:depth]
                ]
                asks = [
                    [float(a["price"]), float(a["size"])] if isinstance(a, dict) else [float(a[0]), float(a[1])]
                    for a in book.get("sell", [])[:depth]
                ]
                return {"success": True, "bids": bids, "asks": asks}
    except Exception:
        pass
    return {"success": False, "bids": [], "asks": []}


def _parse_delta_candles(raw_candles: Any, before_ts: int) -> List[Dict[str, Any]]:
    by_ts: Dict[int, Dict[str, Any]] = {}
    for c in (raw_candles or []):
        if isinstance(c, dict):
            ts_raw = int(c.get("time", c.get("timestamp", 0)) or 0)
            o_v, h_v, l_v, c_v = c.get("open", 0), c.get("high", 0), c.get("low", 0), c.get("close", 0)
            v_v = c.get("volume", 1.0)
        elif isinstance(c, (list, tuple)) and len(c) >= 6:
            ts_raw = int(c[0] or 0)
            o_v, h_v, l_v, c_v, v_v = c[1], c[2], c[3], c[4], c[5]
        else:
            continue
        if ts_raw > 10_000_000_000:
            ts_raw //= 1000
        if ts_raw <= 0 or ts_raw >= before_ts:
            continue
        by_ts[ts_raw] = {
            "time": ts_raw,
            "open": float(o_v),
            "high": float(h_v),
            "low": float(l_v),
            "close": float(c_v),
            "volume": max(1.0, float(v_v or 1.0)),
        }
    return [by_ts[k] for k in sorted(by_ts.keys())]


def prime_initial_candles_for_symbol(symbol: str, chunks: int = 12):
    """
    Primes 24,000 completed 1m candles (~16.7 days) so EMA(7200) (`emam`) and EMA(1440) (`emad`)
    have 99.87% convergence on startup (fixing the 8,000-bar startup truncation drift).
    """
    try:
        cur_min_ts = (int(time.time()) // 60) * 60
        chunk_secs = 2000 * 60
        by_ts: Dict[int, Dict[str, Any]] = {}
        chunk_end = cur_min_ts
        for _ in range(max(1, int(chunks))):
            chunk_start = chunk_end - chunk_secs
            url = (
                f"{ENV_CFG['DELTA_BASE_URL']}/v2/history/candles"
                f"?symbol={symbol}&resolution=1m&start={chunk_start}&end={chunk_end}"
            )
            req = urllib.request.Request(
                url, headers={"User-Agent": "SyndicateMultiBroker/4.0", "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                res = json.loads(r.read().decode("utf-8"))
            for bar in _parse_delta_candles(res.get("result", []), before_ts=cur_min_ts):
                by_ts[bar["time"]] = bar
            chunk_end = chunk_start

        parsed = [by_ts[k] for k in sorted(by_ts.keys())]
        m = HUB.markets[symbol]
        for bar in parsed:
            m.append_bar(bar)
        if parsed:
            m.last_closed_min_ts = int(parsed[-1]["time"])
            m.last_official_vol = max(1.0, float(parsed[-1]["volume"]))
        print(f"[CANDLE PRIMER] {symbol}: Primed {len(m.bars_1m)} official completed 1m bars (${m.last_price:.2f}).")
    except Exception as e:
        print(f"[CANDLE PRIMER] {symbol} notice: {e}")


def fetch_latest_closed_1m_candles(symbol: str, lookback_mins: int = 6) -> List[Dict[str, Any]]:
    cur_min_ts = (int(time.time()) // 60) * 60
    start_ts = cur_min_ts - max(2, int(lookback_mins)) * 60
    url = (
        f"{ENV_CFG['DELTA_BASE_URL']}/v2/history/candles"
        f"?symbol={symbol}&resolution=1m&start={start_ts}&end={cur_min_ts}"
    )
    req = urllib.request.Request(
        url, headers={"User-Agent": "SyndicateMultiBroker/4.0", "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=4.0) as r:
            res = json.loads(r.read().decode("utf-8"))
        return _parse_delta_candles(res.get("result", []), before_ts=cur_min_ts)
    except Exception:
        return []


def stream_3asset_l2_every_second():
    refresh_product_ids()
    for sym in PAIRS:
        prime_initial_candles_for_symbol(sym)
    ENGINE.sync_all_accounts_positions()

    last_sync_ts = time.time()
    sync_future = None
    tick_count = 0
    l2_pool = ThreadPoolExecutor(max_workers=6)

    while True:
        t0 = time.time()
        try:
            now_dt = datetime.now(timezone.utc)
            now_ts = int(now_dt.timestamp())
            now_min_ts = (now_ts // 60) * 60
            prev_closed_ts = now_min_ts - 60
            sec_in_min = now_dt.second

            l2_batch = dict(zip(PAIRS, l2_pool.map(fetch_l2_orderbook_public, PAIRS)))

            need_1m = [sym for sym in PAIRS if HUB.markets[sym].last_closed_min_ts < prev_closed_ts]
            official_1m_batch: Dict[str, List[Dict[str, Any]]] = {}
            if need_1m and sec_in_min >= 2:
                official_1m_batch = dict(zip(need_1m, l2_pool.map(fetch_latest_closed_1m_candles, need_1m)))

            for sym in PAIRS:
                m = HUB.markets[sym]
                l2 = l2_batch.get(sym, {})
                if l2.get("success") and l2["bids"] and l2["asks"]:
                    bids, asks = l2["bids"], l2["asks"]
                    m.update_l2(bids, asks)
                    mid_px = round((float(bids[0][0]) + float(asks[0][0])) / 2.0, 2)
                    m.last_price = mid_px
                    if float(m.l2_depth.get("spread", 0.0)) <= float(PAIR_SPECS[sym]["max_spread"]):
                        ENGINE.on_tick(sym, mid_px)

                    if m.cur_bar is None or now_min_ts != m.cur_min_ts:
                        if m.cur_bar is not None and m.cur_min_ts > 0:
                            fb_bar = dict(m.cur_bar)
                            fb_bar["volume"] = m.last_official_vol
                            m.prev_fallback_bar = fb_bar
                        m.cur_min = now_dt.minute
                        m.cur_min_ts = now_min_ts
                        m.cur_bar = {
                            "time": now_min_ts,
                            "open": mid_px,
                            "high": mid_px,
                            "low": mid_px,
                            "close": mid_px,
                            "volume": m.last_official_vol,
                        }
                    else:
                        m.cur_bar["high"] = max(m.cur_bar["high"], mid_px)
                        m.cur_bar["low"] = min(m.cur_bar["low"], mid_px)
                        m.cur_bar["close"] = mid_px

                if m.last_closed_min_ts < prev_closed_ts and sec_in_min >= 2:
                    closed_bars = official_1m_batch.get(sym, [])
                    if closed_bars:
                        if m.last_closed_min_ts == 0:
                            for b in closed_bars[:-1]:
                                m.append_bar(b)
                            last_b = closed_bars[-1]
                            m.last_closed_min_ts = int(last_b["time"])
                            m.last_official_vol = max(1.0, float(last_b["volume"]))
                            if last_b["time"] == prev_closed_ts:
                                ENGINE.on_bar_close(sym, last_b)
                        else:
                            for b in closed_bars:
                                b_ts = int(b["time"])
                                if m.last_closed_min_ts < b_ts <= prev_closed_ts:
                                    m.last_closed_min_ts = b_ts
                                    m.last_official_vol = max(1.0, float(b["volume"]))
                                    ENGINE.on_bar_close(sym, b)
                    elif sec_in_min >= 12 and m.prev_fallback_bar and m.prev_fallback_bar.get("time") == prev_closed_ts:
                        fb = dict(m.prev_fallback_bar)
                        fb["volume"] = m.last_official_vol
                        m.last_closed_min_ts = prev_closed_ts
                        ENGINE.on_bar_close(sym, fb)

            tick_count += 1
            if tick_count % 30 == 0:
                snap = HUB.get_tickers_snapshot()
                print(
                    f"[1s 3-ASSET L2 RAM] GOLD=${snap['XAUTUSD']['price']:.2f} ({snap['XAUTUSD']['ratio']:.2f}x) | "
                    f"BTC=${snap['BTCUSD']['price']:.2f} ({snap['BTCUSD']['ratio']:.2f}x) | "
                    f"ETH=${snap['ETHUSD']['price']:.2f} ({snap['ETHUSD']['ratio']:.2f}x) | Open={len(HUB.open_positions)}"
                )

            if time.time() - last_sync_ts >= 5.0:
                last_sync_ts = time.time()
                if sync_future is None or sync_future.done():
                    sync_future = HUB.executor.submit(ENGINE.sync_all_accounts_positions)
                HUB.check_ist_daily_summary()

        except Exception as e:
            print(f"[STREAM LOOP NOTICE] {e}")

        elapsed = time.time() - t0
        time.sleep(max(0.05, 1.0 - elapsed))


# ==============================================================================
# HTTP SERVER & MULTI-BROKER API (PORT 5057)
# ==============================================================================
def _build_account_from_payload(body: Dict[str, Any], existing: Optional[MultiBrokerAccountClient] = None) -> MultiBrokerAccountClient:
    merged: Dict[str, Any] = existing.to_storage_dict() if existing else {}
    secret_fields = {"api_key", "api_secret", "webhook_token", "webhook_secret", "tradovate_pass", "tradovate_sec"}
    for k, v in body.items():
        if k in secret_fields:
            if v is not None and str(v).strip() != "":
                merged[k] = str(v).strip()
        elif v is not None:
            merged[k] = v
    if "id" not in merged or not merged["id"]:
        merged["id"] = f"acct_{int(time.time()*1000)}"
    return MultiBrokerAccountClient(merged, ENV_CFG["DELTA_BASE_URL"])


def _canonicalize_symbol(raw_sym: str) -> Optional[str]:
    s = str(raw_sym or "").strip().upper()
    if s in PAIRS:
        return s
    aliases = {
        "GOLD": "XAUTUSD",
        "XAUUSD": "XAUTUSD",
        "MGC": "XAUTUSD",
        "MGC1!": "XAUTUSD",
        "MGCZ6": "XAUTUSD",
        "GC": "XAUTUSD",
        "GC1!": "XAUTUSD",
        "GCZ6": "XAUTUSD",
        "BTC": "BTCUSD",
        "MBT": "BTCUSD",
        "MBT1!": "BTCUSD",
        "MBTZ6": "BTCUSD",
        "ETH": "ETHUSD",
        "MET": "ETHUSD",
        "MET1!": "ETHUSD",
        "METZ6": "ETHUSD",
    }
    return aliases.get(s)


class UnifiedJournalHTTPHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any):
        if args and isinstance(args[0], str) and (
            "/api/public/" in args[0] or "/api/admin/overview" in args[0] or "/api/bridge/poll" in args[0]
        ):
            return
        super().log_message(format, *args)

    def _send_json(self, payload: Any, code: int = 200, extra_headers: Optional[Dict[str, str]] = None):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _is_authenticated(self) -> bool:
        cookie_hdr = self.headers.get("Cookie", "")
        if not cookie_hdr:
            return False
        c = SimpleCookie()
        try:
            c.load(cookie_hdr)
        except Exception:
            return False
        morsel = c.get("syndicate_admin_token")
        if not morsel:
            return False
        tok = morsel.value
        with HUB.lock:
            exp = HUB.active_sessions.get(tok)
            if exp and exp > time.time():
                return True
            HUB.active_sessions.pop(tok, None)
            return False

    def _find_bridge_account(self, tok: str) -> Optional[MultiBrokerAccountClient]:
        t = str(tok or "").strip()
        if not t:
            return None
        with HUB.lock:
            for a in HUB.accounts:
                if a.broker_type == "MT5_API" and a.bridge_token and hmac.compare_digest(a.bridge_token, t):
                    return a
        return None

    def _serve_html_file(self, filename: str):
        fpath = os.path.join(BASE_DIR, filename)
        if not os.path.exists(fpath):
            self.send_error(404, f"{filename} not found")
            return
        with open(fpath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/dashboard", "/index.html", "/live_journal_multibroker.html", "/live_journal_unified.html"):
            self._serve_html_file("live_journal_multibroker.html")
            return

        if path in ("/agents", "/agents/", "/agents.html"):
            self._serve_html_file("agents.html")
            return

        if path == "/api/public/status":
            is_auth = self._is_authenticated()
            with HUB.lock:
                def_acct = HUB.get_default_account()
                pub_open = [
                    dict(p)
                    for (aid, _), p in HUB.open_positions.items()
                    if def_acct is not None and aid == def_acct.id and p.get("broker_type", "DELTA") == "DELTA"
                ]
                pub_acct_meta = {
                    "label": def_acct.label if def_acct else "Primary Delta Account",
                    "broker_type": "DELTA",
                    "sizing_mode": def_acct.sizing_mode if def_acct else "RISK_USD",
                    "risk_usd": round(def_acct.risk_usd, 2) if def_acct else 5.0,
                    "leverage": int(def_acct.leverage) if def_acct else 100,
                    "mode": def_acct.mode if def_acct else "PAPER",
                }
                payload = {
                    "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "port": ENV_CFG["PORT"],
                    "authenticated": is_auth,
                    "engine_paused": HUB.engine_paused,
                    "pair_states": dict(HUB.pair_states),
                    "pair_allow_1m": dict(HUB.pair_allow_1m),
                    "public_account": pub_acct_meta,
                    "tickers": HUB.get_tickers_snapshot(public_only=True),
                    "open_positions": pub_open,
                }
            self._send_json(payload)
            return

        if path == "/api/public/trades":
            with HUB.lock:
                def_acct = HUB.get_default_account()
                if def_acct is None:
                    pub_trades = []
                else:
                    pub_trades = [
                        dict(t) for t in HUB.closed_trades
                        if (t.get("account_id") == def_acct.id and t.get("broker_type", "DELTA") == "DELTA")
                        or (not t.get("account_id") and t.get("broker_type", "DELTA") == "DELTA")
                    ]
            self._send_json(pub_trades)
            return

        if path == "/api/bridge/poll":
            qs = parse_qs(parsed.query)
            tok = qs.get("token", [""])[0] or self.headers.get("X-Bridge-Token", "")
            acct = self._find_bridge_account(tok)
            if not acct:
                self._send_json({"success": False, "error": "Invalid MT5 bridge token"}, code=401)
                return
            acct.last_bridge_heartbeat_ts = time.time()
            cmds = HUB.pop_bridge_cmds(acct.id)
            self._send_json({"success": True, "account_id": acct.id, "commands": cmds, "server_ts": int(time.time())})
            return

        if path == "/api/admin/overview":
            if not self._is_authenticated():
                self._send_json({"error": "Unauthorized"}, code=401)
                return
            qs = parse_qs(parsed.query)
            req_acct = qs.get("account_id", ["DEFAULT"])[0]
            with HUB.lock:
                def_acct = HUB.get_default_account()
                def_id = def_acct.id if def_acct else (HUB.accounts[0].id if HUB.accounts else "acct_default")
                target_id = def_id if req_acct == "DEFAULT" else req_acct
                raw_open = [dict(p) for p in HUB.open_positions.values()]
                raw_closed = [dict(t) for t in HUB.closed_trades]
                today_ist = fmt_ist_date()
                account_summaries: Dict[str, Dict[str, Any]] = {}
                for a in HUB.accounts:
                    a_tr = [
                        t for t in raw_closed
                        if t.get("account_id") == a.id or (not t.get("account_id") and a.id == def_id)
                    ]
                    a_open = [p for p in raw_open if p.get("account_id") == a.id]
                    a_today = [t for t in a_tr if t.get("date_ist") == today_ist]
                    account_summaries[a.id] = {
                        "trades": len(a_tr),
                        "wins": sum(1 for t in a_tr if float(t.get("net_pnl_usd", 0)) > 0),
                        "open_count": len(a_open),
                        "gross_pnl_usd": round(sum(float(t.get("gross_pnl_usd", 0)) for t in a_tr), 2),
                        "fee_usd": round(sum(float(t.get("fee_usd", 0)) for t in a_tr), 2),
                        "net_pnl_usd": round(sum(float(t.get("net_pnl_usd", 0)) for t in a_tr), 2),
                        "today_net_usd": round(sum(float(t.get("net_pnl_usd", 0)) for t in a_today), 2),
                    }
                all_open = list(raw_open)
                all_closed = list(raw_closed)
                if req_acct != "ALL_ACCOUNTS":
                    all_open = [p for p in all_open if p.get("account_id") == target_id]
                    all_closed = [
                        t for t in all_closed
                        if t.get("account_id") == target_id or (not t.get("account_id") and target_id == def_id)
                    ]
                payload = {
                    "accounts": [a.to_public_dict() for a in HUB.accounts],
                    "account_summaries": account_summaries,
                    "default_account_id": def_id,
                    "selected_account_id": target_id if req_acct != "ALL_ACCOUNTS" else "ALL_ACCOUNTS",
                    "open_positions": all_open,
                    "closed_trades": all_closed,
                    "pair_states": dict(HUB.pair_states),
                    "pair_allow_1m": dict(HUB.pair_allow_1m),
                    "tickers": HUB.get_tickers_snapshot(public_only=False, account_id=req_acct),
                    "engine_paused": HUB.engine_paused,
                }
            self._send_json(payload)
            return

        if path == "/api/admin/dispatch_logs":
            if not self._is_authenticated():
                self._send_json({"error": "Unauthorized"}, code=401)
                return
            qs = parse_qs(parsed.query)
            req_acct = qs.get("account_id", [""])[0]
            with HUB.lock:
                logs_list = list(HUB.dispatch_logs)
                if req_acct and req_acct not in ("ALL", "ALL_ACCOUNTS"):
                    def_acct = HUB.get_default_account()
                    tid = (def_acct.id if (req_acct == "DEFAULT" and def_acct) else req_acct)
                    logs_list = [l for l in logs_list if l.get("account_id") == tid]
                self._send_json({
                    "logs": logs_list,
                    "inbound_webhook_token": HUB.inbound_webhook_token,
                    "port": ENV_CFG["PORT"],
                })
            return

        if path == "/api/admin/parity":
            if not self._is_authenticated():
                self._send_json({"error": "Unauthorized"}, code=401)
                return
            self._send_json({
                "XAUTUSD": {
                    "strategy": "GOLD #1 VPS-Exact Master (24k-Bar EMA7200 + 1m Mean-L2 Entry + 5m 4H Judas)",
                    "sl_mode": "Decoupled 0.60x ATR SL (3.00–9.35) + Maker Chaser + 18% GST Fee Sync + Specialist CD",
                    "exits": "Frozen DAVID k-NN + 1H-6H S/R TP & Flip + Bar-Close Stall/Rev + 1s Spread-Guarded Ratchet",
                    "parity_ok": True,
                },
                "BTCUSD": {
                    "strategy": "BTC #1 VPS-Exact Master (Co-Fire Router Loop + M1+M2+M5 + 1m Mean-L2 Entry)",
                    "sl_mode": "3-Tier Multi-SL (FOUR_TIER_RR / TF_SESS_NYCAP / FOUR_TIER_SAFE) + 90m/60m Specialist CD",
                    "exits": "Resting Maker Multi-TP (0.02% + GST) + Bar-Close Stall Lock + 1s Spread-Guarded Ratchet",
                    "parity_ok": True,
                },
                "ETHUSD": {
                    "strategy": "ETH #1 VPS-Exact Master (Co-Fire G3P Router + Wick Shield + 1m Mean-L2 Entry)",
                    "sl_mode": "Wick Shield 1.45x / COFIRE_TIERED / TIERED_HYBRID_M4 + 30m/15m Specialist CD",
                    "exits": "Maker Multi-TP (CHAMP_1 Uncapped) + 50% S/R Wall & Rev Lock + 480m Hold + 1s Ratchet",
                    "parity_ok": True,
                },
            })
            return

        if path == "/api/admin/telegram":
            if not self._is_authenticated():
                self._send_json({"error": "Unauthorized"}, code=401)
                return
            with HUB.lock:
                tg = HUB.telegram
                self._send_json({
                    "bot_token_masked": mask_secret(tg.get("bot_token", ""), "tg_"),
                    "chat_id": tg.get("chat_id", ""),
                    "daily_summary_ist": tg.get("daily_summary_ist", "23:55"),
                    "alert_open": bool(tg.get("alert_open", True)),
                    "alert_partial": bool(tg.get("alert_partial", True)),
                    "alert_close": bool(tg.get("alert_close", True)),
                    "alert_daily": bool(tg.get("alert_daily", True)),
                    "alert_error": bool(tg.get("alert_error", True)),
                })
            return

        self.send_error(404, "Endpoint not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_json_body()

        if path == "/api/auth/login":
            pwd = str(body.get("password", ""))
            with HUB.lock:
                if hmac.compare_digest(pwd, HUB.admin_password):
                    tok = secrets.token_urlsafe(32)
                    HUB.active_sessions[tok] = time.time() + 86400 * 7
                    cookie = f"syndicate_admin_token={tok}; HttpOnly; Path=/; Max-Age=604800; SameSite=Lax"
                    self._send_json({"success": True}, extra_headers={"Set-Cookie": cookie})
                    return
            self._send_json({"success": False, "error": "Invalid admin password"}, code=401)
            return

        if path == "/api/auth/logout":
            cookie = "syndicate_admin_token=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax"
            self._send_json({"success": True}, extra_headers={"Set-Cookie": cookie})
            return

        # MT5 Bridge Report Endpoint (Token Authenticated)
        if path == "/api/bridge/report":
            qs = parse_qs(parsed.query)
            tok = str(body.get("bridge_token") or qs.get("token", [""])[0] or self.headers.get("X-Bridge-Token") or "")
            acct = self._find_bridge_account(tok)
            if not acct:
                self._send_json({"success": False, "error": "Invalid MT5 bridge token"}, code=401)
                return
            acct.last_bridge_heartbeat_ts = time.time()
            with HUB.lock:
                if body.get("wallet_balance_usd") is not None:
                    acct.wallet_balance_usd = round(float(body["wallet_balance_usd"]), 2)
                if body.get("free_margin_usd") is not None:
                    acct.free_margin_usd = round(float(body["free_margin_usd"]), 2)
                rep_sym = _canonicalize_symbol(str(body.get("canonical_symbol") or body.get("symbol") or ""))
                if rep_sym and (acct.id, rep_sym) in HUB.open_positions:
                    pos = HUB.open_positions[(acct.id, rep_sym)]
                    if body.get("fill_price"):
                        try:
                            fpx = round(float(body["fill_price"]), 2)
                            if fpx > 0:
                                pos["broker_fill_price"] = fpx
                        except Exception:
                            pass
                    if body.get("closed") and body.get("close_price"):
                        try:
                            cpx = round(float(body["close_price"]), 2)
                            ENGINE.close_position(acct, pos, cpx, str(body.get("reason") or "MT5_BROKER_EXIT"), already_flat_on_exchange=True)
                        except Exception:
                            pass
            self._send_json({"success": True})
            return

        # Inbound Signal Webhook (Token Protected)
        if path == "/api/webhook/inbound":
            tok = str(body.get("token") or self.headers.get("X-Webhook-Token") or "")
            if not tok or not hmac.compare_digest(tok, HUB.inbound_webhook_token):
                self._send_json({"success": False, "error": "Invalid inbound webhook token"}, code=401)
                return
            canon_sym = _canonicalize_symbol(str(body.get("symbol", "")))
            if not canon_sym:
                self._send_json({"success": False, "error": "Unknown symbol (use XAUTUSD/BTCUSD/ETHUSD/GOLD/MGCZ6)"}, code=400)
                return
            raw_act = str(body.get("action") or body.get("direction") or body.get("data") or "").strip().upper()
            target_acct_id = str(body.get("account_id") or "").strip()

            with HUB.lock:
                ref_px = HUB.markets[canon_sym].last_price or float(body.get("price") or 0.0)
                if ref_px <= 0:
                    ref_px = 4100.0 if canon_sym == "XAUTUSD" else (95000.0 if canon_sym == "BTCUSD" else 3300.0)
                if raw_act in ("CLOSE", "EXIT", "FLATTEN"):
                    closed_cnt = 0
                    for (aid, sym), pos in list(HUB.open_positions.items()):
                        if sym == canon_sym and (not target_acct_id or aid == target_acct_id):
                            acct = next((a for a in HUB.accounts if a.id == aid), None)
                            if acct:
                                ENGINE.close_position(acct, pos, round(ref_px, 2), "INBOUND_WEBHOOK_CLOSE")
                                closed_cnt += 1
                    self._send_json({"success": True, "action": "CLOSE", "symbol": canon_sym, "closed": closed_cnt})
                    return

                direction = "LONG" if raw_act in ("BUY", "LONG") else ("SHORT" if raw_act in ("SELL", "SHORT") else "")
                if not direction:
                    self._send_json({"success": False, "error": "Invalid action/direction (use BUY, SELL, or CLOSE)"}, code=400)
                    return

                default_sl = 5.0 if canon_sym == "XAUTUSD" else (350.0 if canon_sym == "BTCUSD" else 18.0)
                sl_dist = float(body.get("sl_dist") or default_sl)
                tp_dist = float(body.get("tp_dist") or (sl_dist * 2.5))
                accts = [
                    a for a in HUB.accounts
                    if a.mode != "PAUSED"
                    and a.get_pair_cfg(canon_sym).get("enabled", False)
                    and (a.id, canon_sym) not in HUB.open_positions
                    and (not target_acct_id or a.id == target_acct_id)
                ]
                sig = {
                    "symbol": canon_sym,
                    "direction": direction,
                    "tf": "WEBHOOK",
                    "setup_agent": str(body.get("agent") or "INBOUND_WEBHOOK"),
                    "orig_agent": "INBOUND_WEBHOOK",
                    "playbook": "INBOUND_WEBHOOK",
                    "sl_dist": sl_dist,
                    "exit_sd": sl_dist,
                    "sl_tier": "WEBHOOK_SIGNAL",
                    "max_hold_mins": int(body.get("max_hold_mins") or 240),
                    "partials_spec": [],
                    "sr_info": {
                        "tp_price": round(ref_px + (tp_dist if direction == "LONG" else -tp_dist), 2),
                        "tp_dist": tp_dist,
                        "sr_walls": [],
                        "sr1h": 1.0,
                    },
                }
            if accts:
                ENGINE.dispatch_entry_parallel(sig, accts, ref_px)
            self._send_json({"success": True, "action": direction, "symbol": canon_sym, "dispatched_accounts": len(accts)})
            return

        # All remaining POST endpoints require Admin authentication
        if not self._is_authenticated():
            self._send_json({"success": False, "error": "Unauthorized"}, code=401)
            return

        if path == "/api/auth/change_password":
            cur = str(body.get("current_password", ""))
            nw = str(body.get("new_password", ""))
            with HUB.lock:
                if not hmac.compare_digest(cur, HUB.admin_password):
                    self._send_json({"success": False, "error": "Current password incorrect"}, code=400)
                    return
                if len(nw) < 8:
                    self._send_json({"success": False, "error": "New password must be at least 8 characters"}, code=400)
                    return
                HUB.admin_password = nw
                HUB.save_runtime_config()
            self._send_json({"success": True})
            return

        if path == "/api/admin/webhooks/rotate_inbound":
            with HUB.lock:
                HUB.inbound_webhook_token = secrets.token_urlsafe(24)
                HUB.save_runtime_config()
                tok = HUB.inbound_webhook_token
            self._send_json({"success": True, "inbound_webhook_token": tok})
            return

        if path == "/api/admin/pairs/toggle":
            pair = str(body.get("pair", "")).upper()
            with HUB.lock:
                if pair == "ALL":
                    HUB.engine_paused = not HUB.engine_paused
                    for s in PAIRS:
                        HUB.pair_states[s] = "PAUSED" if HUB.engine_paused else "RUNNING"
                elif pair in PAIRS:
                    HUB.pair_states[pair] = "PAUSED" if HUB.pair_states.get(pair) == "RUNNING" else "RUNNING"
                HUB.save_runtime_config()
            self._send_json({"success": True, "engine_paused": HUB.engine_paused, "pair_states": HUB.pair_states})
            return

        if path == "/api/admin/pairs/toggle_1m":
            pair = str(body.get("pair", "")).upper()
            if pair not in PAIRS:
                self._send_json({"success": False, "error": "Invalid pair"}, code=400)
                return
            with HUB.lock:
                if "allow_1m" in body:
                    HUB.pair_allow_1m[pair] = bool(body["allow_1m"])
                else:
                    HUB.pair_allow_1m[pair] = not bool(HUB.pair_allow_1m.get(pair, True))
                HUB.save_runtime_config()
            self._send_json({"success": True, "pair_allow_1m": dict(HUB.pair_allow_1m)})
            return

        if path == "/api/admin/manual_signal":
            canon_sym = _canonicalize_symbol(str(body.get("symbol", "")))
            if not canon_sym:
                self._send_json({"success": False, "error": "Invalid symbol"}, code=400)
                return
            raw_act = str(body.get("action", "")).strip().upper()
            target_acct_id = str(body.get("account_id") or "").strip()
            if target_acct_id in ("ALL", "ALL_ACCOUNTS"):
                target_acct_id = ""
            elif target_acct_id == "DEFAULT":
                def_a = HUB.get_default_account()
                target_acct_id = def_a.id if def_a else ""

            with HUB.lock:
                ref_px = HUB.markets[canon_sym].last_price or float(body.get("price") or 0.0)
                if ref_px <= 0:
                    ref_px = 4120.0 if canon_sym == "XAUTUSD" else (95000.0 if canon_sym == "BTCUSD" else 3300.0)
                if raw_act in ("CLOSE", "EXIT", "FLATTEN"):
                    closed_cnt = 0
                    for (aid, sym), pos in list(HUB.open_positions.items()):
                        if sym == canon_sym and (not target_acct_id or aid == target_acct_id):
                            acct = next((a for a in HUB.accounts if a.id == aid), None)
                            if acct:
                                ENGINE.close_position(acct, pos, round(ref_px, 2), "ADMIN_MANUAL_CLOSE")
                                closed_cnt += 1
                    self._send_json({"success": True, "action": "CLOSE", "symbol": canon_sym, "closed": closed_cnt})
                    return

                direction = "LONG" if raw_act in ("BUY", "LONG") else ("SHORT" if raw_act in ("SELL", "SHORT") else "")
                if not direction:
                    self._send_json({"success": False, "error": "Invalid action (use BUY, SELL, or CLOSE)"}, code=400)
                    return

                default_sl = 5.0 if canon_sym == "XAUTUSD" else (350.0 if canon_sym == "BTCUSD" else 18.0)
                sl_dist = float(body.get("sl_dist") or default_sl)
                tp_dist = float(body.get("tp_dist") or (sl_dist * 2.5))
                accts = [
                    a for a in HUB.accounts
                    if a.mode != "PAUSED"
                    and a.get_pair_cfg(canon_sym).get("enabled", False)
                    and (a.id, canon_sym) not in HUB.open_positions
                    and (not target_acct_id or a.id == target_acct_id)
                ]
                sig = {
                    "symbol": canon_sym,
                    "direction": direction,
                    "tf": "MANUAL",
                    "setup_agent": "ADMIN_MANUAL",
                    "initiator": "ADMIN_MANUAL",
                    "orig_agent": "ADMIN_MANUAL",
                    "playbook": "ADMIN_MANUAL_DISPATCH",
                    "sl_dist": sl_dist,
                    "exit_sd": sl_dist,
                    "sl_tier": "MANUAL_SIGNAL",
                    "max_hold_mins": int(body.get("max_hold_mins") or 240),
                    "partials_spec": [],
                    "sr_info": {
                        "tp_price": round(ref_px + (tp_dist if direction == "LONG" else -tp_dist), 2),
                        "tp_dist": tp_dist,
                        "sr_walls": [],
                        "sr1h": 1.0,
                    },
                }
            if accts:
                ENGINE.dispatch_entry_parallel(sig, accts, ref_px)
            self._send_json({"success": True, "action": direction, "symbol": canon_sym, "dispatched_accounts": len(accts)})
            return

        if path == "/api/admin/accounts/test":
            existing = next((a for a in HUB.accounts if a.id == body.get("id")), None) if body.get("id") else None
            client = _build_account_from_payload(body, existing=existing)

            if client.broker_type == "DELTA":
                if not client.api_key or not client.api_secret:
                    if client.mode == "PAPER":
                        self._send_json({"success": True, "wallet_balance_usd": 100.0, "free_margin_usd": 100.0, "message": "PAPER mode verified"})
                    else:
                        self._send_json({"success": False, "error": "Enter both API Key and API Secret for LIVE mode"})
                    return
                res = client.request("GET", "/v2/wallet/balances", tag="TEST_CONN")
                if not res.get("success"):
                    self._send_json({"success": False, "error": res.get("error", "Delta authentication failed")})
                    return
                bal, avail = client.fetch_balance(force=True)
                self._send_json({"success": True, "wallet_balance_usd": bal, "free_margin_usd": avail})
                return

            if client.broker_type in ("PICKMYTRADE", "WEBHOOK"):
                pcfg = client.get_pair_cfg("XAUTUSD")
                ref_px = HUB.markets["XAUTUSD"].last_price or 4120.0
                if client.broker_type == "PICKMYTRADE":
                    sample = client.build_pickmytrade_payload("OPEN", "XAUTUSD", "LONG", pcfg["lots"], ref_px, 5.0, 15.0, "TEST")
                else:
                    sample = client.build_generic_webhook_payload("OPEN", "XAUTUSD", "LONG", pcfg["lots"], ref_px, 5.0, 15.0, -5.0, "TEST")
                if not client.webhook_url:
                    self._send_json({"success": False, "error": "Enter a valid Webhook URL first", "sample_payload": sample})
                    return
                if not bool(body.get("send_ping", False)):
                    self._send_json({
                        "success": True,
                        "preview_only": True,
                        "message": f"Payload validated for {client.broker_type} ({pcfg['symbol']} x {pcfg['lots']}). Enable 'Send Live Ping' to fire HTTP POST.",
                        "sample_payload": sample,
                    })
                    return
                ping_payload = sample if client.broker_type == "WEBHOOK" else {**sample, "test_ping": True}
                res = client.post_webhook_json(client.webhook_url, ping_payload, secret=client.webhook_secret)
                HUB.record_dispatch_log(
                    client, "TEST_PING", pcfg["symbol"], pcfg["lots"],
                    bool(res.get("success")), int(res.get("http_code", 0)),
                    res.get("error") or res.get("response") or "Webhook Ping",
                )
                self._send_json({
                    "success": bool(res.get("success")),
                    "http_code": res.get("http_code", 0),
                    "response": res.get("response", ""),
                    "error": res.get("error", ""),
                    "sample_payload": ping_payload,
                })
                return

            if client.broker_type == "TRADOVATE_API":
                res = client.test_tradovate_connection()
                self._send_json(res)
                return

            if client.broker_type == "MT5_API":
                if client.mt5_api_url:
                    pcfg = client.get_pair_cfg("XAUTUSD")
                    ping = client.build_mt5_command("PING", "XAUTUSD", "LONG", pcfg["lots"], 5.0, 15.0, reason="TEST")
                    res = client.post_webhook_json(client.mt5_api_url, ping, secret=client.bridge_token)
                    self._send_json({
                        "success": bool(res.get("success")),
                        "http_code": res.get("http_code", 0),
                        "response": res.get("response", ""),
                        "error": res.get("error", ""),
                        "bridge_token": client.bridge_token,
                    })
                    return
                self._send_json({
                    "success": True,
                    "bridge_token": client.bridge_token,
                    "bridge_online": (time.time() - client.last_bridge_heartbeat_ts) < 30.0 if client.last_bridge_heartbeat_ts > 0 else False,
                    "message": f"MT5 Bridge Token ready ({client.bridge_token[:8]}...). Connect SyndicateBridgeEA.mq5 to /api/bridge/poll.",
                })
                return

            self._send_json({"success": True})
            return

        if path in ("/api/admin/accounts/add", "/api/admin/accounts/update"):
            with HUB.lock:
                acct_id = str(body.get("id") or f"acct_{int(time.time()*1000)}")
                existing = next((a for a in HUB.accounts if a.id == acct_id), None)
                req_btype = str(body.get("broker_type") or (existing.broker_type if existing else "DELTA")).upper()
                if req_btype != "DELTA":
                    body["is_default"] = False
                elif body.get("is_default"):
                    for a in HUB.accounts:
                        a.is_default = False
                body["id"] = acct_id
                updated_client = _build_account_from_payload(body, existing=existing)
                if updated_client.broker_type == "DELTA":
                    updated_client.fetch_balance(force=True)
                if existing:
                    if existing.broker_type == "DELTA" and updated_client.broker_type != "DELTA":
                        other_deltas = [a for a in HUB.accounts if a.id != acct_id and a.broker_type == "DELTA"]
                        if not other_deltas:
                            self._send_json(
                                {"success": False, "error": "At least one DELTA account must remain to anchor the Default Public Dashboard."},
                                code=400,
                            )
                            return
                    idx = HUB.accounts.index(existing)
                    HUB.accounts[idx] = updated_client
                else:
                    HUB.accounts.append(updated_client)
                HUB.enforce_delta_default()
                HUB.save_runtime_config()
            self._send_json({"success": True, "account": updated_client.to_public_dict()})
            return

        if path == "/api/admin/accounts/delete":
            del_id = str(body.get("id", ""))
            with HUB.lock:
                if len(HUB.accounts) <= 1:
                    self._send_json({"success": False, "error": "Cannot delete the only configured account"}, code=400)
                    return
                target_del = next((a for a in HUB.accounts if a.id == del_id), None)
                if target_del and target_del.broker_type == "DELTA":
                    rem_deltas = [a for a in HUB.accounts if a.id != del_id and a.broker_type == "DELTA"]
                    if not rem_deltas:
                        self._send_json(
                            {"success": False, "error": "Keep at least one DELTA account as the Default Public Account (you can set its Mode to PAUSED or PAPER)."},
                            code=400,
                        )
                        return
                HUB.accounts = [a for a in HUB.accounts if a.id != del_id]
                HUB.enforce_delta_default()
                HUB.save_runtime_config()
            self._send_json({"success": True})
            return

        if path == "/api/admin/accounts/apply_risk_all":
            amt = float(body.get("risk_usd", 0))
            if amt <= 0:
                self._send_json({"success": False, "error": "Risk amount must be positive"}, code=400)
                return
            with HUB.lock:
                for a in HUB.accounts:
                    a.risk_usd = amt
                HUB.save_runtime_config()
            self._send_json({"success": True})
            return

        if path == "/api/admin/positions/close":
            acct_id = str(body.get("account_id", ""))
            sym = str(body.get("symbol", "")).upper()
            with HUB.lock:
                pos = HUB.open_positions.get((acct_id, sym))
                acct = next((a for a in HUB.accounts if a.id == acct_id), None)
            if not pos or not acct:
                self._send_json({"success": False, "error": "Active position not found"}, code=404)
                return
            px = HUB.markets[sym].last_price or pos["entry_price"]
            ENGINE.close_position(acct, pos, round(px, 2), "ADMIN_FORCE_CLOSE")
            self._send_json({"success": True})
            return

        if path == "/api/admin/telegram":
            with HUB.lock:
                if body.get("bot_token"):
                    tok_clean = str(body["bot_token"]).strip()
                    if tok_clean.lower().startswith("bot") and ":" in tok_clean:
                        tok_clean = tok_clean[3:].strip()
                    HUB.telegram["bot_token"] = tok_clean
                if "chat_id" in body:
                    HUB.telegram["chat_id"] = str(body.get("chat_id", "")).strip()
                if "daily_summary_ist" in body:
                    HUB.telegram["daily_summary_ist"] = str(body.get("daily_summary_ist", "23:55")).strip()
                for k in ("alert_open", "alert_partial", "alert_close", "alert_daily", "alert_error"):
                    if k in body:
                        HUB.telegram[k] = bool(body[k])
                HUB.save_runtime_config()
                masked = mask_secret(HUB.telegram.get("bot_token", ""), "tg_")
            self._send_json({"success": True, "bot_token_masked": masked})
            return

        if path == "/api/admin/telegram/test":
            with HUB.lock:
                changed = False
                if body.get("bot_token"):
                    tok_clean = str(body["bot_token"]).strip()
                    if tok_clean.lower().startswith("bot") and ":" in tok_clean:
                        tok_clean = tok_clean[3:].strip()
                    HUB.telegram["bot_token"] = tok_clean
                    changed = True
                if body.get("chat_id"):
                    HUB.telegram["chat_id"] = str(body["chat_id"]).strip()
                    changed = True
                if body.get("daily_summary_ist"):
                    HUB.telegram["daily_summary_ist"] = str(body["daily_summary_ist"]).strip()
                    changed = True
                for k in ("alert_open", "alert_partial", "alert_close", "alert_daily", "alert_error"):
                    if k in body:
                        HUB.telegram[k] = bool(body[k])
                        changed = True
                if changed:
                    HUB.save_runtime_config()
                masked = mask_secret(HUB.telegram.get("bot_token", ""), "tg_")
            ok, msg = HUB.send_telegram(
                "✅ <b>TRADING JOURNAL · MULTI-BROKER 3-ASSET FLOOR (PORT 5057)</b>\n"
                "Telegram alert connection verified! Live multi-broker alerts &amp; IST daily summaries are active.",
                kind="test",
            )
            self._send_json({"success": ok, "error": None if ok else msg, "bot_token_masked": masked})
            return

        self._send_json({"success": False, "error": "Unknown endpoint"}, code=404)


def start_http_server():
    server = ThreadingHTTPServer(("0.0.0.0", ENV_CFG["PORT"]), UnifiedJournalHTTPHandler)
    print(f"[MULTI-BROKER 3-ASSET DAEMON] Live Journal active at http://0.0.0.0:{ENV_CFG['PORT']}/")
    print(f"[MULTI-BROKER 3-ASSET DAEMON] Agents Architecture at http://0.0.0.0:{ENV_CFG['PORT']}/agents")
    server.serve_forever()


if __name__ == "__main__":
    t_srv = threading.Thread(target=start_http_server, daemon=True)
    t_srv.start()

    t_l2 = threading.Thread(target=stream_3asset_l2_every_second, daemon=True)
    t_l2.start()

    print("=" * 78)
    print(f"  🏛️ TRADING TEAMS — MULTI-BROKER 3-ASSET LIVE ENGINE (PORT {ENV_CFG['PORT']})")
    print("  Strategy: MERGED 👑 #1 VPS-EXACT 3-Strategy Master (Gold + BTC + ETH)")
    print("  Public  : Default DELTA Account Only (Secondary Accounts Admin-Login Only)")
    print("  Streams : 24,000-Bar 1m RAM + 1m Mean-L2 Entry + 1s Spread-Guarded Exits")
    print("  Isolate : Ports 5050, 5055, and 5056 remain 100% untouched")
    print("=" * 78)
    while True:
        time.sleep(1.0)
