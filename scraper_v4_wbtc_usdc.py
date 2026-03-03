# scraper_v4_wbtc_usdc.py
# ------------------------------------------------------------
# Uniswap V4 Swap Downloader (Arbitrum) — WBTC/USDC 0.05%
#
# V4-specific:
#   - Scrapes PoolManager singleton (not individual pool contracts)
#   - Filters Swap events by poolId (indexed bytes32 topic)
#   - Computes poolId from PoolKey at startup, auto-discovers
#     USDC variant and tickSpacing via preflight probing
#   - V4 Swap event has 6 data fields (amount0, amount1,
#     sqrtPriceX96, liquidity, tick, fee) — fee not written to CSV
#   - No recipient in V4 — column filled with zero address
#
# Long-haul safe (same guarantees as V3 scraper):
#   - Constant saving per chunk
#   - Exactly-once semantics via inflight_chunk + truncate on restart
#   - Handles 10k log truncation, block range errors, rate limits
#   - Strict timestamps (never silently drops rows)
#   - Adaptive chunking + dual RPC failover
# ------------------------------------------------------------

import os
import sys
import time
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Iterable

# --- deps (auto-install if missing) ---
try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

try:
    from web3 import Web3
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "web3"])
    from web3 import Web3

try:
    import eth_abi
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "eth_abi"])
    import eth_abi


# ================================
# CONFIG
# ================================
SCRIPT_VERSION = "1.0-v4-wbtc"

RPC_PRIMARY = "https://arbitrum-one-rpc.publicnode.com"
RPC_FALLBACK = "https://arb1.arbitrum.io/rpc"

OUT_DIR = "arb_univ4_wbtc_usdc_005"
PARTITION = "day"  # "day" or "month"

# Uniswap V4 launched on Arbitrum around Jan 31 2025.
# Adjust START_DT to your desired backfill start.
START_DT = datetime(2025, 1, 31, 0, 0, 0, tzinfo=timezone.utc)
END_DT   = datetime(2026, 3, 3, 23, 59, 59, tzinfo=timezone.utc)

# Chunk sizing (safe defaults for providers that limit block span)
CHUNK_START = 2000
CHUNK_MAX   = 10000
CHUNK_MIN   = 100
CHUNK_ABS_MIN = 1

MAX_RETRIES_PER_CHUNK = 5
BACKOFF_BASE_SEC = 2.0

# eth_getLogs truncation guard
LOG_RESULT_LIMIT = 10000

# Timestamp batching
TS_BATCH_PRIMARY = 300
TS_BATCH_FALLBACK = 50
TS_TIMEOUT = 60
TS_MAX_RETRIES = 5
TS_SLEEP_BETWEEN_BATCHES = 0.05
TS_CACHE_LIMIT = 200_000

# Disk safety
FSYNC_EVERY_CHUNK = True

# Optional dust filter (USDC)
MIN_NOTIONAL_USDC = 0.0

# Auto-switch logs RPC if primary is being throttled hard
SWITCH_TO_FALLBACK_ON_RATE_LIMIT_HITS = 4
SWITCH_TO_FALLBACK_ON_SERVER_ERROR_HITS = 4


# ================================
# Constants: V4 PoolManager + Tokens
# ================================
POOL_MANAGER = Web3.to_checksum_address("0x360E68faCcca8cA495c1B759Fd9EEe466db9FB32")

WBTC_ARB = Web3.to_checksum_address("0x2f2a2543B76A4166549F7aaB2e75Bef0aeFc5B0f")
USDC_NATIVE_ARB = Web3.to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
USDC_E_ARB = Web3.to_checksum_address("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8")

POOL_FEE = 500        # 0.05%
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# V4 Swap event: Swap(bytes32 indexed id, address indexed sender,
#   int128 amount0, int128 amount1, uint160 sqrtPriceX96,
#   uint128 liquidity, int24 tick, uint24 fee)
# topic0 = keccak256("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
SWAP_TOPIC0_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"


# ================================
# CSV schema (matches V3 scraper exactly)
# ================================
COLUMNS = [
    "timestamp",
    "block_number",
    "tx_hash",
    "log_index",
    "sender",
    "recipient",
    "amount0",
    "amount1",
    "sqrtPriceX96",
    "liquidity",
    "tick",
]
HEADER_LINE = (",".join(COLUMNS) + "\n").encode("utf-8")


# ================================
# PoolId computation
# ================================
def compute_pool_id(currency0: str, currency1: str, fee: int, tick_spacing: int, hooks: str) -> str:
    """
    Compute V4 poolId = keccak256(abi.encode(currency0, currency1, fee, tickSpacing, hooks))
    PoolKey is 5 x 32 bytes (160 bytes total).
    currency0 must be < currency1 (sorted by address).
    """
    c0 = Web3.to_checksum_address(currency0)
    c1 = Web3.to_checksum_address(currency1)
    h = Web3.to_checksum_address(hooks)

    if int(c0, 16) >= int(c1, 16):
        raise ValueError(f"currency0 ({c0}) must be < currency1 ({c1})")

    encoded = eth_abi.encode(
        ['address', 'address', 'uint24', 'int24', 'address'],
        [c0, c1, fee, tick_spacing, h]
    )
    pool_id = Web3.keccak(encoded)
    return "0x" + pool_id.hex()


# ================================
# requests.Session with pooling
# ================================
_SESSION = requests.Session()
try:
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
    _SESSION.mount("https://", adapter)
    _SESSION.mount("http://", adapter)
except Exception:
    pass


# ================================
# RPC helpers
# ================================
class RpcError(RuntimeError):
    def __init__(self, msg: str, status: Optional[int] = None, body: Optional[str] = None):
        super().__init__(msg)
        self.status = status
        self.body = body or ""
        self.rpc_code: Optional[int] = None


def _rpc_post_with_retries(rpc_url: str, payload, timeout: int = 120, max_retries: int = 10):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = _SESSION.post(rpc_url, json=payload, timeout=timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    raise RpcError(f"Non-JSON response: {e}", status=200, body=r.text[:2000])

            body = ""
            try:
                body = r.text[:2000]
            except Exception:
                body = "<no body>"

            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(60.0, BACKOFF_BASE_SEC * (1.8 ** (attempt - 1)))
                time.sleep(sleep_s)
                continue

            raise RpcError(f"HTTP {r.status_code} from RPC", status=r.status_code, body=body)

        except requests.RequestException as e:
            last_err = e
            sleep_s = min(60.0, BACKOFF_BASE_SEC * (1.8 ** (attempt - 1)))
            time.sleep(sleep_s)

    if isinstance(last_err, Exception):
        raise RpcError(f"RPC request failed after retries: {last_err}")
    raise RpcError("RPC request failed after retries")


def rpc_call(rpc_url: str, method: str, params: list, timeout: int = 120, max_retries: int = 10):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = _rpc_post_with_retries(rpc_url, payload, timeout=timeout, max_retries=max_retries)

    if isinstance(resp, dict) and resp.get("error"):
        err_data = resp["error"]
        code = err_data.get("code") if isinstance(err_data, dict) else None
        msg = str(err_data)

        e = RpcError(f"RPC error for {method}: {msg}", status=200, body=msg)
        e.rpc_code = code
        raise e

    return resp.get("result")


def rpc_call_batch(rpc_url: str, calls: List[Tuple[str, list]], timeout: int = 120, max_retries: int = 10):
    payload = []
    for i, (method, params) in enumerate(calls, 1):
        payload.append({"jsonrpc": "2.0", "id": i, "method": method, "params": params})
    resp = _rpc_post_with_retries(rpc_url, payload, timeout=timeout, max_retries=max_retries)

    if isinstance(resp, dict) and resp.get("error"):
        raise RpcError(f"RPC batch error: {resp['error']}", status=200, body=str(resp["error"]))

    if not isinstance(resp, list):
        raise RpcError("Unexpected batch response type", status=200, body=str(resp)[:500])

    return resp


def rpc_get_logs_v4(rpc_url: str, from_block: int, to_block: int, pool_id: str, timeout: int = 180):
    """
    Fetch Swap events from PoolManager filtered by poolId.
    topics[0] = Swap event signature, topics[1] = poolId
    """
    flt = {
        "fromBlock": hex(int(from_block)),
        "toBlock": hex(int(to_block)),
        "address": POOL_MANAGER,
        "topics": [SWAP_TOPIC0_V4, pool_id],
    }
    return rpc_call(rpc_url, "eth_getLogs", [flt], timeout=timeout, max_retries=MAX_RETRIES_PER_CHUNK)


def looks_like_block_range_too_large(text: str) -> bool:
    t = (text or "").lower()
    return ("block range is too large" in t) or ("range too large" in t)


def looks_like_limit_or_timeout(text: str) -> bool:
    t = (text or "").lower()
    keys = [
        "query returned more than", "too many results", "response size", "result window",
        "limit", "exceed", "exceeded",
        "timeout", "time out", "timed out",
        "payload too large",
        "block range is too large", "range too large",
    ]
    return any(k in t for k in keys)


def robust_get_logs(rpc_url: str, from_b: int, to_b: int, pool_id: str) -> Tuple[Optional[List[dict]], bool, str]:
    """
    Returns (logs, must_shrink, reason)
    """
    try:
        logs = rpc_get_logs_v4(rpc_url, from_b, to_b, pool_id, timeout=180)
    except RpcError as e:
        msg = (str(e) + " " + (e.body or "")).lower()

        if e.rpc_code == -32062 or looks_like_block_range_too_large(msg):
            return None, True, "block_range_too_large"

        if e.status == 429:
            return None, True, "rate_limit"

        if looks_like_limit_or_timeout(msg):
            if looks_like_block_range_too_large(msg):
                return None, True, "block_range_too_large"
            return None, True, "limit_or_timeout"

        if e.status in (500, 502, 503, 504):
            return None, True, "server_error"

        if (
            e.status is None
            or "request failed after retries" in msg
            or "connection" in msg
            or "read timed out" in msg
        ):
            return None, True, "rpc_unreachable"

        raise

    # Silent truncation guard
    if isinstance(logs, list) and len(logs) >= LOG_RESULT_LIMIT:
        return None, True, f"possible_truncation_{len(logs)}"

    return logs, False, "ok"


# ================================
# Timestamp cache + STRICT fetch
# ================================
_BLOCK_TS_CACHE: Dict[int, int] = {}

def _batch_fetch_ts(rpc_url: str, blocks: List[int], batch_size: int) -> None:
    if not blocks:
        return
    i = 0
    bs = int(batch_size)
    while i < len(blocks):
        chunk = blocks[i:i + bs]
        calls = [("eth_getBlockByNumber", [hex(b), False]) for b in chunk]
        resp = rpc_call_batch(rpc_url, calls, timeout=TS_TIMEOUT, max_retries=TS_MAX_RETRIES)

        for item in resp:
            if not isinstance(item, dict):
                continue
            res = item.get("result")
            if not res:
                continue
            bn_hex = res.get("number")
            ts_hex = res.get("timestamp")
            if bn_hex and ts_hex:
                _BLOCK_TS_CACHE[int(bn_hex, 16)] = int(ts_hex, 16)

        time.sleep(TS_SLEEP_BETWEEN_BATCHES)
        i += len(chunk)


def get_block_timestamps_strict(blocks: List[int]) -> Dict[int, int]:
    if len(_BLOCK_TS_CACHE) > TS_CACHE_LIMIT:
        _BLOCK_TS_CACHE.clear()

    want = sorted(set(int(b) for b in blocks))
    if not want:
        return {}

    missing = [b for b in want if b not in _BLOCK_TS_CACHE]
    if missing:
        try:
            _batch_fetch_ts(RPC_PRIMARY, missing, TS_BATCH_PRIMARY)
        except Exception:
            pass

    missing2 = [b for b in want if b not in _BLOCK_TS_CACHE]
    if missing2:
        try:
            _batch_fetch_ts(RPC_FALLBACK, missing2, TS_BATCH_FALLBACK)
        except Exception:
            pass

    for b in want:
        if b in _BLOCK_TS_CACHE:
            continue

        for url in (RPC_FALLBACK, RPC_PRIMARY):
            try:
                res = rpc_call(url, "eth_getBlockByNumber", [hex(b), False], timeout=TS_TIMEOUT, max_retries=TS_MAX_RETRIES)
                if res and res.get("timestamp") and res.get("number"):
                    _BLOCK_TS_CACHE[int(res["number"], 16)] = int(res["timestamp"], 16)
                    break
            except Exception:
                continue

        if b not in _BLOCK_TS_CACHE:
            raise RuntimeError(f"FATAL: No timestamp for block {b}")

    return {b: _BLOCK_TS_CACHE[b] for b in want}


# ================================
# Utils
# ================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)

def decode_topic_address(topic_hex: str) -> str:
    t = topic_hex[2:] if topic_hex.startswith("0x") else topic_hex
    return Web3.to_checksum_address("0x" + t[-40:])

def signed_int256_from_32bytes(hex64: str) -> int:
    v = int(hex64, 16)
    if v & (1 << 255):
        v -= 1 << 256
    return v

def unsigned_int_from_32bytes(hex64: str) -> int:
    return int(hex64, 16)

def chunk_hex(data_hex: str, idx: int) -> str:
    return data_hex[idx * 64:(idx + 1) * 64]

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def ensure_csv_header(path: str) -> None:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "ab") as f:
            f.write(HEADER_LINE)
            f.flush()
            if FSYNC_EVERY_CHUNK:
                os.fsync(f.fileno())

def read_first_line(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            line = f.readline()
        return line.decode("utf-8", errors="ignore").strip()
    except Exception:
        return None

def truncate_file(path: str, byte_offset: int) -> None:
    with open(path, "rb+") as f:
        f.truncate(byte_offset)
        f.flush()
        if FSYNC_EVERY_CHUNK:
            os.fsync(f.fileno())

def write_lines_binary(f, lines: List[bytes]) -> None:
    for ln in lines:
        f.write(ln)

def format_row(ts: int, blk: int, txh: str, logi: int, sender: str, recipient: str,
               a0: int, a1: int, sqrtP: int, liq: int, tick: int) -> bytes:
    s = f"{ts},{blk},{txh},{logi},{sender},{recipient},{a0},{a1},{sqrtP},{liq},{tick}\n"
    return s.encode("utf-8")


# ================================
# Partitioning
# ================================
def day_start(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=timezone.utc)

def month_start(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc)

def next_month(dt: datetime) -> datetime:
    y, m = dt.year, dt.month
    if m == 12:
        return datetime(y + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return datetime(y, m + 1, 1, 0, 0, 0, tzinfo=timezone.utc)

def iter_partitions_forward(start_dt: datetime, end_dt: datetime, mode: str) -> Iterable[Tuple[str, datetime, datetime]]:
    cur = start_dt
    while cur <= end_dt:
        if mode == "month":
            ms = month_start(cur)
            nm = next_month(ms)
            p_start = cur
            p_end = min(end_dt, nm - timedelta(seconds=1))
            tag = f"{ms.year:04d}_{ms.month:02d}"
            cur = nm
        else:
            ds = day_start(cur)
            p_start = cur
            p_end = min(end_dt, ds + timedelta(days=1) - timedelta(seconds=1))
            tag = f"{ds.year:04d}_{ds.month:02d}_{ds.day:02d}"
            cur = ds + timedelta(days=1)
        yield tag, p_start, p_end


# ================================
# Block boundary search
# ================================
def _read_rpc_urls() -> List[str]:
    urls: List[str] = []
    for url in (RPC_PRIMARY, RPC_FALLBACK):
        if url and url not in urls:
            urls.append(url)
    return urls

def get_head_block_primary() -> int:
    last_err: Optional[Exception] = None
    for url in _read_rpc_urls():
        try:
            return int(rpc_call(url, "eth_blockNumber", [], timeout=60, max_retries=10), 16)
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    raise RuntimeError("No RPC read URLs configured")

def get_block_ts_single(block_number: int) -> int:
    last_err: Optional[Exception] = None
    for url in _read_rpc_urls():
        try:
            res = rpc_call(url, "eth_getBlockByNumber", [hex(int(block_number)), False], timeout=TS_TIMEOUT, max_retries=TS_MAX_RETRIES)
            if res and res.get("timestamp"):
                return int(res["timestamp"], 16)
            last_err = RuntimeError(f"Empty block response from RPC: {url}")
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    raise RuntimeError("No RPC read URLs configured")

def find_first_block_at_or_after_ts(target_ts: int) -> int:
    low = 0
    high = get_head_block_primary()
    closest = high
    failures = 0
    attempts = 0
    while low <= high:
        attempts += 1
        mid = (low + high) // 2
        try:
            ts = get_block_ts_single(mid)
            failures = 0
        except Exception as e:
            failures += 1
            if failures == 1 or failures % 3 == 0:
                print(f"   ... boundary lookup retry {failures}/10 (target_ts={target_ts}, mid={mid}) err={type(e).__name__}")
            if failures > 10:
                raise RuntimeError("Too many failures during block binary search")
            time.sleep(0.8)
            continue

        if ts < target_ts:
            low = mid + 1
        else:
            closest = mid
            high = mid - 1
        if attempts % 8 == 0:
            print(f"   ... boundary search progress (target_ts={target_ts}, low={low}, high={high}, closest={closest})")
    return int(closest)

def find_last_block_at_or_before_ts(target_ts: int) -> int:
    b = find_first_block_at_or_after_ts(target_ts + 1)
    return max(0, int(b) - 1)

def compute_partition_bounds(part_start: datetime, part_end: datetime) -> Tuple[int, int]:
    start_block = find_first_block_at_or_after_ts(int(part_start.timestamp()))
    end_block = find_last_block_at_or_before_ts(int(part_end.timestamp()))
    return int(start_block), int(end_block)


# ================================
# Preflight: discover pool
# ================================
def discover_pool(rpc_url: str, head_block: int) -> Tuple[str, str, int, str]:
    """
    Attempt to find the WBTC/USDC V4 pool by probing for recent Swap events
    with different PoolKey combinations (USDC variant, tickSpacing).

    Returns (pool_id, usdc_address, tick_spacing, hooks) on success.
    Raises RuntimeError if no pool found.
    """
    usdc_variants = [
        ("native USDC", USDC_NATIVE_ARB),
        ("bridged USDC.e", USDC_E_ARB),
    ]
    tick_spacings = [10, 60, 1, 100]
    hooks = ZERO_ADDRESS

    # Search recent blocks in small windows (public RPCs limit to ~50k blocks)
    # Try several windows stepping backward, each ≤ 40k blocks
    PROBE_WINDOW = 40_000
    PROBE_STEPS = 15  # 15 × 40k = 600k blocks ≈ ~2.5 days on Arbitrum

    for usdc_label, usdc_addr in usdc_variants:
        for ts in tick_spacings:
            # Sort tokens: currency0 < currency1
            c0, c1 = sorted([WBTC_ARB.lower(), usdc_addr.lower()])
            c0 = Web3.to_checksum_address(c0)
            c1 = Web3.to_checksum_address(c1)

            try:
                pid = compute_pool_id(c0, c1, POOL_FEE, ts, hooks)
            except Exception as e:
                print(f"   ... poolId computation failed for {usdc_label} ts={ts}: {e}")
                continue

            print(f"   ... probing {usdc_label} tickSpacing={ts} poolId={pid[:18]}...")

            # Step backward through probe windows
            total_found = 0
            for step in range(PROBE_STEPS):
                to_b = head_block - step * PROBE_WINDOW
                from_b = max(0, to_b - PROBE_WINDOW + 1)
                if to_b < 0:
                    break
                try:
                    flt = {
                        "fromBlock": hex(from_b),
                        "toBlock": hex(to_b),
                        "address": POOL_MANAGER,
                        "topics": [SWAP_TOPIC0_V4, pid],
                    }
                    logs = rpc_call(rpc_url, "eth_getLogs", [flt], timeout=60, max_retries=3)
                except Exception as e:
                    print(f"   ... query failed (window {step}): {type(e).__name__}: {e}")
                    continue

                if logs and len(logs) > 0:
                    total_found += len(logs)
                    print(f"   ✅ FOUND! {usdc_label} tickSpacing={ts} → {total_found} events")
                    print(f"   poolId = {pid}")
                    return pid, usdc_addr, ts, hooks

            if total_found == 0:
                print(f"   ... no events found for {usdc_label} ts={ts}")

    raise RuntimeError(
        "FATAL: Could not find WBTC/USDC V4 pool on Arbitrum.\n"
        "Tried native USDC + USDC.e with tickSpacings [10, 60, 1, 100] and hooks=0x0.\n"
        "Possible causes:\n"
        "  - Pool does not exist yet\n"
        "  - Pool uses non-standard hooks address\n"
        "  - Pool uses a different fee tier\n"
        "Check https://app.uniswap.org or Arbiscan for the correct pool parameters."
    )


# ================================
# Partition completion check
# ================================
def is_partition_complete(prog_path: str, pool_id: str, part_start: datetime, part_end: datetime) -> bool:
    if not os.path.exists(prog_path):
        return False
    try:
        with open(prog_path, "r", encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        return False

    if not isinstance(progress, dict):
        return False
    if progress.get("pool_id") != pool_id:
        return False
    if progress.get("part_start") != part_start.isoformat():
        return False
    if progress.get("part_end") != part_end.isoformat():
        return False

    if bool(progress.get("complete", False)):
        return True

    try:
        return int(progress.get("last_scanned_block", -1)) >= int(progress.get("end_block", 0))
    except Exception:
        return False


# ================================
# Downloader per partition
# ================================
def download_partition(
    pool_id: str,
    usdc_addr: str,
    part_start: datetime,
    part_end: datetime,
    csv_path: str,
    prog_path: str,
    start_block_override: Optional[int] = None,
    end_block_override: Optional[int] = None,
) -> None:
    TAG = "wbtc_usdc"

    # header check
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        first = read_first_line(csv_path)
        if first and first.strip() != ",".join(COLUMNS):
            raise RuntimeError(f"Header mismatch in {csv_path}. Found: {first}")

    progress = None
    if os.path.exists(prog_path):
        try:
            with open(prog_path, "r", encoding="utf-8") as f:
                progress = json.load(f)
        except Exception:
            progress = None

    # If interrupted mid-chunk: truncate
    if progress and progress.get("inflight_chunk"):
        off = int(progress["inflight_chunk"].get("file_offset", -1))
        if off >= 0 and os.path.exists(csv_path):
            print(f"\n\U0001f9f9 [{TAG}] inflight -> truncating {os.path.basename(csv_path)} to {off}")
            truncate_file(csv_path, off)
        progress["inflight_chunk"] = None
        atomic_write_json(prog_path, progress)

    # Resume / init
    if (
        progress
        and progress.get("pool_id") == pool_id
        and progress.get("part_start") == part_start.isoformat()
        and progress.get("part_end") == part_end.isoformat()
    ):
        start_block = int(progress["start_block"])
        end_block = int(progress["end_block"])
        last_scanned = int(progress.get("last_scanned_block", start_block - 1))
        rows_written = int(progress.get("rows_written_total", 0))
        chunk_size = int(progress.get("chunk_size", CHUNK_START))
        adaptive_ceiling = int(progress.get("adaptive_ceiling", CHUNK_MAX))
        complete = bool(progress.get("complete", False))

        if complete or last_scanned >= end_block:
            print(f"\u2705 [{TAG}] Partition already complete: {os.path.basename(csv_path)}")
            return

        chunk_size = min(chunk_size, adaptive_ceiling)

        current_b = last_scanned + 1
        log_rpc = progress.get("log_rpc", "primary")
        print(f"\U0001f504 [{TAG}] Resume from block {current_b} (log_rpc={log_rpc}, chunk={chunk_size}, ceiling={adaptive_ceiling})")
    else:
        if start_block_override is None or end_block_override is None:
            print(f"\n\U0001f50d [{TAG}] Finding blocks for {part_start.date()}...")
            start_block = find_first_block_at_or_after_ts(int(part_start.timestamp()))
            end_block = find_last_block_at_or_before_ts(int(part_end.timestamp()))
        else:
            start_block = int(start_block_override)
            end_block = int(end_block_override)

        current_b = int(start_block)
        rows_written = 0
        chunk_size = int(CHUNK_START)
        adaptive_ceiling = int(CHUNK_MAX)
        log_rpc = "primary"

        progress = dict(
            version=SCRIPT_VERSION,
            pool_id=pool_id,
            pool_manager=POOL_MANAGER,
            pool_name="WBTC/USDC 0.05% (V4)",
            usdc_address=usdc_addr,
            part_start=part_start.isoformat(),
            part_end=part_end.isoformat(),
            start_block=int(start_block),
            end_block=int(end_block),
            last_scanned_block=int(start_block - 1),
            rows_written_total=int(rows_written),
            chunk_size=int(chunk_size),
            adaptive_ceiling=int(adaptive_ceiling),
            updated_at=utc_now_iso(),
            out_file=csv_path,
            inflight_chunk=None,
            complete=False,
            log_rpc=log_rpc,
        )
        atomic_write_json(prog_path, progress)

    if int(end_block) < int(start_block):
        progress["complete"] = True
        progress["updated_at"] = utc_now_iso()
        atomic_write_json(prog_path, progress)
        print(f"\u2705 [{TAG}] Empty partition (no blocks).")
        return

    ensure_csv_header(csv_path)

    total_blocks = int(end_block) - int(start_block) + 1
    logs_rpc_url = RPC_PRIMARY if progress.get("log_rpc", "primary") == "primary" else RPC_FALLBACK
    logs_rpc_label = "primary" if logs_rpc_url == RPC_PRIMARY else "fallback"

    # Determine which token index is USDC for dust filtering
    # currency0 (lower address) is token0 in the CSV
    c0 = min(WBTC_ARB.lower(), usdc_addr.lower())
    usdc_is_token0 = (c0 == usdc_addr.lower())
    USDC_DEC = 6

    consecutive_rate_limits = 0
    consecutive_server_errors = 0

    with open(csv_path, "ab") as f_out:
        while current_b <= int(end_block):
            to_b = min(int(current_b) + int(chunk_size) - 1, int(end_block))

            # getLogs (shrink on trunc/limit)
            shrink_tries = 0
            while True:
                logs, must_shrink, reason = robust_get_logs(logs_rpc_url, int(current_b), int(to_b), pool_id)

                if logs is not None:
                    consecutive_rate_limits = 0
                    consecutive_server_errors = 0
                    break

                if must_shrink:
                    if reason == "rate_limit":
                        consecutive_rate_limits += 1
                    elif reason in ("server_error", "rpc_unreachable"):
                        consecutive_server_errors += 1

                    if logs_rpc_url == RPC_PRIMARY:
                        if consecutive_rate_limits >= SWITCH_TO_FALLBACK_ON_RATE_LIMIT_HITS:
                            logs_rpc_url = RPC_FALLBACK
                            logs_rpc_label = "fallback"
                            progress["log_rpc"] = "fallback"
                            atomic_write_json(prog_path, progress)
                            print(f"\n\U0001f501 [{TAG}] switching getLogs to FALLBACK (rate limit streak={consecutive_rate_limits})")
                        elif consecutive_server_errors >= SWITCH_TO_FALLBACK_ON_SERVER_ERROR_HITS:
                            logs_rpc_url = RPC_FALLBACK
                            logs_rpc_label = "fallback"
                            progress["log_rpc"] = "fallback"
                            atomic_write_json(prog_path, progress)
                            print(f"\n\U0001f501 [{TAG}] switching getLogs to FALLBACK (server error streak={consecutive_server_errors})")

                    if reason == "block_range_too_large":
                        adaptive_ceiling = min(adaptive_ceiling, max(CHUNK_ABS_MIN, int(chunk_size) - 1))

                    shrink_tries += 1
                    new_size = max(CHUNK_ABS_MIN, int(int(chunk_size) * 0.5))
                    floor = CHUNK_MIN if int(chunk_size) > CHUNK_MIN else CHUNK_ABS_MIN
                    chunk_size = max(floor, new_size)
                    chunk_size = min(chunk_size, adaptive_ceiling)

                    to_b = min(int(current_b) + int(chunk_size) - 1, int(end_block))
                    sleep_s = min(15.0, BACKOFF_BASE_SEC * (1.4 ** (shrink_tries - 1)))

                    print(f"\n\u26a0\ufe0f [{TAG}] shrink ({reason}) -> chunk={chunk_size}, ceiling={adaptive_ceiling}, sleep {sleep_s:.1f}s, retry {current_b}-{to_b}")
                    time.sleep(sleep_s)
                    continue

                raise RuntimeError("Unexpected getLogs state")

            if logs is None:
                logs = []

            # timestamps strict
            blocks_touched: List[int] = []
            for lg in logs:
                bn_hex = lg.get("blockNumber")
                if bn_hex and isinstance(bn_hex, str) and bn_hex.startswith("0x"):
                    blocks_touched.append(int(bn_hex, 16))
            if blocks_touched:
                ts_attempt = 0
                while True:
                    try:
                        ts_map = get_block_timestamps_strict(blocks_touched)
                        break
                    except Exception as e:
                        ts_attempt += 1
                        sleep_s = min(60.0, 0.8 * (1.5 ** (ts_attempt - 1)))
                        bmin, bmax = min(blocks_touched), max(blocks_touched)
                        print(
                            f"\n\u26a0\ufe0f [{TAG}] timestamp fetch retry {ts_attempt} "
                            f"for blocks {bmin}-{bmax} ({type(e).__name__}); sleep {sleep_s:.1f}s"
                        )
                        time.sleep(sleep_s)
            else:
                ts_map = {}

            out_lines: List[bytes] = []
            for lg in logs:
                bn_hex = lg.get("blockNumber")
                if not bn_hex:
                    continue
                blk = int(bn_hex, 16)

                ts = int(ts_map.get(blk, 0))
                if ts <= 0:
                    raise RuntimeError(f"FATAL: Missing timestamp for block {blk}")

                topics = lg.get("topics", [])
                # V4: topics[0]=event sig, topics[1]=poolId, topics[2]=sender
                sender = decode_topic_address(topics[2]) if len(topics) > 2 else ""
                recipient = ZERO_ADDRESS  # V4 has no recipient

                tx_hash = lg.get("transactionHash", "")
                log_index = int(lg.get("logIndex", "0x0"), 16)

                data_hex = lg.get("data", "")
                if data_hex.startswith("0x"):
                    data_hex = data_hex[2:]
                # V4 has 6 x 32-byte fields (384 hex chars)
                if len(data_hex) < 64 * 6:
                    continue

                a0 = signed_int256_from_32bytes(chunk_hex(data_hex, 0))
                a1 = signed_int256_from_32bytes(chunk_hex(data_hex, 1))
                sqrtP = unsigned_int_from_32bytes(chunk_hex(data_hex, 2))
                liq = unsigned_int_from_32bytes(chunk_hex(data_hex, 3))
                tick = signed_int256_from_32bytes(chunk_hex(data_hex, 4))
                # data[5] = fee (uint24) — not written to CSV

                # dust filter in USDC
                usdc_amt_raw = abs(a0) if usdc_is_token0 else abs(a1)
                usdc_amt = usdc_amt_raw / (10 ** USDC_DEC)
                if MIN_NOTIONAL_USDC > 0 and usdc_amt < MIN_NOTIONAL_USDC:
                    continue

                out_lines.append(format_row(ts, blk, tx_hash, log_index, sender, recipient, a0, a1, sqrtP, liq, tick))

            # EXACTLY-ONCE write
            file_offset = f_out.tell()
            progress["inflight_chunk"] = dict(
                fromBlock=int(current_b),
                toBlock=int(to_b),
                file_offset=int(file_offset),
                logs=int(len(logs)),
                rows_planned=int(len(out_lines)),
            )
            progress["updated_at"] = utc_now_iso()
            atomic_write_json(prog_path, progress)

            if out_lines:
                write_lines_binary(f_out, out_lines)

            f_out.flush()
            if FSYNC_EVERY_CHUNK:
                os.fsync(f_out.fileno())

            rows_written += len(out_lines)

            progress["last_scanned_block"] = int(to_b)
            progress["rows_written_total"] = int(rows_written)
            progress["chunk_size"] = int(chunk_size)
            progress["adaptive_ceiling"] = int(adaptive_ceiling)
            progress["inflight_chunk"] = None
            progress["updated_at"] = utc_now_iso()
            progress["log_rpc"] = logs_rpc_label
            atomic_write_json(prog_path, progress)

            done_blocks = (int(to_b) - int(start_block) + 1)
            pct = (done_blocks / total_blocks) * 100.0
            sys.stdout.write(
                f"\r\u2705 [{TAG}] {os.path.basename(csv_path)} | {to_b}/{end_block} ({pct:6.2f}%) "
                f"| logs={len(logs):6d} | wrote={len(out_lines):6d} | chunk={chunk_size:5d} | ceiling={adaptive_ceiling:5d} | total_rows={rows_written} | rpc={logs_rpc_label}"
            )
            sys.stdout.flush()

            # adaptive growth (respect ceiling)
            if len(logs) < 1500:
                proposed = int(chunk_size) + 700
                chunk_size = min(proposed, adaptive_ceiling)
            elif len(logs) > 9000:
                chunk_size = max(CHUNK_MIN, int(int(chunk_size) * 0.7))

            current_b = int(to_b) + 1

    progress = progress or {}
    progress["complete"] = True
    progress["updated_at"] = utc_now_iso()
    atomic_write_json(prog_path, progress)
    print(f"\n\u2705 [{TAG}] Partition complete: {os.path.basename(csv_path)} rows={progress.get('rows_written_total', 0)}")


# ================================
# Main
# ================================
def main():
    global RPC_PRIMARY, RPC_FALLBACK
    ensure_dir(OUT_DIR)

    print(f"\U0001f539 scraper version: {SCRIPT_VERSION}")
    print(f"\U0001f539 Target: Uniswap V4 WBTC/USDC 0.05% on Arbitrum One")
    print(f"\U0001f539 PoolManager: {POOL_MANAGER}")
    print(f"\U0001f50c RPC PRIMARY : {RPC_PRIMARY}")
    print(f"\U0001f50c RPC FALLBACK: {RPC_FALLBACK}")

    print("\u23f3 Probing RPC connectivity...")
    startup_head: Optional[int] = None
    startup_chain_id: Optional[int] = None
    startup_url: Optional[str] = None
    last_startup_err: Optional[Exception] = None

    for label, url in [("primary", RPC_PRIMARY), ("fallback", RPC_FALLBACK)]:
        try:
            print(f"   ... trying {label} RPC")
            startup_head = int(rpc_call(url, "eth_blockNumber", [], timeout=12, max_retries=2), 16)
            startup_chain_id = int(rpc_call(url, "eth_chainId", [], timeout=12, max_retries=2), 16)
            startup_url = url
            break
        except Exception as e:
            last_startup_err = e
            print(f"   ... {label} RPC unavailable ({type(e).__name__})")

    if startup_url is None or startup_head is None or startup_chain_id is None:
        if last_startup_err:
            raise RuntimeError(f"No reachable RPC endpoint at startup: {last_startup_err}")
        raise RuntimeError("No reachable RPC endpoint at startup")

    if startup_url != RPC_PRIMARY:
        RPC_PRIMARY, RPC_FALLBACK = startup_url, RPC_PRIMARY
        print("\u26a0\ufe0f Using FALLBACK as PRIMARY for this run (original PRIMARY unavailable).")

    print(f"\U0001f539 Connected. chain_id={startup_chain_id} | Head block={startup_head}")

    if startup_chain_id != 42161:
        print(f"\u26a0\ufe0f WARNING: Expected Arbitrum One (chain_id=42161), got {startup_chain_id}")

    if END_DT < START_DT:
        print("\u2705 Nothing to do.")
        return

    # ── Preflight: discover pool ──────────────────────────────────
    print("\n==================== POOL DISCOVERY ====================")
    pool_id, usdc_addr, tick_spacing, hooks = discover_pool(RPC_PRIMARY, startup_head)

    usdc_label = "native USDC" if usdc_addr.lower() == USDC_NATIVE_ARB.lower() else "bridged USDC.e"
    print(f"   Pool:         WBTC/USDC 0.05% (V4)")
    print(f"   USDC variant: {usdc_label} ({usdc_addr})")
    print(f"   tickSpacing:  {tick_spacing}")
    print(f"   hooks:        {hooks}")
    print(f"   poolId:       {pool_id}")

    if usdc_addr.lower() == USDC_E_ARB.lower():
        print("\n\u26a0\ufe0f  WARNING: This pool uses bridged USDC.e, NOT native USDC!")
        print("   Your wallet holds native USDC. You may need to bridge or swap.")
        print("   Continuing with scrape (data collection only)...\n")

    print("=============================================================\n")

    # ── Main scraping loop ────────────────────────────────────────
    bounds_cache: Dict[str, Tuple[int, int]] = {}

    for tag, p_start, p_end in iter_partitions_forward(START_DT, END_DT, PARTITION):
        print(f"\n==================== {tag} ====================")

        prog_path = os.path.join(OUT_DIR, f"swaps_{tag}.progress.json")
        if is_partition_complete(prog_path, pool_id, p_start, p_end):
            print(f"\u2705 [wbtc_usdc] Partition already complete: swaps_{tag}.csv")
            continue

        if tag not in bounds_cache:
            print(f"\u23f3 Computing block bounds for {tag} ({p_start.isoformat()} -> {p_end.isoformat()})")
            sblk, eblk = compute_partition_bounds(p_start, p_end)
            bounds_cache[tag] = (sblk, eblk)
            print(f"\u2705 Block bounds for {tag}: {sblk}..{eblk}")

        sblk, eblk = bounds_cache[tag]

        csv_path = os.path.join(OUT_DIR, f"swaps_{tag}.csv")

        download_partition(
            pool_id, usdc_addr,
            p_start, p_end,
            csv_path, prog_path,
            start_block_override=sblk,
            end_block_override=eblk,
        )

    print("\n\U0001f389 Done. Safe to rerun \u2014 resumes per day.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n\U0001f6d1 Stopped by user. Safe to rerun \u2014 it will resume.")
