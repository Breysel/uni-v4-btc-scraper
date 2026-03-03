# Uniswap V4 WBTC/USDC Swap Scraper (Arbitrum One)

Historical swap event scraper for the Uniswap V4 WBTC/USDC 0.05% pool on Arbitrum One.
Outputs CSV files in the same format as the V3 WETH/USDC scraper for backtesting.

## How it works

- Scrapes `Swap` events from the V4 **PoolManager** singleton contract
- Filters by computed **poolId** (keccak256 of PoolKey) — only gets swaps for this specific pool
- Auto-discovers the correct USDC variant (native vs bridged) and tickSpacing at startup
- Outputs daily CSV partitions with exactly-once semantics and crash-safe resume

## Setup

```bash
# Clone
git clone https://github.com/Breysel/uni-v4-btc-scraper.git
cd uni-v4-btc-scraper

# Install dependencies (or let the script auto-install them)
pip install requests web3 eth_abi
```

## Usage

```bash
# Edit START_DT / END_DT in the script to set your date range, then:
python scraper_v4_wbtc_usdc.py
```

The scraper will:
1. Probe RPC connectivity (primary + fallback)
2. Auto-discover the pool (tries native USDC first, then USDC.e, with multiple tickSpacings)
3. Scrape daily partitions from `START_DT` to `END_DT`
4. Save CSV files to `arb_univ4_wbtc_usdc_005/`

Safe to interrupt with Ctrl+C and rerun — it resumes from the last completed chunk.

## Configuration

Edit the constants at the top of `scraper_v4_wbtc_usdc.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_PRIMARY` | publicnode.com | Primary Arbitrum RPC endpoint |
| `RPC_FALLBACK` | arb1.arbitrum.io | Fallback RPC endpoint |
| `START_DT` | 2025-01-31 | Backfill start date (V4 launch) |
| `END_DT` | 2026-03-03 | Backfill end date |
| `MIN_NOTIONAL_USDC` | 0.0 | Minimum swap size to include (USDC) |

## Output format

Daily CSV files: `arb_univ4_wbtc_usdc_005/swaps_YYYY_MM_DD.csv`

```
timestamp,block_number,tx_hash,log_index,sender,recipient,amount0,amount1,sqrtPriceX96,liquidity,tick
```

- **recipient** is always `0x0000000000000000000000000000000000000000` (V4 Swap events have no recipient field)
- **amount0/amount1** are raw signed integers (not decimal-adjusted)
- Format is identical to the V3 WETH/USDC scraper output

## V4 vs V3 differences

| Aspect | V3 | V4 |
|--------|----|----|
| Contract | Individual pool contract | PoolManager singleton |
| Event filter | By pool address | By poolId topic |
| Swap event | 5 data fields | 6 data fields (extra `fee`) |
| recipient | From indexed topic | Not available (zero address) |
| amounts | int256 | int128 (ABI-padded to 32 bytes) |

## Dependencies

- `requests` — HTTP/RPC calls
- `web3` — keccak256, checksum addresses
- `eth_abi` — ABI encoding for PoolKey → poolId computation
