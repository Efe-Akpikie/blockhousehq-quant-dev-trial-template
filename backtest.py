import json
import os
import time
import math
import pandas as pd
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from collections import defaultdict

# Config / Constants
ORDER_SIZE = 5000
KAFKA_TOPIC = 'mock_l1_stream'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092'] 
VENUE_FEES = defaultdict(lambda: 0.0)
VENUE_REBATES = defaultdict(lambda: 0.0)

# --- Allocation logic 
def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue):
    executed = 0
    cash_spent = 0.0
    for i, alloc in enumerate(split):
        exe = min(alloc, venues[i]['ask_sz_00'])
        executed += exe
        cash_spent += exe * (venues[i]['ask_px_00'] + venues[i]['fee'])
        maker_rebate = max(alloc - exe, 0) * venues[i]['rebate']
        cash_spent -= maker_rebate
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_pen = theta_queue * (underfill + overfill)
    cost_pen = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_pen + cost_pen

def allocate_allow_underfill(order_size, venues, lambda_over, lambda_under, theta_queue):
    """
    Brute-force allocation in increments of 'step', allowing underfill.
    Returns best_split and best_cost.
    """
    step = 100
    splits = [[]]
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v]['ask_sz_00'])
            # increments of step
            for q in range(0, max_v + 1, step):
                new_splits.append(alloc + [q])
            if max_v % step != 0:
                new_splits.append(alloc + [max_v])
        splits = new_splits
    best_cost = float('inf')
    best_split = None
    for alloc in splits:
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost

# --- Baseline strategies returning executed and avg_px ---
def best_ask_strategy(venues, order_size):
    # buy from cheapest ask until exhausted or order_size
    venues_sorted = sorted(venues, key=lambda v: v['ask_px_00'])
    remaining = order_size
    cash = 0.0
    executed = 0
    for v in venues_sorted:
        take = min(remaining, v['ask_sz_00'])
        cash += take * v['ask_px_00']
        executed += take
        remaining -= take
        if remaining <= 0:
            break
    avg_px = cash / executed if executed else 0.0
    return cash, executed, avg_px

def twap_strategy_with_schedule(venue_snapshots, order_size, interval_sec=60):
    """
    TWAP baseline handling underfill, recording per-window execution schedule.
    Returns:
      total_cash, executed_size, avg_px, execution_schedule
    where execution_schedule is a list of dicts: {'ts': pd.Timestamp, 'cost': float, 'executed': int}
    """
    execution_schedule = []
    total_cash = 0.0
    executed = 0
    remaining_order = order_size

    if not venue_snapshots:
        return 0.0, 0, 0.0, execution_schedule

    # Ensure sorted by ts
    venue_snapshots = sorted(venue_snapshots, key=lambda s: s['ts'])
    # Build DataFrame for grouping by window
    df = pd.DataFrame({
        'ts': [snap['ts'] for snap in venue_snapshots],
        'venues': [snap['venues'] for snap in venue_snapshots]
    })
    df = df.sort_values('ts').reset_index(drop=True)
    t0 = df['ts'].iloc[0]
    t_end = df['ts'].iloc[-1]
    total_seconds = (t_end - t0).total_seconds()
    # number of windows, using ceil to capture final partial window
    n_intervals = max(1, int(math.ceil((total_seconds + 1e-9) / interval_sec)))
    # Compute seconds since start and window idx
    df['seconds_since_start'] = (df['ts'] - t0).dt.total_seconds()
    df['window_idx'] = (df['seconds_since_start'] // interval_sec).astype(int)

    base_chunk = order_size // n_intervals
    remainder = order_size - base_chunk * n_intervals
    last_snapshot = None

    for i in range(n_intervals):
        if remaining_order <= 0:
            break
        # pick snapshot for this window: last in that window if exists, else last known
        group = df[df['window_idx'] == i]
        if not group.empty:
            snap = group.iloc[-1]
            last_snapshot = snap
        else:
            if last_snapshot is None:
                # no data in this window and no previous snapshot: skip
                continue
            snap = last_snapshot
        venues = snap['venues']
        # determine chunk for this window
        chunk = base_chunk + (1 if i < remainder else 0)
        # but if remaining_order smaller, use remaining_order
        chunk = min(chunk, remaining_order)
        # fill chunk by best-ask logic on this snapshot
        venues_sorted = sorted(venues, key=lambda v: v['ask_px_00'])
        to_fill = chunk
        cash_this = 0.0
        exec_this = 0
        for v in venues_sorted:
            if to_fill <= 0:
                break
            available = v['ask_sz_00']
            take = min(to_fill, available)
            cash_this += take * v['ask_px_00']
            exec_this += take
            to_fill -= take
        # Record schedule for this window
        execution_schedule.append({
            'ts': snap['ts'],
            'cost': cash_this,
            'executed': exec_this
        })
        total_cash += cash_this
        executed += exec_this
        # Decrease remaining_order by chunk even if underfilled, to avoid infinite loop
        remaining_order -= chunk

    avg_px = total_cash / executed if executed else 0.0
    return total_cash, executed, avg_px, execution_schedule

def vwap_strategy(venues, order_size):
    total_sz = sum(v['ask_sz_00'] for v in venues)
    if total_sz <= 0:
        return 0.0, 0, 0.0
    total_cash = 0.0
    executed = 0
    for v in venues:
        take = int(order_size * v['ask_sz_00'] / total_sz)
        take = min(take, v['ask_sz_00'])
        total_cash += take * v['ask_px_00']
        executed += take
    avg_px = total_cash / executed if executed else 0.0
    return total_cash, executed, avg_px

def plot_cumulative_cost(timestamps, costs_per_interval, labels=None, output_prefix="cumulative_execution_cost"):
    """
    Plot cumulative execution cost over time.
    - timestamps: list of pd.Timestamp
    - costs_per_interval: list of floats
    - labels: if provided, a list of labels for multiple series; but for single series, None.
      If multiple series, pass timestamps and costs_per_interval as lists of lists, e.g.:
         timestamps=[ts_list1, ts_list2], costs_per_interval=[costs1, costs2]
         labels=['Optimized','TWAP']
    - Saves:
         output_prefix + ".png"
         output_prefix + ".pdf"
    """
    # Single series case
    multiple = False
    if labels is not None:
        # Expect lists of series
        if isinstance(timestamps[0], list) or isinstance(costs_per_interval[0], list):
            multiple = True
    if not multiple:
        # wrap into lists
        timestamps = [timestamps]
        costs_per_interval = [costs_per_interval]
        labels = labels or ["Series"]
    # Build DataFrames and plot
    plt.figure()
    for idx, (ts_list, cost_list, lbl) in enumerate(zip(timestamps, costs_per_interval, labels)):
        df = pd.DataFrame({
            'timestamp': ts_list,
            'cost_per_interval': cost_list
        }).sort_values('timestamp').reset_index(drop=True)
        df['cumulative_cost'] = df['cost_per_interval'].cumsum()
        plt.plot(df['timestamp'], df['cumulative_cost'], marker='o', label=lbl)
    plt.xlabel('Timestamp')
    plt.ylabel('Cumulative Execution Cost')
    plt.title('Cumulative Execution Cost Over Time')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    # Ensure output directory exists
    out_png = f"{output_prefix}.png"
    out_pdf = f"{output_prefix}.pdf"
    try:
        plt.savefig(out_png)
        plt.savefig(out_pdf)
        print(f"Saved cumulative cost plots: {out_png}, {out_pdf}")
    except Exception as e:
        print(f"Failed to save cumulative cost plots: {e}")
    finally:
        plt.close()

# --- Main Backtest ---
def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000  # exit loop if no messages for 5 seconds
    )

    snapshots = []  # list of {'ts': pd.Timestamp, 'venues': [...]}
    venues_by_ts = defaultdict(list)
    msg_count = 0

    print("Starting Kafka consumer to collect snapshots...")
    try:
        for msg in consumer:
            msg_count += 1
            snap = msg.value
            ts_str = snap.get('ts_event')
            if ts_str is None:
                continue
            try:
                ts_dt = pd.to_datetime(ts_str, utc=True)
            except Exception:
                continue
            venues_by_ts[ts_str].append(snap)
            if len(venues_by_ts[ts_str]) == 1:
                snapshots.append({'ts': ts_dt, 'venues': []})
            snapshots[-1]['venues'].append({
                'publisher_id': snap.get('publisher_id'),
                'ask_px_00': snap.get('ask_px_00'),
                'ask_sz_00': snap.get('ask_sz_00'),
                'fee': VENUE_FEES[snap.get('publisher_id')],
                'rebate': VENUE_REBATES[snap.get('publisher_id')]
            })
            if ts_dt.strftime('%H:%M:%S') == '13:45:14':
                print(f"Reached target timestamp 13:45:14 after {msg_count} messages")
                break
    except Exception as e:
        print("Error during consuming:", e)
    finally:
        consumer.close()
        print(f"Kafka consumer closed after reading {msg_count} messages, collected {len(snapshots)} timestamp groups")

    if not snapshots:
        print("No snapshots collected; exiting backtest.")
        return

    # Prepare numeric venues from first snapshot
    first_snapshot = snapshots[0]
    first_venues_raw = first_snapshot['venues']
    valid_venues = []
    for v in first_venues_raw:
        try:
            valid_venues.append({
                'ask_px_00': float(v['ask_px_00']),
                'ask_sz_00': int(v['ask_sz_00']),
                'fee': float(v.get('fee', 0.0)),
                'rebate': float(v.get('rebate', 0.0))
            })
        except Exception:
            continue
    if not valid_venues:
        print("No valid venues in the first snapshot; cannot run allocation.")
        return

    total_available = sum(v['ask_sz_00'] for v in valid_venues)
    print(f"Total available size in first snapshot: {total_available} (ORDER_SIZE={ORDER_SIZE})")

    # Prepare result variables
    executed = 0
    total_cash = 0.0
    avg_fill_px = 0.0
    underfill = ORDER_SIZE
    best_split = []
    lambda_over = lambda_under = theta_queue = None

    # For plotting: record optimized schedule (static first-snapshot) as single entry
    execution_schedule_opt = []

    if total_available <= 0:
        print("No liquidity available in first snapshot; cannot execute any size.")
        # optimized: no execution; schedule stays empty
    elif total_available < ORDER_SIZE:
        print(f"Warning: total_available < ORDER_SIZE; allocating all available ({total_available}).")
        # Allocate all available
        best_split = [v['ask_sz_00'] for v in valid_venues]
        executed = total_available
        total_cash = sum(v['ask_sz_00'] * (v['ask_px_00'] + v['fee']) for v in valid_venues)
        avg_fill_px = total_cash / executed if executed else 0.0
        underfill = ORDER_SIZE - executed
        # Record single-point schedule
        execution_schedule_opt.append({
            'ts': first_snapshot['ts'],
            'cost': total_cash,
            'executed': executed
        })
    else:
        # total_available >= ORDER_SIZE: parameter search
        print("Sufficient liquidity for full ORDER_SIZE; beginning parameter search...")
        best = None
        best_params = None
        for lo in [0.1, 0.4, 1.0]:
            for lu in [0.1, 0.6, 1.0]:
                for tq in [0.1, 0.3, 0.5]:
                    split, cost = allocate_allow_underfill(ORDER_SIZE, valid_venues, lo, lu, tq)
                    if split is not None:
                        if best is None or cost < best:
                            best = cost
                            best_params = (lo, lu, tq, split)
        if best_params is None:
            print("No feasible allocation found even allowing underfill. (Unexpected if total_available>=ORDER_SIZE)")
            return
        lambda_over, lambda_under, theta_queue, best_split = best_params
        print(f"Best parameters: lambda_over={lambda_over}, lambda_under={lambda_under}, theta_queue={theta_queue}")
        # Compute executed and cash
        executed = sum(min(best_split[i], valid_venues[i]['ask_sz_00']) for i in range(len(valid_venues)))
        total_cash = sum(min(best_split[i], valid_venues[i]['ask_sz_00']) * (valid_venues[i]['ask_px_00'] + valid_venues[i]['fee'])
                         for i in range(len(valid_venues)))
        avg_fill_px = total_cash / executed if executed else 0.0
        underfill = ORDER_SIZE - executed
        # Record single-point schedule for optimized baseline
        execution_schedule_opt.append({
            'ts': first_snapshot['ts'],
            'cost': total_cash,
            'executed': executed
        })

    print(f"Allocation split: {best_split}")
    print(f"Executed size: {executed}/{ORDER_SIZE}, underfill: {underfill}, avg_fill_px={avg_fill_px:.4f}")

    # Baseline strategies on first snapshot and TWAP with schedule
    ba_cash, ba_exec, ba_avg = best_ask_strategy(valid_venues, ORDER_SIZE)
    tw_cash, tw_exec, tw_avg, execution_schedule_twap = twap_strategy_with_schedule(snapshots, ORDER_SIZE)
    vw_cash, vw_exec, vw_avg = vwap_strategy(valid_venues, ORDER_SIZE)

    print(f"Best-ask baseline: executed={ba_exec}, total_cash={ba_cash:.2f}, avg_fill_px={ba_avg:.4f}")
    print(f"TWAP baseline: executed={tw_exec}, total_cash={tw_cash:.2f}, avg_fill_px={tw_avg:.4f}")
    print(f"VWAP baseline: executed={vw_exec}, total_cash={vw_cash:.2f}, avg_fill_px={vw_avg:.4f}")

    # Savings in basis points vs. baselines; only if executed>0
    savings_vs_baselines_bps = {}
    if executed > 0 and ba_exec > 0:
        savings_vs_baselines_bps['best_ask'] = round((ba_cash - total_cash) / ba_cash * 10000, 2)
    else:
        savings_vs_baselines_bps['best_ask'] = None
    if executed > 0 and tw_exec > 0:
        savings_vs_baselines_bps['twap'] = round((tw_cash - total_cash) / tw_cash * 10000, 2)
    else:
        savings_vs_baselines_bps['twap'] = None
    if executed > 0 and vw_exec > 0:
        savings_vs_baselines_bps['vwap'] = round((vw_cash - total_cash) / vw_cash * 10000, 2)
    else:
        savings_vs_baselines_bps['vwap'] = None

    result = {
        'best_parameters': {
            'lambda_over': lambda_over,
            'lambda_under': lambda_under,
            'theta_queue': theta_queue
        },
        'optimized': {
            'executed_size': executed,
            'underfill': underfill,
            'total_cash': round(total_cash, 2),
            'avg_fill_px': round(avg_fill_px, 4)
        },
        'baselines': {
            'best_ask': {'executed_size': ba_exec, 'total_cash': round(ba_cash, 2), 'avg_fill_px': round(ba_avg, 4)},
            'twap': {'executed_size': tw_exec, 'total_cash': round(tw_cash, 2), 'avg_fill_px': round(tw_avg, 4)},
            'vwap': {'executed_size': vw_exec, 'total_cash': round(vw_cash, 2), 'avg_fill_px': round(vw_avg, 4)}
        },
        'savings_vs_baselines_bps': savings_vs_baselines_bps
    }

    print("Backtest result:")
    print(json.dumps(result, indent=2))

    # Write result to JSON
    output_filename = 'results.json'
    try:
        with open(output_filename, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Backtest result written to {output_filename}")
    except Exception as e:
        print(f"Failed to write result file: {e}")
        print("Result:", json.dumps(result, indent=2))

    # --- Plot cumulative cost ---
    # For optimized static (single snapshot), execution_schedule_opt may have one entry or be empty.
    # For TWAP, we have execution_schedule_twap with multiple entries.
    # Build lists for plotting:
    # Optimized: if schedule empty, skip or plot single point.
    if execution_schedule_opt:
        ts_opt = [entry['ts'] for entry in execution_schedule_opt]
        cost_opt = [entry['cost'] for entry in execution_schedule_opt]
    else:
        ts_opt, cost_opt = [], []
    ts_twap = [entry['ts'] for entry in execution_schedule_twap]
    cost_twap = [entry['cost'] for entry in execution_schedule_twap]

    # Plot both series on same chart if data exists
    plot_dir = "figures"
    os.makedirs(plot_dir, exist_ok=True)
    # Plot only TWAP if optimized empty; else plot both
    if ts_opt:
        plot_cumulative_cost(
            timestamps=[ts_opt, ts_twap],
            costs_per_interval=[cost_opt, cost_twap],
            labels=['Optimized', 'TWAP'],
            output_prefix=os.path.join(plot_dir, "cumulative_cost_comparison")
        )
    else:
        plot_cumulative_cost(
            timestamps=ts_twap,
            costs_per_interval=cost_twap,
            labels=None,
            output_prefix=os.path.join(plot_dir, "cumulative_cost_twap")
        )

if __name__ == '__main__':
    main()
