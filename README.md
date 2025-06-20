# Smart Order Router Backtest (Cont-Kukanov)

## Overview
This project simulates and backtests a Smart Order Router (SOR) using the Cont & Kukanov cost model. It streams market data via Kafka, benchmarks against basic execution strategies, and is deployable on AWS EC2.

## Files
- `kafka_producer.py`: Streams `l1_day.csv` snapshots to Kafka topic `mock_l1_stream`.
- `backtest.py`: Consumes the stream, applies allocator logic, benchmarks, and prints JSON results.
- `requirements.txt`: Python dependencies.
- `allocator_pseudocode.txt`: Provided allocator logic.
- `l1_day.csv`: Market data (not included in repo).

## Approach
- **Producer**: Reads and filters `l1_day.csv` for the window 13:36:32 to 13:45:14 UTC. Streams per-timestamp, per-venue snapshots to Kafka, simulating real-time pacing.
- **Backtest**: Consumes snapshots, applies the static Cont-Kukanov allocator, and benchmarks against Best Ask, TWAP, and VWAP. Performs grid search over `lambda_over`, `lambda_under`, and `theta_queue`.
- **Output**: Prints a JSON summary with best parameters, optimized results, baselines, and savings in basis points.

## Parameter Tuning
- Grid search over:
  - `lambda_over`: [0.1, 0.4, 1.0]
  - `lambda_under`: [0.1, 0.6, 1.0]
  - `theta_queue`: [0.1, 0.3, 0.5]
- Selects the configuration with the lowest expected cost.

## EC2 & Kafka Setup
1. **Launch EC2 Instance**
   - Use Ubuntu 20.04, t3.micro or t4g.micro.
   - Open ports 22 (SSH) and 9092 (Kafka) in security group.
2. **Install Dependencies**
   ```sh
   sudo apt update && sudo apt install -y python3-pip openjdk-11-jre-headless
   pip3 install pandas numpy kafka-python
   ```
3. **Install Kafka & Zookeeper**
   ```sh
   wget https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz
   tar -xzf kafka_2.13-3.5.1.tgz
   cd kafka_2.13-3.5.1
   # Start Zookeeper
   bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
   # Start Kafka
   bin/kafka-server-start.sh -daemon config/server.properties
   ```
4. **Clone Repo & Run**
   ```sh
   git clone <your_repo_url>
   cd <repo>
   python3 kafka_producer.py
   python3 backtest.py
   ```

## Notes
- Adjust `KAFKA_BOOTSTRAP_SERVERS` in scripts if running remotely.

## Output Example
```
{
  "best_parameters": {"lambda_over": 0.4, "lambda_under": 0.6, "theta_queue": 0.3},
  "optimized": {"total_cash": 248750, "avg_fill_px": 49.75},
  "baselines": {
    "best_ask": {"total_cash": 250000, "avg_fill_px": 50.00},
    "twap": {"total_cash": 251000, "avg_fill_px": 50.20},
    "vwap": {"total_cash": 249800, "avg_fill_px": 49.96}
  },
  "savings_vs_baselines_bps": {"best_ask": 5.0, "twap": 15.0, "vwap": 4.2}
}
``` 