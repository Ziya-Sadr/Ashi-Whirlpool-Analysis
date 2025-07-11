# Analysis of Ashigaru Whirlpool: Unspent Capacity & Anonymity Sets

**`ashidetector.py`** is a Python-based tool for tracing the lineage of Whirlpool CoinJoin transactions on the Bitcoin blockchain.

It starts from the first Whirlpool transactions for pool and follows on for analysis of anonymity sets over time.

---

## 🔧 How It Works

* **Blockchain Sync:** Syncs with the Bitcoin blockchain via [blockstream.info](https://blockstream.info) to find the transactions.
* **Local Database:** Uses a local SQLite database (`whirlpool.db`) to store data and progress. You can pause/resume without losing data.
* **Whirlpool Detection:** Identifies valid Whirlpool CoinJoins (5-input, 5-output).
* **Anonymity Set Tracking:** Tracks unspent outputs (UTXOs) currently in anonymity sets.
* **Reporting:** Generates CSV reports for time-series analysis and trend visualization.

---

## 🛠️ Installation

```bash
git clone <[your-repository-url](https://github.com/Ziya-Sadr/Ashi-Whirlpool-Analysis/edit/main/README.md)>
cd <your-repository-directory>
pip install requests python-bitcoinlib
```

---

## Usage

The tool is CLI-based and supports multiple modes.

### RUN

```bash
python ashidetector.py run
```

* On the first run, it fetches the first transactions and scans from the earliest block.
* You can stop (Ctrl+C) and resume anytime.

To start fresh:

```bash
python ashidetector.py run --fresh
```

---

### View Stats

```bash
python ashidetector.py stats
```

Displays the current anonymity set state based on synced data.

---

### Generate Reports

#### Simple Report

```bash
python ashidetector.py simplereport
```

With a custom block interval:

```bash
python ashidetector.py simplereport --interval 100
```

#### Detailed Report

```bash
python ashidetector.py report
```

With a custom block interval:

```bash
python ashidetector.py report --interval 500
```

---

## License

This project is licensed under the **MIT License**.
See the `LICENSE` file for more details.

---
