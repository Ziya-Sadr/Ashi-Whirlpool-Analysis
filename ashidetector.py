import sqlite3
import requests
import time
import json
import os
import argparse
import logging
from typing import List, Dict, Any, Optional, Tuple
from collections import Counter
from datetime import datetime, UTC
from bitcoin.core import CBlock, CTransaction, CBlockHeader

# --- Constants ---
DB_FILE = "whirlpool.db"
API_BASE_URL = "https://blockstream.info/api"
RETRY_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 5
PROCESS_LOOP_DELAY_SECONDS = 60 * 60 * 12  # 12 hours
SATOSHIS_PER_BTC = 100_000_000
WHIRLPOOL_TX_INPUTS = 5
WHIRLPOOL_TX_OUTPUTS = 5

GENESIS_TXS = {
    "0.25_BTC_Pool": {
        "txid": "7784df1182ab86ee33577b75109bb0f7c5622b9fb91df24b65ab2ab01b27dffa",
        "denomination_sats": int(0.25 * SATOSHIS_PER_BTC),
    },
    "0.025_BTC_Pool": {
        "txid": "737a867727db9a2c981ad622f2fa14b021ce8b1066a001e34fb793f8da833155",
        "denomination_sats": int(0.025 * SATOSHIS_PER_BTC),
    },
    "0.0025_BTC_Pool": {
        "txid": "efe738007ab4cef44f6625531730f2479646c4cc6074bc31bcbf8a6fab0d3979",
        "denomination_sats": int(0.0025 * SATOSHIS_PER_BTC),
    },
}

# Optimization: The block before which no genesis UTXOs were spent.
NO_SPEND_UNTIL_BLOCK = 899335

# The block height of the earliest genesis transaction.
# START_BLOCK_HEIGHT = 813000 # This will now be determined dynamically.

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Management ---
class DatabaseManager:
    """Handles all SQLite database operations for Whirlpool lineage tracking."""
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def setup_db(self, start_block_height: int, fresh_start=False):
        logging.info("Setting up database for Whirlpool lineage tracking...")
        if fresh_start:
            logging.warning("FRESH START: Dropping all existing tables.")
            self.cursor.execute('DROP TABLE IF EXISTS progress')
            self.cursor.execute('DROP TABLE IF EXISTS whirlpool_txs')
            self.cursor.execute('DROP TABLE IF EXISTS anonymity_set_utxos')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
        ''')
        # Initialize progress if table is new
        self.cursor.execute(
            "INSERT OR IGNORE INTO progress (key, value) VALUES ('last_processed_block_height', ?)",
            (start_block_height - 1,)
        )

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS whirlpool_txs (
                txid TEXT PRIMARY KEY,
                block_height INTEGER,
                block_hash TEXT,
                pool_name TEXT
            )
        ''')
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_whirlpool_txs_block_height ON whirlpool_txs(block_height)")

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS anonymity_set_utxos (
                output_id TEXT PRIMARY KEY, -- format: txid:vout
                txid TEXT,
                vout INTEGER,
                value_sats INTEGER,
                pool_name TEXT,
                is_spent BOOLEAN DEFAULT 0,
                spent_in_txid TEXT,
                spent_in_block_height INTEGER
            )
        ''')
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_utxos_txid ON anonymity_set_utxos(txid)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_utxos_is_spent ON anonymity_set_utxos(is_spent)")

        self.conn.commit()
        logging.info("Database setup complete.")

    def get_progress(self, key: str) -> Optional[int]:
        self.cursor.execute("SELECT value FROM progress WHERE key=?", (key,))
        result = self.cursor.fetchone()
        return result['value'] if result else None

    def update_progress(self, key: str, value: int):
        with self.conn:
            self.cursor.execute("REPLACE INTO progress (key, value) VALUES (?, ?)", (key, value))

    def is_db_seeded(self) -> bool:
        """Check if the genesis transactions are already in the database."""
        self.cursor.execute("SELECT 1 FROM whirlpool_txs WHERE txid=?", (GENESIS_TXS["0.25_BTC_Pool"]["txid"],))
        return self.cursor.fetchone() is not None

    def add_whirlpool_tx_with_utxos(self, tx_data: Dict[str, Any], utxos: List[Dict[str, Any]]):
        with self.conn:
            self.cursor.execute('''
                INSERT OR IGNORE INTO whirlpool_txs (txid, block_height, block_hash, pool_name)
                VALUES (:txid, :block_height, :block_hash, :pool_name)
            ''', tx_data)
            utxo_records = [(
                f"{u['txid']}:{u['vout']}", u['txid'], u['vout'], u['value_sats'], u['pool_name']
            ) for u in utxos]
            self.cursor.executemany('''
                INSERT OR IGNORE INTO anonymity_set_utxos (output_id, txid, vout, value_sats, pool_name)
                VALUES (?, ?, ?, ?, ?)
            ''', utxo_records)

    def get_unspent_utxo_by_id(self, output_id: str) -> Optional[sqlite3.Row]:
        self.cursor.execute("SELECT * FROM anonymity_set_utxos WHERE output_id = ? AND is_spent = 0", (output_id,))
        return self.cursor.fetchone()

    def mark_utxo_as_spent(self, output_id: str, spent_in_txid: str, spent_in_block_height: int):
        with self.conn:
            self.cursor.execute('''
                UPDATE anonymity_set_utxos
                SET is_spent = 1, spent_in_txid = ?, spent_in_block_height = ?
                WHERE output_id = ?
            ''', (spent_in_txid, spent_in_block_height, output_id))

    def get_anonymity_set_stats(self) -> Dict[str, Any]:
        self.cursor.execute("""
            SELECT pool_name, COUNT(*), SUM(value_sats)
            FROM anonymity_set_utxos
            WHERE is_spent = 0
            GROUP BY pool_name
        """)
        stats = {}
        for row in self.cursor.fetchall():
            stats[row['pool_name']] = {
                "count": row['COUNT(*)'],
                "total_sats": row['SUM(value_sats)']
            }
        return stats

    def close(self):
        self.conn.close()

# --- Blockstream API Client ---
class BlockstreamClient:
    def __init__(self):
        self.base_url = API_BASE_URL
        self._log_api_key_status()

    def _log_api_key_status(self):
        if os.path.exists("apikey"):
            logging.info("Found 'apikey' file. Note: The public blockstream.info API does not use authentication.")
        else:
            logging.info("No 'apikey' file found. Proceeding with unauthenticated public API.")

    def _request(self, endpoint: str, is_json=True) -> Optional[Any]:
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                if is_json:
                    return response.json()
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request failed for {url}: {e}. Attempt {attempt + 1}/{RETRY_ATTEMPTS}.")
                time.sleep(RETRY_DELAY_SECONDS)
        logging.error(f"Failed to fetch data from {url} after {RETRY_ATTEMPTS} attempts.")
        return None

    def get_tip_height(self) -> Optional[int]:
        logging.info("Fetching current blockchain tip height...")
        response = self._request("blocks/tip/height", is_json=False)
        return int(response.text) if response else None

    def get_block_hash(self, height: int) -> Optional[str]:
        logging.info(f"Fetching block hash for height {height}...")
        response = self._request(f"block-height/{height}", is_json=False)
        return response.text if response else None

    def get_raw_block(self, block_hash: str) -> Optional[bytes]:
        logging.info(f"Fetching raw block data for {block_hash}...")
        response = self._request(f"block/{block_hash}/raw", is_json=False)
        return response.content if response else None

    def get_transaction(self, txid: str) -> Optional[Dict[str, Any]]:
        logging.info(f"Fetching transaction details for {txid}...")
        return self._request(f"tx/{txid}")

# --- Whirlpool Lineage Tracer ---
class WhirlpoolTracer:
    def __init__(self, fresh_start: bool = False):
        self.db_manager = DatabaseManager(DB_FILE)
        self.client = BlockstreamClient()
        self.start_block_height = self._get_earliest_genesis_block_height()
        self.db_manager.setup_db(self.start_block_height, fresh_start)

    def _get_earliest_genesis_block_height(self) -> int:
        """Fetches all genesis txs to find the lowest block height."""
        logging.info("Determining earliest genesis block height from known transactions...")
        min_height = float('inf')
        for pool_name, genesis_info in GENESIS_TXS.items():
            txid = genesis_info['txid']
            tx_details = self.client.get_transaction(txid)
            if not tx_details or 'status' not in tx_details or not tx_details['status']['confirmed']:
                logging.error(f"FATAL: Could not fetch confirmed transaction details for genesis tx {txid} ({pool_name}).")
                raise SystemExit(f"Could not fetch genesis tx {txid}")
            
            height = tx_details['status']['block_height']
            logging.info(f"  -> Genesis tx for {pool_name} ({txid}) is in block {height}.")
            if height < min_height:
                min_height = height
        
        if min_height == float('inf'):
            logging.error("FATAL: Could not determine the minimum block height from any genesis transactions.")
            raise SystemExit("Could not determine start block.")

        logging.info(f"Determined earliest start block height to be: {min_height}")
        return min_height

    def _seed_database_with_genesis(self):
        logging.info("Database is not seeded. Seeding with genesis transactions...")
        for pool_name, genesis_info in GENESIS_TXS.items():
            txid = genesis_info['txid']
            logging.info(f"Processing genesis tx for {pool_name}: {txid}")
            tx_details = self.client.get_transaction(txid)
            if not tx_details:
                logging.error(f"FATAL: Could not fetch genesis transaction {txid}. Exiting.")
                raise SystemExit(f"Could not fetch genesis tx {txid}")

            tx_data = {
                "txid": tx_details['txid'],
                "block_height": tx_details['status']['block_height'],
                "block_hash": tx_details['status']['block_hash'],
                "pool_name": pool_name,
            }
            utxos = [{
                "txid": tx_details['txid'],
                "vout": i,
                "value_sats": vout['value'],
                "pool_name": pool_name
            } for i, vout in enumerate(tx_details['vout'])]

            self.db_manager.add_whirlpool_tx_with_utxos(tx_data, utxos)
            logging.info(f"Successfully seeded {pool_name}.")
        logging.info("All genesis transactions have been seeded.")

    def process_block(self, block: CBlock, block_height: int, block_hash: str):
        logging.info(f"Processing block {block_height}...")
        lineage_tx_count = 0
        for tx in block.vtx:
            # Coinbase transactions have no parents to trace
            if tx.is_coinbase():
                continue

            spent_utxo_pools = set()
            spent_utxos_from_set = []

            for vin in tx.vin:
                input_id = f"{vin.prevout.hash[::-1].hex()}:{vin.prevout.n}"
                parent_utxo = self.db_manager.get_unspent_utxo_by_id(input_id)
                if parent_utxo:
                    spent_utxo_pools.add(parent_utxo['pool_name'])
                    spent_utxos_from_set.append(parent_utxo)

            if not spent_utxos_from_set:
                continue # This transaction is not descended from our set.

            txid = tx.GetTxid()[::-1].hex()
            logging.info(f"  -> Transaction {txid} spends UTXOs from our anonymity set.")

            # Mark all identified parent UTXOs as spent
            for utxo in spent_utxos_from_set:
                self.db_manager.mark_utxo_as_spent(utxo['output_id'], txid, block_height)
                logging.info(f"     - Marked {utxo['output_id']} as spent.")

            # A valid Whirlpool transaction must only mix coins from a single pool.
            if len(spent_utxo_pools) > 1:
                logging.warning(f"  -> Tx {txid} mixes UTXOs from multiple pools: {spent_utxo_pools}. Not a valid Whirlpool mix. Pruning lineage.")
                continue

            # Check if it's a valid Whirlpool transaction (5-in, 5-out)
            if len(tx.vin) == WHIRLPOOL_TX_INPUTS and len(tx.vout) == WHIRLPOOL_TX_OUTPUTS:
                lineage_tx_count += 1
                pool_name = spent_utxo_pools.pop()
                logging.info(f"  -> Tx {txid} confirmed as a {pool_name} Whirlpool transaction. Adding its outputs to the anonymity set.")
                
                tx_data = {
                    "txid": txid,
                    "block_height": block_height,
                    "block_hash": block_hash,
                    "pool_name": pool_name,
                }
                new_utxos = [{
                    "txid": txid,
                    "vout": i,
                    "value_sats": vout.nValue,
                    "pool_name": pool_name
                } for i, vout in enumerate(tx.vout)]
                
                self.db_manager.add_whirlpool_tx_with_utxos(tx_data, new_utxos)
            else:
                logging.info(f"  -> Tx {txid} is not a 5-in/5-out Whirlpool mix. Pruning this lineage.")

        logging.info(f"Block {block_height} processed. Found {lineage_tx_count} new Whirlpool transaction(s).")

    def run(self):
        logging.info("Starting Whirlpool Lineage Tracer...")

        if not self.db_manager.is_db_seeded():
            self._seed_database_with_genesis()

        try:
            while True:
                last_processed = self.db_manager.get_progress('last_processed_block_height')
                start_block = last_processed + 1 if last_processed is not None else self.start_block_height
                
                # Optimization: Skip blocks before the first known spend from a genesis UTXO
                if start_block < NO_SPEND_UNTIL_BLOCK:
                    logging.info(f"Optimization: No known spends before block {NO_SPEND_UNTIL_BLOCK}. Fast-forwarding...")
                    # Update progress so if we stop, we don't re-scan the skipped range
                    self.db_manager.update_progress('last_processed_block_height', NO_SPEND_UNTIL_BLOCK - 1)
                    start_block = NO_SPEND_UNTIL_BLOCK

                tip_height = self.client.get_tip_height()
                if not tip_height:
                    logging.error("Could not get tip height. Retrying in 1 minute.")
                    time.sleep(60)
                    continue

                logging.info(f"Current tip height: {tip_height}. Last processed: {last_processed or 'None'}.")

                if start_block > tip_height:
                    self.display_stats()
                    logging.info(f"Caught up to tip. Waiting for {PROCESS_LOOP_DELAY_SECONDS // 3600} hours.")
                    time.sleep(PROCESS_LOOP_DELAY_SECONDS)
                    continue

                for height in range(start_block, tip_height + 1):
                    block_hash = self.client.get_block_hash(height)
                    if not block_hash:
                        logging.error(f"Could not get block hash for height {height}. Skipping.")
                        continue
                    
                    # For lineage tracing, we need full block data, not just tx list
                    raw_block_data = self.client.get_raw_block(block_hash)
                    if not raw_block_data:
                        logging.error(f"Could not get raw block for {height}. Skipping.")
                        continue

                    block = CBlock.deserialize(raw_block_data)
                    self.process_block(block, height, block_hash)

                    self.db_manager.update_progress('last_processed_block_height', height)
                    
                    if height % 100 == 0:
                        logging.info(f"--- Progress: Reached block {height}/{tip_height} ---")


        except KeyboardInterrupt:
            logging.info("Detector stopped by user.")
        finally:
            self.display_stats()
            self.db_manager.close()
            logging.info("Whirlpool Tracer shut down.")

    def display_stats(self):
        logging.info("--- Current Anonymity Set Stats ---")
        stats = self.db_manager.get_anonymity_set_stats()
        if not stats:
            logging.info("No unspent UTXOs in the anonymity set.")
            return

        total_utxos = 0
        total_btc = 0
        for pool, data in stats.items():
            btc_value = data['total_sats'] / SATOSHIS_PER_BTC
            total_utxos += data['count']
            total_btc += btc_value
            logging.info(f"  {pool}: {data['count']} UTXOs ({btc_value:.4f} BTC)")
        
        logging.info("-" * 20)
        logging.info(f"  Total: {total_utxos} UTXOs ({total_btc:.4f} BTC)")
        logging.info("-------------------------------------")

    def generate_report(self, interval=1000, output_file=None):
        logging.info(f"--- Generating Chart-Ready Time-Series Report (Interval: {interval} blocks) ---")

        # Get all "add" and "spend" events
        self.db_manager.cursor.execute("""
            SELECT t.block_height AS block, u.pool_name, u.value_sats
            FROM anonymity_set_utxos u JOIN whirlpool_txs t ON u.txid = t.txid
            UNION ALL
            SELECT spent_in_block_height AS block, pool_name, -value_sats AS value_sats
            FROM anonymity_set_utxos
            WHERE is_spent = 1 AND spent_in_block_height IS NOT NULL
        """)
        events = self.db_manager.cursor.fetchall()
        
        if not events:
            logging.info("No data in the database to generate a report.")
            return

        # Sort all events chronologically
        events.sort(key=lambda x: x['block'])
        
        min_block = events[0]['block']
        max_block = events[-1]['block']
        logging.info(f"Data ranges from block {min_block} to {max_block}.")
        
        # --- Prepare for report generation ---
        pool_names = sorted(GENESIS_TXS.keys())
        header = ['end_block', 'total_unspent_btc']
        for name in pool_names:
            header.append(f'delta_{name}_btc')
        
        report_rows = []
        cumulative_stats = {name: 0 for name in pool_names} # Store sats
        event_idx = 0
        
        # --- Generate snapshots interval by interval ---
        for end_block in range((min_block // interval + 1) * interval, (max_block // interval + 2) * interval, interval):
            start_block = end_block - interval
            period_delta_stats = {name: 0 for name in pool_names}

            # Process all events that fall within this interval
            while event_idx < len(events) and events[event_idx]['block'] < end_block:
                event = events[event_idx]
                pool = event['pool_name']
                sats_delta = event['value_sats']
                
                if pool in cumulative_stats:
                    cumulative_stats[pool] += sats_delta
                    period_delta_stats[pool] += sats_delta
                event_idx += 1
            
            # Don't add empty rows at the beginning
            if not any(period_delta_stats.values()) and sum(cumulative_stats.values()) == 0:
                continue

            total_cumulative_sats = sum(cumulative_stats.values())
            
            row = {'end_block': end_block}
            row['total_unspent_btc'] = total_cumulative_sats / SATOSHIS_PER_BTC
            for name in pool_names:
                row[f'delta_{name}_btc'] = period_delta_stats[name] / SATOSHIS_PER_BTC
            
            report_rows.append(row)

        if output_file and report_rows:
            self._write_chart_report_to_csv(output_file, header, report_rows)

    def _write_chart_report_to_csv(self, filename: str, header: List[str], rows: List[Dict[str, Any]]):
        """Writes the chart-optimized report data to a CSV file."""
        import csv
        try:
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=header)
                writer.writeheader()
                writer.writerows(rows)
            logging.info(f"Chart-ready report successfully saved to {filename}")
            print(f"\nChart-ready report saved to {filename}")
            print("You can now import this file into any charting tool.")
        except IOError as e:
            logging.error(f"Failed to write report to {filename}: {e}")
    
    def generate_simple_report(self, interval=10, output_file=None):
        logging.info(f"--- Generating Simple Chart-Ready Report (Interval: {interval} blocks) ---")

        # Get all "add" and "spend" events
        self.db_manager.cursor.execute("""
            SELECT t.block_height AS block, u.value_sats, 1 AS utxo_delta
            FROM anonymity_set_utxos u JOIN whirlpool_txs t ON u.txid = t.txid
            UNION ALL
            SELECT spent_in_block_height AS block, -value_sats AS value_sats, -1 AS utxo_delta
            FROM anonymity_set_utxos
            WHERE is_spent = 1 AND spent_in_block_height IS NOT NULL
        """)
        events = self.db_manager.cursor.fetchall()
        
        if not events:
            logging.info("No data in the database to generate a report.")
            return

        # Sort all events chronologically
        events.sort(key=lambda x: x['block'])
        
        min_block = events[0]['block']
        max_block = events[-1]['block']
        logging.info(f"Data ranges from block {min_block} to {max_block}.")
        
        header = ['end_block', 'total_unspent_btc', 'total_unspent_utxos', 'net_change_btc', 'net_change_utxos']
        report_rows = []
        cumulative_sats = 0
        cumulative_utxos = 0
        event_idx = 0
        
        for end_block in range((min_block // interval + 1) * interval, (max_block // interval + 2) * interval, interval):
            period_delta_sats = 0
            period_delta_utxos = 0

            while event_idx < len(events) and events[event_idx]['block'] < end_block:
                event = events[event_idx]
                sats_delta = event['value_sats']
                utxo_delta = event['utxo_delta']
                
                cumulative_sats += sats_delta
                cumulative_utxos += utxo_delta
                period_delta_sats += sats_delta
                period_delta_utxos += utxo_delta
                event_idx += 1
            
            if period_delta_sats == 0 and cumulative_sats == 0 and not any(r['net_change_btc'] != 0 for r in report_rows):
                continue

            report_rows.append({
                'end_block': end_block,
                'total_unspent_btc': cumulative_sats / SATOSHIS_PER_BTC,
                'total_unspent_utxos': cumulative_utxos,
                'net_change_btc': period_delta_sats / SATOSHIS_PER_BTC,
                'net_change_utxos': period_delta_utxos
            })

        if output_file and report_rows:
            self._write_chart_report_to_csv(output_file, header, report_rows)

    def _print_report_to_console(self, rows: List[Dict[str, Any]]):
        """DEPRECATED: This function is no longer suitable for the new report format."""
        pass

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="WhirlpoolTracer: A tool for tracing Whirlpool CoinJoin transaction lineages.")
    parser.add_argument("command", choices=["run", "stats", "report", "simplereport"], help="'run' to sync, 'stats' to view current stats, 'report' for detailed report, 'simplereport' for a simpler chart-focused report.")
    parser.add_argument("--fresh", action="store_true", help="Start with a fresh, empty database. Deletes existing data.")
    parser.add_argument("--interval", type=int, help="The block interval for reports (default: 1000 for 'report', 10 for 'simplereport').")
    args = parser.parse_args()

    if args.command == "run":
        tracer = WhirlpoolTracer(fresh_start=args.fresh)
        tracer.run()
    elif args.command == "stats":
        tracer = WhirlpoolTracer()
        tracer.display_stats()
        tracer.db_manager.close()
    elif args.command == "report":
        tracer = WhirlpoolTracer()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"whirlpool_report_{timestamp}.csv"
        interval = args.interval if args.interval is not None else 1000
        tracer.generate_report(interval=interval, output_file=filename)
        tracer.db_manager.close()
    elif args.command == "simplereport":
        tracer = WhirlpoolTracer()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"whirlpool_simplereport_{timestamp}.csv"
        interval = args.interval if args.interval is not None else 10
        tracer.generate_simple_report(interval=interval, output_file=filename)
        tracer.db_manager.close()

if __name__ == "__main__":
    main()
