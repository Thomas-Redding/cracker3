use anyhow::{Context, Result};
use csv::WriterBuilder;
use flate2::read::GzDecoder;
use md5::{Digest, Md5};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ==============================================================================================
// STEP 1: CONFIGURATION
// ==============================================================================================

// Toggle this or use env vars
const IS_PI: bool = false;

struct Config {
    input_path: &'static str,
    partition_path: &'static str,
    sorted_path: &'static str,
    output_path: &'static str,
    markets_path: &'static str,
    num_batches: u64,
    max_items_in_batch_writer: usize,
}

const CONFIG: Config = if IS_PI {
    // Raspberry Pi
    Config {
        input_path: "/media/exfat/thomas-polymarket-data/order-filled-2025-nov-26/",
        partition_path: "/media/exfat/thomas-polymarket-data/order-filled-2025-nov-26-batched/",
        sorted_path: "/media/exfat/thomas-polymarket-data/order-filled-2025-nov-26-batched-sorted/",
        output_path: "/media/exfat/thomas-polymarket-data/order-filled-2025-nov-26-batched-trades/",
        markets_path: "markets.jsonl",
        num_batches: 500,
        max_items_in_batch_writer: 100_000,
    }
} else {
    // MacOS
    Config {
        input_path:
            "/Volumes/Extreme SSD/polymarket-data/AWSDynamoDB/01764193682987-aad2f876/data/",
        partition_path: "/Volumes/Extreme SSD/polymarket-data/intermediate/",
        sorted_path: "/Volumes/Extreme SSD/polymarket-data/sorted/",
        output_path: "/Volumes/Extreme SSD/polymarket-data/output/",
        markets_path: "/Users/thomasredding/proj/cracker2/markets.jsonl",
        num_batches: 50,
        max_items_in_batch_writer: 2_000_000,
    }
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Partition,
    Sorting,
    Processing,
    Complete,
}

const MAX_PARALLEL_BATCH_JOBS: usize = 3;
const SORT_CHILD_PARALLELISM: usize = 4;
const SORT_MEMORY_LIMIT: &str = "30%";

const SPECIAL_WALLETS: [&str; 2] = [
    "0xc5d563a36ae78145c45a50134d48a1215220f80a", // Neg Risk CTF Exchange
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", // CTF Exchange
];

const UNEXPECTED_WALLETS: [&str; 3] = [
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", // USDC
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296", // Neg risk adapter
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045", // CTF Conditional Token Framework
];

// ==============================================================================================
// STEP 2: DATA STRUCTURES
// ==============================================================================================

// Input JSON structures (DynamoDB style)
#[derive(Deserialize, Debug)]
struct DynamoString {
    S: String,
}
#[derive(Deserialize, Debug)]
struct DynamoNumber {
    N: String,
}
#[derive(Deserialize, Debug)]
struct DynamoBytes {
    B: String,
}

#[derive(Deserialize, Debug)]
struct RawItemInner {
    _gs_gid: DynamoString,
    maker: DynamoString,
    maker_amount_filled: DynamoNumber,
    maker_asset_id: DynamoString,
    fee: DynamoNumber,
    timestamp: DynamoNumber,
    taker_asset_id: DynamoString,
    transaction_hash: DynamoBytes,
    taker_amount_filled: DynamoNumber,
    taker: DynamoString,
    block_range: DynamoString,
    order_hash: DynamoBytes,
    _gs_chain: DynamoString,
    id: DynamoString,
    vid: DynamoNumber,
}

#[derive(Deserialize, Debug)]
struct RawItem {
    #[serde(rename = "Item")]
    item: RawItemInner,
}

// Intermediate CSV Row
#[derive(Serialize, Deserialize, Debug, Clone)]
struct IntermediateRow {
    // Put transaction_hash first for sorting
    transaction_hash: String,
    _gs_gid: String,
    maker: String,
    maker_amount_filled: f64,
    maker_asset_id: String,
    fee: f64,
    timestamp: f64,
    taker_asset_id: String,
    taker_amount_filled: f64,
    taker: String,
    block_range: String,
    order_hash: String,
    _gs_chain: String,
    id: String,
    vid: f64,
    condition_id: String,
}

// Market Definition
#[derive(Deserialize, Debug)]
struct MarketToken {
    token_id: String,
}
#[derive(Deserialize, Debug)]
struct Market {
    condition_id: String,
    tokens: Vec<MarketToken>,
}

// Final Output Structures
#[derive(Serialize, Debug)]
struct TradeSide {
    user: String,
    action: String,
    shares: f64,
    outcome: String,
    price: f64,
    usdc: f64,
    order_hash: String,
}

#[derive(Serialize, Debug)]
struct Trade {
    timestamp: f64,
    transaction_hash: String,
    taker: TradeSide,
    makers: Vec<TradeSide>,
}

#[derive(Serialize, Debug)]
struct ErrorLog {
    fills: Vec<IntermediateRow>,
    error: String,
}

#[derive(Serialize)]
#[serde(untagged)] // Allows writing either Trade or ErrorLog to the same JSONL stream
enum OutputItem {
    Trade(Trade),
    Error(ErrorLog),
}

// ==============================================================================================
// STEP 3: HELPER CLASSES
// ==============================================================================================

struct BatchWriter<T: Serialize> {
    dirpath: PathBuf,
    extension: String,
    max_items: usize,
    num_items: usize,
    batches: HashMap<String, Vec<T>>,
    write_header: bool,
    header_written: HashMap<String, bool>,
}

impl<T: Serialize> BatchWriter<T> {
    fn new(dirpath: &str, extension: &str, max_items: usize, write_header: bool) -> Result<Self> {
        if !Path::new(dirpath).exists() {
            fs::create_dir_all(dirpath)?;
        }
        Ok(Self {
            dirpath: PathBuf::from(dirpath),
            extension: extension.to_string(),
            max_items,
            num_items: 0,
            batches: HashMap::new(),
            write_header,
            header_written: HashMap::new(),
        })
    }

    fn add(&mut self, batch_key: String, item: T) -> Result<()> {
        self.batches.entry(batch_key).or_default().push(item);
        self.num_items += 1;
        if self.num_items >= self.max_items {
            self.flush_largest_batch()?;
        }
        Ok(())
    }

    fn flush_largest_batch(&mut self) -> Result<()> {
        let largest_key = self
            .batches
            .iter()
            .max_by_key(|entry| entry.1.len())
            .map(|(k, _)| k.clone());

        if let Some(key) = largest_key {
            self.flush_batch(&key)?;
        }
        Ok(())
    }

    fn flush_batch(&mut self, batch_key: &str) -> Result<()> {
        if let Some(items) = self.batches.remove(batch_key) {
            let count = items.len();
            if count == 0 {
                return Ok(());
            }

            let filepath = self
                .dirpath
                .join(format!("{}{}", batch_key, self.extension));
            let file_exists = filepath.exists();

            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filepath)?;
            let mut writer = std::io::BufWriter::new(file);

            if self.extension == ".jsonl" {
                for item in items {
                    serde_json::to_writer(&mut writer, &item)?;
                    writer.write_all(b"\n")?;
                }
            } else if self.extension == ".csv" {
                let mut csv_wtr = WriterBuilder::new()
                    .has_headers(
                        self.write_header
                            && !file_exists
                            && !*self.header_written.get(batch_key).unwrap_or(&false),
                    )
                    .from_writer(writer);

                for item in items {
                    csv_wtr.serialize(item)?;
                }
                csv_wtr.flush()?;

                // Mark header as written for this batch key
                if self.write_header {
                    self.header_written.insert(batch_key.to_string(), true);
                }
            }
            self.num_items -= count;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let keys: Vec<String> = self.batches.keys().cloned().collect();
        for key in keys {
            self.flush_batch(&key)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct SortConfig {
    binary: String,
    supports_gnu_flags: bool,
}

impl SortConfig {
    fn detect() -> Self {
        if is_gnu_sort("gsort") {
            Self {
                binary: "gsort".to_string(),
                supports_gnu_flags: true,
            }
        } else if is_gnu_sort("sort") {
            Self {
                binary: "sort".to_string(),
                supports_gnu_flags: true,
            }
        } else {
            Self {
                binary: "sort".to_string(),
                supports_gnu_flags: false,
            }
        }
    }
}

fn is_gnu_sort(binary: &str) -> bool {
    Command::new(binary)
        .arg("--version")
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout)
                    .to_ascii_lowercase()
                    .contains("gnu coreutils")
        })
        .unwrap_or(false)
}

// ==============================================================================================
// STEP 4: MAIN LOGIC
// ==============================================================================================

fn current_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn detect_start_stage() -> Result<Stage> {
    let partition_exists = Path::new(CONFIG.partition_path).exists();
    let sorted_exists = Path::new(CONFIG.sorted_path).exists();
    let output_exists = Path::new(CONFIG.output_path).exists();

    if !partition_exists {
        if sorted_exists || output_exists {
            anyhow::bail!(
                "Later stage directories exist without partition data. Remove {} and {}.",
                CONFIG.sorted_path,
                CONFIG.output_path
            );
        }
        return Ok(Stage::Partition);
    }

    if !sorted_exists {
        if output_exists {
            anyhow::bail!(
                "Output directory {} exists without sorted data. Remove it before rerunning.",
                CONFIG.output_path
            );
        }
        return Ok(Stage::Sorting);
    }

    if !output_exists {
        return Ok(Stage::Processing);
    }

    Ok(Stage::Complete)
}

fn load_asset_map() -> Result<HashMap<String, String>> {
    println!("{} Loading markets...", current_time());
    let mut condition_id_counts = HashSet::new();
    let mut asset_id_to_condition_id = HashMap::new();

    let file = File::open(CONFIG.markets_path).context("Failed to open markets file")?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let market: Market = serde_json::from_str(&line)?;

        if market.condition_id.is_empty() {
            continue;
        }
        let token_ids: Vec<&String> = market.tokens.iter().map(|t| &t.token_id).collect();
        if token_ids.len() != 2 {
            panic!("Expected 2 tokens, got {}", token_ids.len());
        }
        if token_ids.iter().any(|t| t.is_empty()) {
            continue;
        }

        if !condition_id_counts.insert(market.condition_id.clone()) {
            panic!(
                "Condition ID {} already in condition_id_counts",
                market.condition_id
            );
        }

        for token_id in token_ids {
            if asset_id_to_condition_id.contains_key(token_id) {
                panic!("Token ID {} already in map", token_id);
            }
            asset_id_to_condition_id.insert(token_id.clone(), market.condition_id.clone());
        }
    }

    Ok(asset_id_to_condition_id)
}

fn main() -> Result<()> {
    let start_stage = detect_start_stage()?;
    if start_stage == Stage::Complete {
        println!(
            "{} All stage directories exist; nothing to do.",
            current_time()
        );
        return Ok(());
    }

    let asset_id_to_condition_id = Arc::new(load_asset_map()?);
    let sort_config = Arc::new(SortConfig::detect());

    if start_stage <= Stage::Partition {
        run_partition_stage(asset_id_to_condition_id.as_ref())?;
    }

    if start_stage <= Stage::Sorting {
        run_sorting_stage(sort_config.as_ref())?;
    }

    if start_stage <= Stage::Processing {
        run_processing_stage(Arc::clone(&asset_id_to_condition_id))?;
    }

    Ok(())
}

fn run_partition_stage(asset_id_to_condition_id: &HashMap<String, String>) -> Result<()> {
    println!(
        "{} Starting partition stage at {}",
        current_time(),
        CONFIG.partition_path
    );
    fs::create_dir_all(CONFIG.partition_path)?;

    let mut csv_writer = BatchWriter::<IntermediateRow>::new(
        CONFIG.partition_path,
        ".csv",
        CONFIG.max_items_in_batch_writer,
        false,
    )?;

    let input_files: Vec<PathBuf> = fs::read_dir(CONFIG.input_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".json.gz"))
        .filter(|p| !p.to_string_lossy().contains("/."))
        .collect();

    for (idx, file_path) in input_files.iter().enumerate() {
        println!(
            "{} Partitioning {} / {}",
            current_time(),
            idx + 1,
            input_files.len()
        );

        let file = File::open(file_path)?;

        println!(
            "{} Processing file {}",
            current_time(),
            file_path.to_string_lossy()
        );

        let gz = GzDecoder::new(file);
        let reader = BufReader::new(gz);

        for line in reader.lines() {
            let line = line?;
            let raw: RawItem = serde_json::from_str(&line)?;
            let item = raw.item;

            let token_id = if item.maker_asset_id.S != "0" {
                &item.maker_asset_id.S
            } else {
                &item.taker_asset_id.S
            };

            if let Some(cond_id) = asset_id_to_condition_id.get(token_id) {
                let mut hasher = Md5::new();
                hasher.update(cond_id.as_bytes());
                let hash = hasher.finalize();
                let hash_val = u128::from_be_bytes(hash.into());
                let batch_index = hash_val % (CONFIG.num_batches as u128);

                let row = IntermediateRow {
                    transaction_hash: item.transaction_hash.B,
                    _gs_gid: item._gs_gid.S,
                    maker: item.maker.S,
                    maker_amount_filled: item.maker_amount_filled.N.parse().unwrap_or(0.0),
                    maker_asset_id: item.maker_asset_id.S,
                    fee: item.fee.N.parse().unwrap_or(0.0),
                    timestamp: item.timestamp.N.parse().unwrap_or(0.0),
                    taker_asset_id: item.taker_asset_id.S,
                    taker_amount_filled: item.taker_amount_filled.N.parse().unwrap_or(0.0),
                    taker: item.taker.S,
                    block_range: item.block_range.S,
                    order_hash: item.order_hash.B,
                    _gs_chain: item._gs_chain.S,
                    id: item.id.S,
                    vid: item.vid.N.parse().unwrap_or(0.0),
                    condition_id: cond_id.clone(),
                };

                csv_writer.add(batch_index.to_string(), row)?;
            }
        }
    }

    csv_writer.flush()?;
    println!(
        "{} Partition stage complete. Output: {}",
        current_time(),
        CONFIG.partition_path
    );
    Ok(())
}

fn run_sorting_stage(sort_config: &SortConfig) -> Result<()> {
    println!(
        "{} Starting sorting stage at {} using {} (GNU flags: {})",
        current_time(),
        CONFIG.sorted_path,
        sort_config.binary,
        sort_config.supports_gnu_flags
    );
    fs::create_dir_all(CONFIG.sorted_path)?;

    let batch_files = collect_stage_files(CONFIG.partition_path, ".csv")?;
    if batch_files.is_empty() {
        println!(
            "{} No partition files found in {}. Skipping sorting.",
            current_time(),
            CONFIG.partition_path
        );
        return Ok(());
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(MAX_PARALLEL_BATCH_JOBS)
        .build()
        .context("Failed to build rayon pool")?;

    let total_batches = batch_files.len();
    pool.install(|| -> Result<()> {
        batch_files
            .par_iter()
            .enumerate()
            .try_for_each(|(idx, filepath)| {
                sort_partition_file(idx, total_batches, filepath, sort_config)
            })
    })?;

    println!(
        "{} Sorting stage complete. Output: {}",
        current_time(),
        CONFIG.sorted_path
    );
    Ok(())
}

fn sort_partition_file(
    idx: usize,
    total: usize,
    filepath: &Path,
    sort_config: &SortConfig,
) -> Result<()> {
    println!(
        "{} Sorting {} / {} -- {}",
        current_time(),
        idx + 1,
        total,
        filepath.to_string_lossy()
    );

    let filename = filepath
        .file_name()
        .context("Failed to determine batch filename")?;
    let sorted_path = Path::new(CONFIG.sorted_path).join(filename);
    let output_file = File::create(&sorted_path)?;

    let mut sort_command = Command::new(&sort_config.binary);
    sort_command.args(["-t", ",", "-k", "1,1"]);
    if sort_config.supports_gnu_flags {
        let parallel_arg = format!("--parallel={}", SORT_CHILD_PARALLELISM);
        sort_command.arg(parallel_arg);
        sort_command.args(["-S", SORT_MEMORY_LIMIT]);
    }
    sort_command.arg(filepath);
    sort_command.stdout(Stdio::from(output_file));

    let status = sort_command.status().with_context(|| {
        format!(
            "Failed to execute {} for {:?}",
            sort_config.binary, filepath
        )
    })?;
    if !status.success() {
        anyhow::bail!(
            "Sort command {} failed for {:?} with status {}",
            sort_config.binary,
            filepath,
            status
        );
    }

    Ok(())
}

fn run_processing_stage(asset_map: Arc<HashMap<String, String>>) -> Result<()> {
    println!(
        "{} Starting processing stage at {}",
        current_time(),
        CONFIG.output_path
    );
    fs::create_dir_all(CONFIG.output_path)?;

    let sorted_files = collect_stage_files(CONFIG.sorted_path, ".csv")?;
    if sorted_files.is_empty() {
        println!(
            "{} No sorted files found in {}. Skipping processing.",
            current_time(),
            CONFIG.sorted_path
        );
        return Ok(());
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(MAX_PARALLEL_BATCH_JOBS)
        .build()
        .context("Failed to build rayon pool")?;

    let total_batches = sorted_files.len();
    pool.install(|| -> Result<()> {
        sorted_files
            .par_iter()
            .enumerate()
            .try_for_each(|(idx, filepath)| {
                process_sorted_file(idx, total_batches, filepath, asset_map.as_ref())
            })
    })?;

    println!(
        "{} Processing stage complete. Output: {}",
        current_time(),
        CONFIG.output_path
    );
    Ok(())
}

fn process_sorted_file(
    idx: usize,
    total: usize,
    filepath: &Path,
    asset_map: &HashMap<String, String>,
) -> Result<()> {
    println!(
        "{} Processing {} / {} -- {}",
        current_time(),
        idx + 1,
        total,
        filepath.to_string_lossy()
    );

    let file = File::open(filepath)?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    let mut output_writer = BatchWriter::<OutputItem>::new(
        CONFIG.output_path,
        ".jsonl",
        CONFIG.max_items_in_batch_writer,
        false,
    )?;

    let mut current_tx_hash = String::new();
    let mut current_group: Vec<IntermediateRow> = Vec::new();

    for result in csv_reader.deserialize::<IntermediateRow>() {
        let row = result?;
        if row.transaction_hash != current_tx_hash {
            if !current_group.is_empty() {
                process_group(&current_group, asset_map, &mut output_writer)?;
            }
            current_tx_hash = row.transaction_hash.clone();
            current_group.clear();
        }
        current_group.push(row);
    }

    if !current_group.is_empty() {
        process_group(&current_group, asset_map, &mut output_writer)?;
    }

    output_writer.flush()?;
    Ok(())
}

fn collect_stage_files(dirpath: &str, extension: &str) -> Result<Vec<PathBuf>> {
    if !Path::new(dirpath).exists() {
        anyhow::bail!("Directory {} does not exist", dirpath);
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(dirpath)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("._") {
                continue;
            }
        }
        if let Some(ext) = path.extension() {
            if ext.to_string_lossy() == extension.trim_start_matches('.') {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

// ==============================================================================================
// STEP 5: DOMAIN LOGIC
// ==============================================================================================

fn process_group(
    fills: &[IntermediateRow],
    asset_map: &HashMap<String, String>,
    writer: &mut BatchWriter<OutputItem>,
) -> Result<()> {
    match parse_fills(fills) {
        Ok(trades) => {
            for trade in trades {
                // Determine condition_id from the taker's outcome (asset_id)
                // In Python: asset_id_to_condition_id[trade["taker"]["outcome"]]
                if let Some(cond_id) = asset_map.get(&trade.taker.outcome) {
                    writer.add(cond_id.clone(), OutputItem::Trade(trade))?;
                } else {
                    // Fallback if mapping missing (shouldn't happen based on prev logic)
                    writer.add("unknown".to_string(), OutputItem::Trade(trade))?;
                }
            }
        }
        Err(e) => {
            writer.add(
                "error".to_string(),
                OutputItem::Error(ErrorLog {
                    fills: fills.to_vec(),
                    error: e.to_string(),
                }),
            )?;
        }
    }
    Ok(())
}

fn parse_fills(rows: &[IntermediateRow]) -> Result<Vec<Trade>> {
    // Group by transaction hash (already grouped by caller, but logic keeps structure)
    // The Python logic splits the input rows into transactions.
    // Here `rows` represents ONE transaction group because of the caller logic,
    // BUT the python function `parse_fills` accepts a list of rows that might contain multiple transactions?
    // Looking at the Python caller: `for tx_hash, group in itertools.groupby... fills = list(group)`.
    // So the input to `parse_fills` is strictly ONE transaction hash.
    // The Python `parse_fills` re-groups them: `transactions = {} ...`.
    // It seems redundant but we will follow the logic to be safe.

    // In our Rust loop, we pass one group.
    let transactions = vec![rows]; // Single transaction list

    let mut rtn = Vec::new();

    for transaction in transactions {
        let mut special_fill: Option<&IntermediateRow> = None;

        for fill in transaction {
            if SPECIAL_WALLETS.contains(&fill.maker.as_str()) {
                if special_fill.is_some() {
                    anyhow::bail!("Multiple special fills found");
                }
                special_fill = Some(fill);
            }
            if SPECIAL_WALLETS.contains(&fill.taker.as_str()) {
                if special_fill.is_some() {
                    anyhow::bail!("Multiple special fills found");
                }
                special_fill = Some(fill);
            }
            if UNEXPECTED_WALLETS.contains(&fill.maker.as_str()) {
                anyhow::bail!("Unexpected maker wallet: {}", fill.maker);
            }
            if UNEXPECTED_WALLETS.contains(&fill.taker.as_str()) {
                anyhow::bail!("Unexpected taker wallet: {}", fill.taker);
            }
        }

        let special_fill = special_fill.ok_or_else(|| anyhow::anyhow!("No special fill found"))?;

        let liquidity_taker_fill = parse_special_fill_row(special_fill)?;

        let mut liquidity_maker_fills = Vec::new();
        for fill in transaction {
            // Need to compare pointers or fields to exclude special_fill.
            // Comparing known unique fields like `id` is safer.
            if fill.id != special_fill.id {
                liquidity_maker_fills.push(parse_non_special_fill_row(
                    fill,
                    &liquidity_taker_fill.user,
                )?);
            }
        }

        // Sanity Checks
        let mut action_outcome_pairs: Vec<(String, String)> = liquidity_maker_fills
            .iter()
            .map(|f| (f.action.clone(), f.outcome.clone()))
            .collect::<HashSet<_>>() // dedup
            .into_iter()
            .collect();

        if action_outcome_pairs.len() > 2 {
            anyhow::bail!(
                "Too many action-outcome pairs: {}",
                action_outcome_pairs.len()
            );
        }

        if action_outcome_pairs.len() == 2 {
            if action_outcome_pairs[0].0 == action_outcome_pairs[1].0 {
                anyhow::bail!("Pairs have same action");
            }
            if action_outcome_pairs[0].1 == action_outcome_pairs[1].1 {
                anyhow::bail!("Pairs have same outcome");
            }
        }

        // Check against taker fill
        if !action_outcome_pairs.is_empty() {
            if action_outcome_pairs[0].0 == liquidity_taker_fill.action {
                if action_outcome_pairs[0].1 == liquidity_taker_fill.outcome {
                    anyhow::bail!("Mismatch A");
                }
            } else {
                if action_outcome_pairs[0].1 != liquidity_taker_fill.outcome {
                    anyhow::bail!("Mismatch B");
                }
            }
        }

        if action_outcome_pairs.len() == 2 {
            if action_outcome_pairs[1].0 == liquidity_taker_fill.action {
                if action_outcome_pairs[1].1 == liquidity_taker_fill.outcome {
                    anyhow::bail!("Mismatch C");
                }
            } else {
                if action_outcome_pairs[1].1 != liquidity_taker_fill.outcome {
                    anyhow::bail!("Mismatch D");
                }
            }
        }

        let sum_maker_shares: f64 = liquidity_maker_fills.iter().map(|f| f.shares).sum();
        if (sum_maker_shares - liquidity_taker_fill.shares).abs() >= 1e-5 {
            anyhow::bail!(
                "Share sum mismatch: {} vs {}",
                sum_maker_shares,
                liquidity_taker_fill.shares
            );
        }

        let mut taker_cash = 0.0;
        for fill in &liquidity_maker_fills {
            if fill.action == liquidity_taker_fill.action {
                taker_cash += fill.shares - fill.usdc;
            } else {
                taker_cash += fill.usdc;
            }
        }

        if (taker_cash - liquidity_taker_fill.usdc).abs() >= 1e-5 {
            anyhow::bail!(
                "Cash mismatch: {} vs {}",
                taker_cash,
                liquidity_taker_fill.usdc
            );
        }

        rtn.push(Trade {
            timestamp: transaction[0].timestamp,
            transaction_hash: transaction[0].transaction_hash.clone(),
            taker: liquidity_taker_fill,
            makers: liquidity_maker_fills,
        });
    }

    Ok(rtn)
}

fn parse_non_special_fill_row(
    fill_row: &IntermediateRow,
    liquidity_taker: &str,
) -> Result<TradeSide> {
    if SPECIAL_WALLETS.contains(&fill_row.taker.as_str()) {
        anyhow::bail!("Taker is special");
    }
    if SPECIAL_WALLETS.contains(&fill_row.maker.as_str()) {
        anyhow::bail!("Maker is special");
    }
    if fill_row.maker == liquidity_taker {
        anyhow::bail!("Maker is liquidity taker");
    }
    if fill_row.taker != liquidity_taker {
        anyhow::bail!("Taker is not liquidity taker");
    }

    Ok(parse_fill_row_from_maker_perspective(fill_row))
}

fn parse_special_fill_row(fill_row: &IntermediateRow) -> Result<TradeSide> {
    if !SPECIAL_WALLETS.contains(&fill_row.taker.as_str()) {
        anyhow::bail!("Expected taker to be special, got {}", fill_row.taker);
    }
    Ok(parse_fill_row_from_maker_perspective(fill_row))
}

fn parse_fill_row_from_maker_perspective(fill_row: &IntermediateRow) -> TradeSide {
    let maker_amt = fill_row.maker_amount_filled / 1e6;
    let taker_amt = fill_row.taker_amount_filled / 1e6;

    let action;
    let shares;
    let usdc;
    let outcome;

    if fill_row.maker_asset_id == "0" {
        // Maker pays cash (buying)
        action = "bought".to_string();
        usdc = maker_amt;
        shares = taker_amt;
        outcome = fill_row.taker_asset_id.clone();
    } else {
        // Maker pays shares (selling)
        action = "sold".to_string();
        shares = maker_amt;
        usdc = taker_amt;
        outcome = fill_row.maker_asset_id.clone();
    }

    let price_usd = if shares > 0.0 { usdc / shares } else { 0.0 };
    let price_cents = price_usd * 100.0;

    TradeSide {
        user: fill_row.maker.clone(),
        action,
        shares,
        outcome,
        price: price_cents,
        usdc,
        order_hash: fill_row.order_hash.clone(),
    }
}
