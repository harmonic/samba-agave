/// Benchmark: batch of 16 noop transactions vs 16 individual dispatches.
/// Measures per-batch dispatch overhead.
///
/// Run with:
///   cargo bench -p solana-core --bench bench_tx_dispatch --features dev-context-only-utils
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use solana_clock::MAX_PROCESSING_AGE;
use solana_keypair::Keypair;
use solana_message::Message;
use solana_runtime::{
    bank::Bank,
    bank_forks::BankForks,
    genesis_utils::{create_genesis_config, GenesisConfigInfo},
};
use solana_signer::Signer;
use solana_svm::transaction_processor::ExecutionRecordingConfig;
use solana_svm_timings::{ExecuteTimingType, ExecuteTimings};
use solana_transaction::{versioned::VersionedTransaction, Transaction};

const NUM_TXS: usize = 16;
const ITERATIONS: usize = 200;

fn make_noop_tx(payer: &Keypair, recent_blockhash: solana_hash::Hash) -> Transaction {
    let message = Message::new(&[], Some(&payer.pubkey()));
    Transaction::new(&[payer], message, recent_blockhash)
}

fn setup() -> (solana_genesis_config::GenesisConfig, Keypair, Vec<Keypair>) {
    let GenesisConfigInfo {
        mut genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(u64::MAX / 2);
    genesis_config.fee_rate_governor = solana_fee_calculator::FeeRateGovernor::new(0, 0);
    let payers: Vec<Keypair> = (0..NUM_TXS).map(|_| Keypair::new()).collect();
    (genesis_config, mint_keypair, payers)
}

fn fresh_bank(
    gc: &solana_genesis_config::GenesisConfig,
    mk: &Keypair,
    payers: &[Keypair],
) -> (Arc<Bank>, Arc<RwLock<BankForks>>) {
    let (bank, bf) = Bank::new_with_bank_forks_for_tests(gc);
    let bh = bank.last_blockhash();
    for p in payers {
        bank.process_transaction(&solana_system_transaction::transfer(
            mk,
            &p.pubkey(),
            1_000_000_000,
            bh,
        ))
        .unwrap();
    }
    (bank, bf)
}

#[derive(Default, Clone)]
struct Phases {
    prepare: Duration,
    load_exec_commit: Duration,
    unlock: Duration,
}
impl Phases {
    fn total(&self) -> Duration {
        self.prepare + self.load_exec_commit + self.unlock
    }
    fn add(&mut self, o: &Phases) {
        self.prepare += o.prepare;
        self.load_exec_commit += o.load_exec_commit;
        self.unlock += o.unlock;
    }
}

fn dispatch_timed(
    bank: &Bank,
    txs: Vec<VersionedTransaction>,
    timings: &mut ExecuteTimings,
) -> Phases {
    let t0 = Instant::now();
    let batch = bank.prepare_entry_batch(txs).unwrap();
    let prepare = t0.elapsed();

    let t1 = Instant::now();
    let (results, _) = bank.load_execute_and_commit_transactions(
        &batch,
        MAX_PROCESSING_AGE,
        ExecutionRecordingConfig::new_single_setting(false),
        timings,
        None,
    );
    let load_exec_commit = t1.elapsed();

    for (i, r) in results.iter().enumerate() {
        assert!(
            r.as_ref().map(|c| c.status.is_ok()).unwrap_or(false),
            "tx {i} failed: {r:?}"
        );
    }

    let t2 = Instant::now();
    drop(batch);
    let unlock = t2.elapsed();

    Phases {
        prepare,
        load_exec_commit,
        unlock,
    }
}

const FIELDS: &[(&str, ExecuteTimingType)] = &[
    ("check", ExecuteTimingType::CheckUs),
    ("validate_fees", ExecuteTimingType::ValidateFeesUs),
    ("load", ExecuteTimingType::LoadUs),
    ("execute", ExecuteTimingType::ExecuteUs),
    ("store", ExecuteTimingType::StoreUs),
    ("update_stakes", ExecuteTimingType::UpdateStakesCacheUs),
    ("update_executors", ExecuteTimingType::UpdateExecutorsUs),
    ("collect_logs", ExecuteTimingType::CollectLogsUs),
    ("update_tx_stat", ExecuteTimingType::UpdateTransactionStatuses),
    ("program_cache", ExecuteTimingType::ProgramCacheUs),
    ("filter_exec", ExecuteTimingType::FilterExecutableUs),
    ("collect_bal", ExecuteTimingType::CollectBalancesUs),
];

fn main() {
    let (gc, mk, payers) = setup();
    let n = ITERATIONS as u32;

    // ── Part 1: top-level comparison ────────────────────────────────────
    println!("\n=== Top-level ({NUM_TXS} txs, {ITERATIONS} iters) ===\n");
    {
        let mut batch_times = Vec::with_capacity(ITERATIONS);
        let mut individual_times = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let (bank, _bf) = fresh_bank(&gc, &mk, &payers);
            let bh = bank.last_blockhash();
            let txs: Vec<Transaction> = payers.iter().map(|p| make_noop_tx(p, bh)).collect();
            let s = Instant::now();
            bank.process_transactions(txs.iter())
                .iter()
                .for_each(|r| assert!(r.is_ok()));
            batch_times.push(s.elapsed());
        }
        for _ in 0..ITERATIONS {
            let (bank, _bf) = fresh_bank(&gc, &mk, &payers);
            let bh = bank.last_blockhash();
            let txs: Vec<Transaction> = payers.iter().map(|p| make_noop_tx(p, bh)).collect();
            let s = Instant::now();
            txs.iter()
                .for_each(|tx| assert!(bank.process_transaction(tx).is_ok()));
            individual_times.push(s.elapsed());
        }
        batch_times.sort();
        individual_times.sort();
        let mean = |v: &[Duration]| v.iter().sum::<Duration>() / v.len() as u32;
        println!("Batch of 16:     mean {:>10.2?}", mean(&batch_times));
        println!("16 x individual: mean {:>10.2?}", mean(&individual_times));
        println!(
            "Overhead ratio:  {:.2}x",
            mean(&individual_times).as_nanos() as f64 / mean(&batch_times).as_nanos() as f64
        );
    }

    // ── Part 2: phase breakdown ─────────────────────────────────────────
    let mut bp = Phases::default();
    let mut bt = ExecuteTimings::default();
    for _ in 0..ITERATIONS {
        let (bank, _bf) = fresh_bank(&gc, &mk, &payers);
        let bh = bank.last_blockhash();
        let vtxs: Vec<VersionedTransaction> = payers
            .iter()
            .map(|p| VersionedTransaction::from(make_noop_tx(p, bh)))
            .collect();
        bp.add(&dispatch_timed(&bank, vtxs, &mut bt));
    }

    let mut ip = Phases::default();
    let mut it = ExecuteTimings::default();
    for _ in 0..ITERATIONS {
        let (bank, _bf) = fresh_bank(&gc, &mk, &payers);
        let bh = bank.last_blockhash();
        for p in &payers {
            let vtx = VersionedTransaction::from(make_noop_tx(p, bh));
            ip.add(&dispatch_timed(&bank, vec![vtx], &mut it));
        }
    }

    let batch_dispatches = ITERATIONS;
    let indiv_dispatches = ITERATIONS * NUM_TXS;

    println!("\n=== Phase breakdown (mean per iteration) ===\n");
    println!(
        "{:<18} {:>10} {:>14} {:>10} {:>10}",
        "", "prepare", "load+exec+cmit", "unlock", "TOTAL"
    );
    println!(
        "{:<18} {:>10.2?} {:>14.2?} {:>10.2?} {:>10.2?}",
        "Batch of 16:",
        bp.prepare / n,
        bp.load_exec_commit / n,
        bp.unlock / n,
        bp.total() / n,
    );
    println!(
        "{:<18} {:>10.2?} {:>14.2?} {:>10.2?} {:>10.2?}",
        "16 x individual:",
        ip.prepare / n,
        ip.load_exec_commit / n,
        ip.unlock / n,
        ip.total() / n,
    );
    println!(
        "{:<18} {:>10.2}x {:>14.2}x {:>10.2}x {:>10.2}x",
        "Ratio:",
        ip.prepare.as_nanos() as f64 / bp.prepare.as_nanos() as f64,
        ip.load_exec_commit.as_nanos() as f64 / bp.load_exec_commit.as_nanos() as f64,
        ip.unlock.as_nanos() as f64 / bp.unlock.as_nanos() as f64,
        ip.total().as_nanos() as f64 / bp.total().as_nanos() as f64,
    );

    // ── Part 3: ExecuteTimings absolute cost ────────────────────────────
    println!("\n=== Absolute cost per iteration (us): 1 batch of 16 vs 16 x batch of 1 ===\n");
    println!(
        "{:<18} {:>12} {:>12} {:>12}",
        "field", "1x16", "16x1", "delta"
    );
    for &(name, field) in FIELDS {
        let bv = bt.metrics[field].0 as f64 / batch_dispatches as f64;
        let iv = it.metrics[field].0 as f64 / indiv_dispatches as f64 * NUM_TXS as f64;
        println!(
            "{:<18} {:>12.2} {:>12.2} {:>+12.2}",
            name, bv, iv, iv - bv
        );
    }

    let bw = bp.total().as_micros() as f64 / batch_dispatches as f64;
    let iw = ip.total().as_micros() as f64 / indiv_dispatches as f64 * NUM_TXS as f64;
    println!(
        "{:<18} {:>12.2} {:>12.2} {:>+12.2}",
        "WALL CLOCK", bw, iw, iw - bw
    );
}
