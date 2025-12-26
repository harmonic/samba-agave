//! Harmonic TPU Service for Firedancer Bundle Integration
//!
//! This service receives TPU connection status updates from the bundle tile
//! (via fd_ext_tpu_update callback) and updates the node's gossip contact info
//! to advertise either the remote TPU or the local TPU address.

use {
    log::{error, info, warn},
    solana_gossip::cluster_info::ClusterInfo,
    std::{
        net::{Ipv4Addr, SocketAddr},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, OnceLock,
        },
    },
};

/// Status values matching FD_BUNDLE_TPU_UPDATE_* in fd_bundle_tpu.h
const TPU_STATUS_DISCONNECTED: i32 = 0;
const TPU_STATUS_CONNECTED: i32 = 1;

/// Global state for the TPU update handler
struct TpuUpdateState {
    cluster_info: Arc<ClusterInfo>,
    local_tpu_addr: SocketAddr,
    local_tpu_fwd_addr: SocketAddr,
    last_status: AtomicBool,
}

static TPU_UPDATE_STATE: OnceLock<Mutex<Option<TpuUpdateState>>> = OnceLock::new();

fn get_tpu_update_state() -> &'static Mutex<Option<TpuUpdateState>> {
    TPU_UPDATE_STATE.get_or_init(|| Mutex::new(None))
}

/// Called from C (fd_poh_tile.c) when the bundle tile sends a TPU update.
/// This function is called from the poh tile thread, so it must be thread-safe.
/// 
/// cavey TODO: technically this is fallible, but we just log errors rn. discoh/poh
/// assume this succeeds.
#[no_mangle]
pub extern "C" fn fd_ext_tpu_update(
    status: i32,
    tpu_ip4_addr: u32,
    tpu_port: u16,
    tpu_fwd_ip4_addr: u32,
    tpu_fwd_port: u16,
) {
    let state_lock = get_tpu_update_state();
    let guard = match state_lock.lock() {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to lock TPU update state: {}", e);
            return;
        }
    };

    let state = match guard.as_ref() {
        Some(s) => s,
        None => {
            // Service not initialized yet, ignore
            return;
}
    };

    let was_connected = state.last_status.load(Ordering::Relaxed);
    let is_connected = status == TPU_STATUS_CONNECTED;

    if is_connected == was_connected {
        // No change
        return;
    }

    state.last_status.store(is_connected, Ordering::Relaxed);

    if status == TPU_STATUS_CONNECTED {
        // Convert network byte order IP to SocketAddr
        let tpu_ip = Ipv4Addr::from(u32::from_be(tpu_ip4_addr));
        let tpu_fwd_ip = Ipv4Addr::from(u32::from_be(tpu_fwd_ip4_addr));
        let tpu_addr = SocketAddr::new(tpu_ip.into(), tpu_port);
        let tpu_fwd_addr = SocketAddr::new(tpu_fwd_ip.into(), tpu_fwd_port);

        info!(
            "Bundle TPU connected, updating gossip: tpu={}, tpu_fwd={}",
            tpu_addr, tpu_fwd_addr
        );

        if let Err(e) = state.cluster_info.set_tpu(tpu_addr) {
            error!("Failed to set TPU address in gossip: {:?}", e);
        }
        if let Err(e) = state.cluster_info.set_tpu_forwards(tpu_fwd_addr) {
            error!("Failed to set TPU forwards address in gossip: {:?}", e);
        }
    } else if status == TPU_STATUS_DISCONNECTED {
        info!(
            "Bundle TPU disconnected, reverting to local: tpu={}, tpu_fwd={}",
            state.local_tpu_addr, state.local_tpu_fwd_addr
        );

        if let Err(e) = state.cluster_info.set_tpu(state.local_tpu_addr) {
            error!("Failed to revert TPU address in gossip: {:?}", e);
        }
        if let Err(e) = state.cluster_info.set_tpu_forwards(state.local_tpu_fwd_addr) {
            error!("Failed to revert TPU forwards address in gossip: {:?}", e);
        }
    }
}

/// Configuration for the Harmonic TPU Service
pub struct HarmonicTpuServiceConfig {
    /// The local TPU address to revert to when disconnected
    pub local_tpu_addr: SocketAddr,
    /// The local TPU forwards address to revert to when disconnected
    pub local_tpu_forwards_addr: SocketAddr,
}

/// Service that manages TPU address in gossip based on bundle connection status.
/// 
/// This service initializes global state that is used by the fd_ext_tpu_update
/// callback. The callback is invoked from the poh tile when the bundle tile
/// sends a TPU connection update.
pub struct HarmonicTpuService {
    // No thread needed - updates come via callback
}

impl HarmonicTpuService {
    pub fn new(config: HarmonicTpuServiceConfig, cluster_info: Arc<ClusterInfo>) -> Self {
        info!(
            "Harmonic TPU Service initialized: local_tpu={}, local_tpu_fwd={}",
            config.local_tpu_addr, config.local_tpu_forwards_addr
        );

        let state = TpuUpdateState {
            cluster_info,
            local_tpu_addr: config.local_tpu_addr,
            local_tpu_fwd_addr: config.local_tpu_forwards_addr,
            last_status: AtomicBool::new(false),
        };

        let state_lock = get_tpu_update_state();
        if let Ok(mut guard) = state_lock.lock() {
            *guard = Some(state);
                            } else {
            warn!("Failed to initialize TPU update state");
                        }

        Self {}
    }

    pub fn join(self) {
        // Clean up global state
        let state_lock = get_tpu_update_state();
        if let Ok(mut guard) = state_lock.lock() {
            *guard = None;
        }
        info!("Harmonic TPU Service stopped");
    }
}

