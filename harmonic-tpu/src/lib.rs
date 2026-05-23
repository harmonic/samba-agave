//! Harmonic TPU Service for Firedancer Bundle Integration
//!
//! This service receives TPU connection status updates from the bundle tile
//! (via fd_ext_tpu_update callback) and updates the node's gossip contact info
//! to advertise either the remote TPU or the local TPU address.

use {
    log::{error, info, warn},
    solana_gossip::cluster_info::ClusterInfo,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, OnceLock,
        },
    },
};

/// Status values matching FD_BUNDLE_TPU_UPDATE_* in fd_bundle_tpu.h
const TPU_STATUS_DISCONNECTED: i32 = 0;
const TPU_STATUS_CONNECTED: i32 = 1;

/// QUIC TPU port offset relative to UDP TPU port (matches Agave's port layout
/// in `gossip::contact_info::Node::new_with_external_ip`).
const TPU_QUIC_PORT_OFFSET: u16 = 6;

fn quic_addr_for(udp_addr: SocketAddr) -> SocketAddr {
    let mut quic = udp_addr;
    quic.set_port(udp_addr.port().wrapping_add(TPU_QUIC_PORT_OFFSET));
    quic
}

/// Global state for the TPU update handler.  All TPU/TPU-forwards addresses
/// stored here are UDP addresses; the matching QUIC addresses are derived as
/// `udp_port + TPU_QUIC_PORT_OFFSET`.
struct TpuUpdateState {
    cluster_info: Arc<ClusterInfo>,
    local_tpu_udp_addr: SocketAddr,
    local_tpu_forwards_udp_addr: SocketAddr,
    last_status: AtomicBool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TpuUpdate {
    is_connected: bool,
    tpu_udp_addr: SocketAddr,
    tpu_forwards_udp_addr: SocketAddr,
}

struct TpuUpdateGlobal {
    state: Option<TpuUpdateState>,
    /// Updates received before HarmonicTpuService::new; applied on init.
    pending: Option<TpuUpdate>,
}

static TPU_UPDATE_GLOBAL: OnceLock<Mutex<TpuUpdateGlobal>> = OnceLock::new();

fn get_tpu_update_global() -> &'static Mutex<TpuUpdateGlobal> {
    TPU_UPDATE_GLOBAL.get_or_init(|| {
        Mutex::new(TpuUpdateGlobal {
            state: None,
            pending: None,
        })
    })
}

fn parse_tpu_update(
    status: i32,
    tpu_ip4_addr: u32,
    tpu_port: u16,
    tpu_fwd_ip4_addr: u32,
    tpu_fwd_port: u16,
) -> Option<TpuUpdate> {
    let tpu_ip = Ipv4Addr::from(u32::from_be(tpu_ip4_addr));
    let tpu_fwd_ip = Ipv4Addr::from(u32::from_be(tpu_fwd_ip4_addr));
    let is_connected = match status {
        TPU_STATUS_CONNECTED => true,
        TPU_STATUS_DISCONNECTED => false,
        _ => {
            warn!("Ignoring unknown TPU status: {}", status);
            return None;
        }
    };
    Some(TpuUpdate {
        is_connected,
        tpu_udp_addr: SocketAddr::new(tpu_ip.into(), tpu_port),
        tpu_forwards_udp_addr: SocketAddr::new(tpu_fwd_ip.into(), tpu_fwd_port),
    })
}

fn connected_addrs_valid(update: &TpuUpdate) -> bool {
    update.tpu_udp_addr.port() != 0
        && update.tpu_udp_addr.ip() != IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

/// Apply TPU and TPU-forwards UDP addresses (and their derived QUIC addresses)
/// to the node's gossip contact info.  Logs and continues on any individual
/// setter failure; returns to the caller regardless.
fn apply_tpu_addrs(
    cluster_info: &ClusterInfo,
    tpu_udp_addr: SocketAddr,
    tpu_forwards_udp_addr: SocketAddr,
) {
    let tpu_quic_addr = quic_addr_for(tpu_udp_addr);
    let tpu_forwards_quic_addr = quic_addr_for(tpu_forwards_udp_addr);

    if let Err(e) = cluster_info.set_tpu_udp(tpu_udp_addr) {
        error!("Failed to set TPU UDP address in gossip: {:?}", e);
    }
    if let Err(e) = cluster_info.set_tpu_quic(tpu_quic_addr) {
        error!("Failed to set TPU QUIC address in gossip: {:?}", e);
    }
    if let Err(e) = cluster_info.set_tpu_forwards_udp(tpu_forwards_udp_addr) {
        error!("Failed to set TPU forwards UDP address in gossip: {:?}", e);
    }
    if let Err(e) = cluster_info.set_tpu_forwards_quic(tpu_forwards_quic_addr) {
        error!("Failed to set TPU forwards QUIC address in gossip: {:?}", e);
    }
}

fn apply_tpu_update(state: &TpuUpdateState, update: &TpuUpdate) {
    if update.is_connected {
        info!(
            "Bundle TPU connected, updating gossip: tpu_udp={}, tpu_quic={}, tpu_fwd_udp={}, \
             tpu_fwd_quic={}",
            update.tpu_udp_addr,
            quic_addr_for(update.tpu_udp_addr),
            update.tpu_forwards_udp_addr,
            quic_addr_for(update.tpu_forwards_udp_addr),
        );

        apply_tpu_addrs(
            &state.cluster_info,
            update.tpu_udp_addr,
            update.tpu_forwards_udp_addr,
        );
    } else {
        info!(
            "Bundle TPU disconnected, reverting to local: tpu_udp={}, tpu_quic={}, \
             tpu_fwd_udp={}, tpu_fwd_quic={}",
            state.local_tpu_udp_addr,
            quic_addr_for(state.local_tpu_udp_addr),
            state.local_tpu_forwards_udp_addr,
            quic_addr_for(state.local_tpu_forwards_udp_addr),
        );

        apply_tpu_addrs(
            &state.cluster_info,
            state.local_tpu_udp_addr,
            state.local_tpu_forwards_udp_addr,
        );
    }
}

fn store_pending_update(global: &mut TpuUpdateGlobal, update: TpuUpdate) {
    if update.is_connected {
        if connected_addrs_valid(&update) {
            global.pending = Some(update);
        }
    } else {
        global.pending = None;
    }
}

fn apply_pending_update(global: &mut TpuUpdateGlobal) {
    let Some(pending) = global.pending.take() else {
        return;
    };

    let Some(state) = global.state.as_ref() else {
        global.pending = Some(pending);
        return;
    };

    let was_connected = state.last_status.load(Ordering::Relaxed);
    if pending.is_connected == was_connected {
        return;
    }

    state.last_status.store(pending.is_connected, Ordering::Relaxed);
    apply_tpu_update(state, &pending);
}

/// Called from C (fd_pohh_tile.c) when the bundle tile sends a TPU update.
/// This function is called from the poh tile thread, so it must be thread-safe.
///
/// `tpu_port` and `tpu_fwd_port` are UDP ports; the QUIC ports advertised in
/// gossip are derived as `udp_port + TPU_QUIC_PORT_OFFSET`.
///
/// cavey TODO: technically this is fallible, but we just log errors rn.
/// discoh/pohh assumes this succeeds.
#[no_mangle]
pub extern "C" fn fd_ext_tpu_update(
    status: i32,
    tpu_ip4_addr: u32,
    tpu_port: u16,
    tpu_fwd_ip4_addr: u32,
    tpu_fwd_port: u16,
) {
    let Some(update) = parse_tpu_update(
        status,
        tpu_ip4_addr,
        tpu_port,
        tpu_fwd_ip4_addr,
        tpu_fwd_port,
    ) else {
        return;
    };

    let global_lock = get_tpu_update_global();
    let mut global = match global_lock.lock() {
        Ok(g) => g,
        Err(e) => {
            error!("Failed to lock TPU update state: {}", e);
            return;
        }
    };

    let Some(state) = global.state.as_ref() else {
        store_pending_update(&mut global, update);
        return;
    };

    let was_connected = state.last_status.load(Ordering::Relaxed);
    if update.is_connected == was_connected {
        // No change
        return;
    }

    state.last_status.store(update.is_connected, Ordering::Relaxed);
    apply_tpu_update(state, &update);
}

/// Configuration for the Harmonic TPU Service.
///
/// Both addresses are UDP TPU addresses; the matching QUIC addresses are
/// derived as `udp_port + TPU_QUIC_PORT_OFFSET` and advertised alongside the
/// UDP addresses on every gossip update.
pub struct HarmonicTpuServiceConfig {
    /// The local TPU UDP address to revert to when disconnected.
    pub local_tpu_udp_addr: SocketAddr,
    /// The local TPU forwards UDP address to revert to when disconnected.
    pub local_tpu_forwards_udp_addr: SocketAddr,
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
            "Harmonic TPU Service initialized: local_tpu_udp={}, local_tpu_quic={}, \
             local_tpu_fwd_udp={}, local_tpu_fwd_quic={}",
            config.local_tpu_udp_addr,
            quic_addr_for(config.local_tpu_udp_addr),
            config.local_tpu_forwards_udp_addr,
            quic_addr_for(config.local_tpu_forwards_udp_addr),
        );

        let state = TpuUpdateState {
            cluster_info,
            local_tpu_udp_addr: config.local_tpu_udp_addr,
            local_tpu_forwards_udp_addr: config.local_tpu_forwards_udp_addr,
            last_status: AtomicBool::new(false),
        };

        let global_lock = get_tpu_update_global();
        if let Ok(mut global) = global_lock.lock() {
            global.state = Some(state);
            apply_pending_update(&mut global);
        } else {
            warn!("Failed to initialize TPU update state");
        }

        Self {}
    }

    pub fn join(self) {
        // Clean up global state
        let global_lock = get_tpu_update_global();
        if let Ok(mut global) = global_lock.lock() {
            global.state = None;
            global.pending = None;
        }
        info!("Harmonic TPU Service stopped");
    }
}
