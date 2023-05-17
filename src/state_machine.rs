use cita_cloud_proto::status_code::StatusCodeEnum;
use prost::Message;
use statig::prelude::*;
use std::{cmp::Ordering, fmt::Debug, ops::Deref};

use crate::{
    chain::Chain,
    crypto::hash_data,
    grpc_client::{consensus::reconfigure, storage::get_full_block},
    node_manager::{ChainStatusInit, NodeAddress},
    protocol::sync_manager::{sync_block_respond::Respond, SyncBlockRequest, SyncBlocks},
    SyncBlockRespond,
};

#[derive(Clone)]
pub struct ControllerStateMachine(Chain);

impl Deref for ControllerStateMachine {
    type Target = Chain;

    fn deref(&self) -> &Chain {
        &self.0
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Event {
    // chain status respond
    SyncBlockReq(SyncBlockRequest, u64),
    // multicast sync block request
    SyncBlock,
    // broadcast chain status init
    BroadCastCSI,
    // record all node chain status
    RecordAllNode,
}

#[state_machine(
    initial = "State::participate_in_consensus()",
    state(derive(Debug, Clone)),
    superstate(derive(Debug, Clone)),
    on_transition = "Self::on_transition",
    on_dispatch = "Self::on_dispatch"
)]
impl ControllerStateMachine {
    #[superstate]
    async fn online(&self, event: &Event) -> Response<State> {
        match event {
            Event::SyncBlockReq(req, origin) => {
                let mut block_vec = Vec::new();

                for h in req.start_height..=req.end_height {
                    if let Ok(block) = get_full_block(h).await {
                        block_vec.push(block);
                    } else {
                        warn!("handle SyncBlockReq failed: get block({}) failed", h);
                        break;
                    }
                }

                if block_vec.len() as u64 != req.end_height - req.start_height + 1 {
                    let sync_block_respond = SyncBlockRespond {
                        respond: Some(Respond::MissBlock(self.local_address.clone())),
                    };
                    self.unicast_sync_block_respond(*origin, sync_block_respond)
                        .await;
                } else {
                    info!(
                        "send SyncBlockRespond: to origin: {:x}, height: {} - {}",
                        origin, req.start_height, req.end_height
                    );
                    let sync_block = SyncBlocks {
                        address: Some(self.local_address.clone()),
                        sync_blocks: block_vec,
                    };
                    let sync_block_respond = SyncBlockRespond {
                        respond: Some(Respond::Ok(sync_block)),
                    };
                    self.unicast_sync_block_respond(*origin, sync_block_respond)
                        .await;
                }
            }
            Event::SyncBlock => {
                debug!("receive SyncBlock event");
                let (global_address, global_status) = self.get_global_status().await;
                let mut own_status = self.get_status().await;
                // get chain lock means syncing
                if self.need_sync(&global_status).await {
                    let mut syncing = false;
                    while let Some((addr, block)) =
                        self.sync_manager.remove_block(own_status.height + 1).await
                    {
                        self.clear_candidate().await;
                        match self.process_block(block).await {
                            Ok((consensus_config, mut status)) => {
                                reconfigure(consensus_config).await.is_success().unwrap();
                                status.address = Some(self.local_address.clone());
                                self.set_status(status.clone()).await;
                                own_status = status.clone();
                                if status.height % self.config.send_chain_status_interval_sync == 0
                                {
                                    self.broadcast_chain_status(status).await;
                                }
                                syncing = true;
                            }
                            Err(e) => {
                                if (e as u64) % 100 == 0 {
                                    warn!("sync block failed: {}", e.to_string());
                                    continue;
                                }
                                warn!(
                                    "sync block failed: {}. set remote misbehavior. origin: {}",
                                    NodeAddress::from(&addr),
                                    e.to_string()
                                );
                                let del_node_addr = NodeAddress::from(&addr);
                                let _ =
                                    self.node_manager.set_misbehavior_node(&del_node_addr).await;
                                if global_address == del_node_addr {
                                    let (ex_addr, ex_status) = self.node_manager.pick_node().await;
                                    self.update_global_status(ex_addr, ex_status).await;
                                }
                                if let Some(range_heights) =
                                    self.sync_manager.clear_node_block(&addr, &own_status).await
                                {
                                    let (global_address, global_status) =
                                        self.get_global_status().await;
                                    if global_address.0 != 0 {
                                        for range_height in range_heights {
                                            if let Some(reqs) = self
                                                .sync_manager
                                                .re_sync_block_req(range_height, &global_status)
                                            {
                                                for req in reqs {
                                                    self.unicast_sync_block(global_address.0, req)
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    syncing = true;
                                }
                            }
                        }
                    }
                    if syncing {
                        self.sync_block().await.unwrap();
                    }
                    return Transition(State::sync());
                } else {
                    return Transition(State::participate_in_consensus());
                }
            }
            Event::BroadCastCSI => {
                info!("receive BroadCastCSI event");
                let status = self.get_status().await;

                let mut chain_status_bytes = Vec::new();
                status
                    .encode(&mut chain_status_bytes)
                    .map_err(|_| {
                        warn!("process BroadCastCSI failed: encode ChainStatus failed");
                        StatusCodeEnum::EncodeError
                    })
                    .unwrap();
                let msg_hash = hash_data(&chain_status_bytes);

                #[cfg(feature = "sm")]
                let crypto = crypto_sm::crypto::Crypto::new(&self.private_key_path);
                #[cfg(feature = "eth")]
                let crypto = crypto_eth::crypto::Crypto::new(&self.private_key_path);

                let signature = crypto.sign_message(&msg_hash).unwrap();

                self.broadcast_chain_status_init(ChainStatusInit {
                    chain_status: Some(status),
                    signature,
                })
                .await
                .await
                .unwrap();
            }
            Event::RecordAllNode => {
                let nodes = self.node_manager.nodes.read().await.clone();
                for (na, current_cs) in nodes.iter() {
                    if let Some(old_cs) = self
                        .node_manager
                        .nodes_pre_status
                        .read()
                        .await
                        .clone()
                        .get(na)
                    {
                        match old_cs.height.cmp(&current_cs.height) {
                            Ordering::Greater => {
                                error!(
                                    "node status rollbacked: old height: {}, current height: {}. set it misbehavior. origin: {}",
                                    old_cs.height,
                                    current_cs.height,
                                    &na
                                );
                                let _ = self.node_manager.set_misbehavior_node(na).await;
                            }
                            Ordering::Equal => {
                                warn!(
                                    "node status stale: height: {}. delete it. origin: {}",
                                    old_cs.height, &na
                                );
                                if self.node_manager.in_node(na).await {
                                    self.node_manager.delete_node(na).await;
                                }
                            }
                            Ordering::Less => {
                                // update node in old status
                                self.node_manager
                                    .nodes_pre_status
                                    .write()
                                    .await
                                    .insert(*na, current_cs.clone());
                            }
                        }
                    } else {
                        self.node_manager
                            .nodes_pre_status
                            .write()
                            .await
                            .insert(*na, current_cs.clone());
                    }
                }
            }
        }
        Super
    }

    #[state(superstate = "online")]
    async fn participate_in_consensus(event: &Event) -> Response<State> {
        match event {
            _ => Super,
        }
    }

    #[state(superstate = "online", exit_action = "exit_sync")]
    async fn sync(event: &Event) -> Response<State> {
        match event {
            _ => Super,
        }
    }

    #[action]
    async fn exit_sync(&self) {
        self.sync_manager.clear().await;
    }
}

impl ControllerStateMachine {
    pub fn new(chain: Chain) -> Self {
        ControllerStateMachine(chain)
    }

    fn on_transition(&mut self, source: &State, target: &State) {
        info!("transitioned from `{source:?}` to `{target:?}`");
    }

    fn on_dispatch(&mut self, state: StateOrSuperstate<Self>, event: &Event) {
        info!("dispatching `{event:?}` to `{state:?}`");
    }
}

#[tokio::test]
async fn test() {
    // let future = async {
    //     let mut state_machine = ControllerStateMachine::default()
    //         .uninitialized_state_machine()
    //         .init()
    //         .await;

    //     state_machine.handle(&Event::TimerElapsed).await;
    // };

    // let _ = tokio::spawn(future).await;
}
