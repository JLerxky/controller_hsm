// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prost::Message;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, RwLock},
    time,
};

use cita_cloud_proto::{
    blockchain::{
        raw_transaction::Tx, Block, BlockHeader, CompactBlock, CompactBlockBody, RawTransaction,
        RawTransactions,
    },
    client::NetworkClientTrait,
    common::{
        Address, ConsensusConfiguration, Empty, Hash, Hashes, NodeNetInfo, NodeStatus, PeerStatus,
        Proof, ProposalInner, StateRoot,
    },
    controller::{BlockNumber, CrossChainProof},
    network::NetworkMsg,
    status_code::StatusCodeEnum,
    storage::Regions,
};
use cloud_util::{
    clean_0x,
    common::extract_compact,
    common::{get_tx_hash, h160_address_check},
    storage::load_data,
    unix_now,
};

use crate::{
    auth::Authentication,
    config::ControllerConfig,
    controller_msg_type::ControllerMsgType,
    crypto::{check_transactions, get_block_hash, hash_data},
    grpc_client::{
        consensus::{check_block, reconfigure},
        executor::{exec_block, get_receipt_proof},
        network::{get_network_status, get_peers_info},
        network_client,
        storage::{
            assemble_proposal, db_get_tx, get_compact_block, get_full_block,
            get_height_by_block_hash, get_proof, get_state_root, load_tx_info, store_data,
        },
        storage_client,
    },
    node_manager::{
        chain_status_respond::Respond, ChainStatus, ChainStatusInit, ChainStatusRespond,
        NodeAddress, NodeManager,
    },
    pool::Pool,
    protocol::sync_manager::{
        SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager, SyncTxRequest, SyncTxRespond,
    },
    state_machine::Event,
    system_config::{SystemConfig, LOCK_ID_BLOCK_LIMIT, LOCK_ID_QUOTA_LIMIT},
    util::*,
    GenesisBlock, {impl_broadcast, impl_multicast, impl_unicast},
};

#[allow(dead_code, clippy::type_complexity)]
#[derive(Clone)]
pub struct Chain {
    pub block_number: Arc<RwLock<u64>>,

    pub block_hash: Arc<RwLock<Vec<u8>>>,
    // elements is block_hash
    pub candidates: Arc<RwLock<HashSet<Vec<u8>>>>,

    pub own_proposal: Arc<RwLock<Option<(u64, Vec<u8>)>>>,

    pub pool: Arc<RwLock<Pool>>,

    pub auth: Arc<RwLock<Authentication>>,

    pub genesis: GenesisBlock,

    pub config: ControllerConfig,

    pub local_address: Address,

    pub validator_address: Address,

    pub current_status: Arc<RwLock<ChainStatus>>,

    pub global_status: Arc<RwLock<(NodeAddress, ChainStatus)>>,

    pub node_manager: NodeManager,

    pub sync_manager: SyncManager,

    pub task_sender: mpsc::Sender<Event>,

    pub forward_pool: Arc<RwLock<RawTransactions>>,

    pub initial_sys_config: SystemConfig,

    pub init_block_number: u64,
    // sync state flag
    is_sync: Arc<RwLock<bool>>,

    pub private_key_path: String,
}

// rpc
impl Chain {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: ControllerConfig,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        sys_config: SystemConfig,
        genesis: GenesisBlock,
        task_sender: mpsc::Sender<Event>,
        initial_sys_config: SystemConfig,
        private_key_path: String,
    ) -> Self {
        let node_address = hex::decode(clean_0x(&config.node_address)).unwrap();
        info!("node address: {}", &config.node_address);
        //
        let validator_address = hex::decode(&clean_0x(&config.validator_address)[..40]).unwrap();
        info!("validator address: {}", &config.validator_address[..40]);

        h160_address_check(Some(&Address {
            address: node_address.clone(),
        }))
        .unwrap();

        let own_status = ChainStatus {
            version: sys_config.version,
            chain_id: sys_config.chain_id.clone(),
            height: 0,
            hash: Some(Hash {
                hash: current_block_hash.clone(),
            }),
            address: Some(Address {
                address: node_address.clone(),
            }),
        };

        let pool = Arc::new(RwLock::new(Pool::new(
            sys_config.block_limit,
            sys_config.quota_limit,
        )));
        let auth = Arc::new(RwLock::new(Authentication::new(sys_config)));

        Chain {
            sync_manager: SyncManager::new(&config),
            node_manager: NodeManager::default(),
            config,
            auth,
            pool,
            local_address: Address {
                address: node_address,
            },
            validator_address: Address {
                address: validator_address,
            },
            current_status: Arc::new(RwLock::new(own_status)),
            global_status: Arc::new(RwLock::new((NodeAddress(0), ChainStatus::default()))),
            task_sender,
            forward_pool: Arc::new(RwLock::new(RawTransactions { body: vec![] })),
            initial_sys_config,
            init_block_number: current_block_number,
            block_number: Arc::new(RwLock::new(current_block_number)),
            block_hash: Arc::new(RwLock::new(current_block_hash)),
            candidates: Arc::new(RwLock::new(HashSet::new())),
            own_proposal: Arc::new(RwLock::new(None)),
            genesis,
            is_sync: Arc::new(RwLock::new(false)),
            private_key_path,
        }
    }

    pub async fn init(&self, init_block_number: u64, sys_config: SystemConfig) {
        let sys_config_clone = sys_config.clone();
        let consensus_config = ConsensusConfiguration {
            height: init_block_number,
            block_interval: sys_config_clone.block_interval,
            validators: sys_config_clone.validators,
        };
        {
            let mut interval =
                time::interval(Duration::from_secs(self.config.server_retry_interval));
            loop {
                interval.tick().await;
                // if height != 0 block_hash is wrong but doesn't matter
                match self
                    .finalize_block(
                        self.genesis.genesis_block(),
                        self.block_hash.read().await.clone(),
                    )
                    .await
                {
                    Ok(()) | Err(StatusCodeEnum::ReenterBlock) => {
                        info!("executor service ready");
                        break;
                    }
                    Err(StatusCodeEnum::ExecuteServerNotReady) => {
                        warn!("executor service not ready: retrying...")
                    }
                    Err(e) => {
                        panic!("check executor service failed: {e:?}");
                    }
                }
            }
            self.auth.write().await.init(init_block_number).await;
            let status = self
                .init_status(init_block_number, sys_config)
                .await
                .unwrap();
            self.set_status(status.clone()).await;
        }
        // send configuration to consensus
        let mut server_retry_interval =
            time::interval(Duration::from_secs(self.config.server_retry_interval));
        tokio::spawn(async move {
            loop {
                server_retry_interval.tick().await;
                {
                    if reconfigure(consensus_config.clone())
                        .await
                        .is_success()
                        .is_ok()
                    {
                        info!("consensus service ready");
                        break;
                    } else {
                        warn!("consensus service not ready: retrying...")
                    }
                }
            }
        });
    }

    pub async fn rpc_get_block_number(&self, _is_pending: bool) -> Result<u64, String> {
        let block_number = self.get_status().await.height;
        Ok(block_number)
    }

    pub async fn rpc_send_raw_transaction(
        &self,
        raw_tx: RawTransaction,
        broadcast: bool,
    ) -> Result<Vec<u8>, StatusCodeEnum> {
        let tx_hash = get_tx_hash(&raw_tx)?.to_vec();

        {
            let auth = self.auth.read().await;
            auth.check_raw_tx(&raw_tx).await?;
        }

        let res = {
            let mut pool = self.pool.write().await;
            pool.insert(raw_tx.clone())
        };
        if res {
            if broadcast {
                let mut f_pool = self.forward_pool.write().await;
                f_pool.body.push(raw_tx);
                if f_pool.body.len() > self.config.count_per_batch {
                    self.broadcast_send_txs(f_pool.clone()).await;
                    f_pool.body.clear();
                }
            }
            Ok(tx_hash)
        } else {
            warn!(
                "rpc send raw transaction failed: tx already in pool. hash: 0x{}",
                hex::encode(&tx_hash)
            );
            Err(StatusCodeEnum::DupTransaction)
        }
    }

    pub async fn batch_transactions(
        &self,
        raw_txs: RawTransactions,
        broadcast: bool,
    ) -> Result<Hashes, StatusCodeEnum> {
        check_transactions(&raw_txs).is_success()?;

        let mut hashes = Vec::new();
        {
            let auth = self.auth.read().await;
            let mut pool = self.pool.write().await;
            auth.check_transactions(&raw_txs)?;
            for raw_tx in raw_txs.body.clone() {
                let hash = get_tx_hash(&raw_tx)?.to_vec();
                if pool.insert(raw_tx) {
                    hashes.push(Hash { hash })
                }
            }
        }
        if broadcast {
            self.broadcast_send_txs(raw_txs).await;
        }
        Ok(Hashes { hashes })
    }

    pub async fn rpc_get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, StatusCodeEnum> {
        load_data(
            storage_client(),
            i32::from(Regions::BlockHash) as u32,
            block_number.to_be_bytes().to_vec(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block({}) hash failed: {}",
                block_number,
                e.to_string()
            );
            StatusCodeEnum::NoBlockHeight
        })
    }

    pub async fn rpc_get_tx_block_number(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.0)
    }

    pub async fn rpc_get_tx_index(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.1)
    }

    pub async fn rpc_get_height_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<BlockNumber, StatusCodeEnum> {
        get_height_by_block_hash(hash).await
    }

    pub async fn rpc_get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        get_compact_block(block_number).await
    }

    pub async fn rpc_get_block_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        let block_number = load_data(
            storage_client(),
            i32::from(Regions::BlockHash2blockHeight) as u32,
            hash.clone(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block height failed: {}. hash: 0x{}",
                e.to_string(),
                hex::encode(&hash),
            );
            StatusCodeEnum::NoBlockHeight
        })
        .map(u64_decode)?;
        self.rpc_get_block_by_number(block_number).await
    }

    pub async fn rpc_get_state_root_by_number(
        &self,
        block_number: u64,
    ) -> Result<StateRoot, StatusCodeEnum> {
        get_state_root(block_number).await
    }

    pub async fn rpc_get_proof_by_number(
        &self,
        block_number: u64,
    ) -> Result<Proof, StatusCodeEnum> {
        get_proof(block_number).await
    }

    pub async fn rpc_get_block_detail_by_number(
        &self,
        block_number: u64,
    ) -> Result<Block, StatusCodeEnum> {
        get_full_block(block_number).await
    }

    pub async fn rpc_get_transaction(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<RawTransaction, StatusCodeEnum> {
        match db_get_tx(&tx_hash).await {
            Ok(tx) => Ok(tx),
            Err(e) => {
                let pool = self.pool.read().await;
                match pool.pool_get_tx(&tx_hash) {
                    Some(tx) => Ok(tx),
                    None => Err(e),
                }
            }
        }
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, StatusCodeEnum> {
        let auth = self.auth.read().await;
        let sys_config = auth.get_system_config();
        Ok(sys_config)
    }

    pub async fn rpc_add_node(&self, info: NodeNetInfo) -> cita_cloud_proto::common::StatusCode {
        let res = network_client().add_node(info).await.unwrap_or_else(|e| {
            warn!("rpc add node failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady.into()
        });

        let controller_for_add = self.clone();
        let code = StatusCodeEnum::from(res.clone());
        if code == StatusCodeEnum::Success || code == StatusCodeEnum::AddExistedPeer {
            let server_retry_interval = controller_for_add.config.server_retry_interval;
            let task_sender = controller_for_add.task_sender;
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(server_retry_interval)).await;
                task_sender.send(Event::BroadCastCSI).await.unwrap();
            });
        }
        res
    }

    pub async fn rpc_get_node_status(&self, _: Empty) -> Result<NodeStatus, StatusCodeEnum> {
        let peers_count = get_network_status().await?.peer_count;
        let peers_netinfo = get_peers_info().await?;
        let mut peers_status = vec![];
        for p in peers_netinfo.nodes {
            let na = NodeAddress(p.origin);
            let (address, height) = self
                .node_manager
                .nodes
                .read()
                .await
                .get(&na)
                .map_or((vec![], 0), |c| {
                    (c.address.clone().unwrap().address, c.height)
                });
            peers_status.push(PeerStatus {
                height,
                address,
                node_net_info: Some(p),
            });
        }
        let chain_status = self.get_status().await;
        let self_status = Some(PeerStatus {
            height: chain_status.height,
            address: chain_status.address.unwrap().address,
            node_net_info: None,
        });

        let node_status = NodeStatus {
            is_sync: self.get_sync_state().await,
            version: env!("CARGO_PKG_VERSION").to_string(),
            self_status,
            peers_count,
            peers_status,
            is_danger: self.config.is_danger,
            init_block_number: self.init_block_number,
        };
        Ok(node_status)
    }

    pub async fn chain_get_proposal(&self) -> Result<(u64, Vec<u8>), StatusCodeEnum> {
        if self.get_sync_state().await {
            return Err(StatusCodeEnum::NodeInSyncMode);
        }

        self.add_proposal(
            &self.get_global_status().await.1,
            self.validator_address.address.clone(),
        )
        .await?;
        self.get_proposal().await
    }

    pub async fn rpc_get_cross_chain_proof(
        &self,
        hash: Hash,
    ) -> Result<CrossChainProof, StatusCodeEnum> {
        let receipt_proof = get_receipt_proof(hash).await?;
        if receipt_proof.receipt.is_empty() || receipt_proof.roots_info.is_none() {
            warn!("not get receipt or roots_info");
            return Err(StatusCodeEnum::NoReceiptProof);
        }
        let height = receipt_proof.roots_info.clone().unwrap().height;
        let compact_block = get_compact_block(height).await?;
        let sys_con = self.rpc_get_system_config().await?;
        let pre_state_root = get_state_root(height - 1).await?.state_root;
        let proposal = ProposalInner {
            pre_state_root,
            proposal: Some(compact_block),
        };
        Ok(CrossChainProof {
            version: sys_con.version,
            chain_id: sys_con.chain_id,
            proposal: Some(proposal),
            receipt_proof: Some(receipt_proof),
            proof: get_proof(height).await?.proof,
            state_root: get_state_root(height).await?.state_root,
        })
    }

    pub async fn chain_check_proposal(
        &self,
        proposal_height: u64,
        data: &[u8],
    ) -> Result<(), StatusCodeEnum> {
        let proposal_inner = ProposalInner::decode(data).map_err(|_| {
            warn!("check proposal failed: decode ProposalInner failed");
            StatusCodeEnum::DecodeError
        })?;

        let block = &proposal_inner
            .proposal
            .ok_or(StatusCodeEnum::NoneProposal)?;
        let header = block
            .header
            .as_ref()
            .ok_or(StatusCodeEnum::NoneBlockHeader)?;
        let block_hash = get_block_hash(block.header.as_ref())?;
        let block_height = header.height;

        //check height is consistent
        if block_height != proposal_height {
            warn!(
                "check proposal({}) failed: proposal_height: {}, block_height: {}",
                proposal_height, proposal_height, block_height,
            );
            return Err(StatusCodeEnum::ProposalCheckError);
        }

        let ret = {
            //if proposal is own, skip check_proposal
            if self.is_own(data).await && self.is_candidate(&block_hash).await {
                info!(
                    "check own proposal({}): skip check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
                return Ok(());
            } else {
                info!(
                    "check remote proposal({}): start check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
            }
            //check height
            self.check_proposal(block_height).await
        };

        match ret {
            Ok(_) => {
                let sys_config = self.rpc_get_system_config().await?;
                let pre_height_bytes = (block_height - 1).to_be_bytes().to_vec();

                //check pre_state_root in proposal
                let pre_state_root = load_data(
                    storage_client(),
                    i32::from(Regions::Result) as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                if proposal_inner.pre_state_root != pre_state_root {
                    warn!(
                        "check proposal({}) failed: pre_state_root: 0x{}, local pre_state_root: 0x{}",
                        block_height,
                        hex::encode(&proposal_inner.pre_state_root),
                        hex::encode(&pre_state_root),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check proposer in block header
                let proposer = header.proposer.as_slice();
                if sys_config.validators.iter().all(|v| &v[..20] != proposer) {
                    warn!(
                        "check proposal({}) failed: proposer: {} not in validators {:?}",
                        block_height,
                        hex::encode(proposer),
                        sys_config
                            .validators
                            .iter()
                            .map(hex::encode)
                            .collect::<Vec<String>>(),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }
                //check timestamp in block header
                let pre_compact_block_bytes = load_data(
                    storage_client(),
                    i32::from(Regions::CompactBlock) as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                let pre_header = CompactBlock::decode(pre_compact_block_bytes.as_slice())
                    .map_err(|_| {
                        warn!(
                            "check proposal({}) failed: decode CompactBlock failed",
                            block_height
                        );
                        StatusCodeEnum::DecodeError
                    })?
                    .header
                    .ok_or(StatusCodeEnum::NoneBlockHeader)?;
                let timestamp = header.timestamp;
                let left_bounds = pre_header.timestamp;
                let right_bounds = unix_now() + sys_config.block_interval as u64 * 1000;
                if timestamp < left_bounds || timestamp > right_bounds {
                    warn!(
                        "check proposal({}) failed: timestamp: {} must be in range of {} - {}",
                        block_height, timestamp, left_bounds, right_bounds,
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check quota and transaction_root
                let mut total_quota = 0;
                let tx_hashes = &block
                    .body
                    .as_ref()
                    .ok_or(StatusCodeEnum::NoneBlockBody)?
                    .tx_hashes;
                let tx_count = tx_hashes.len();
                let mut transantion_data = Vec::new();
                let mut miss_tx_hash_list = Vec::new();
                for tx_hash in tx_hashes {
                    if let Some(tx) = self.pool.read().await.pool_get_tx(tx_hash) {
                        total_quota += get_tx_quota(&tx)?;
                        if total_quota > sys_config.quota_limit {
                            return Err(StatusCodeEnum::QuotaUsedExceed);
                        }
                        transantion_data.extend_from_slice(tx_hash);
                    } else {
                        miss_tx_hash_list.push(tx_hash);
                    }
                }

                if !miss_tx_hash_list.is_empty() {
                    let addr = Address {
                        address: proposer.to_vec(),
                    };
                    let origin = NodeAddress::from(&addr).0;
                    for tx_hash in miss_tx_hash_list {
                        self.unicast_sync_tx(
                            origin,
                            SyncTxRequest {
                                tx_hash: tx_hash.to_vec(),
                            },
                        )
                        .await;
                    }
                    return Err(StatusCodeEnum::NoneRawTx);
                }

                let transactions_root = hash_data(&transantion_data);
                if transactions_root != header.transactions_root {
                    warn!(
                        "check proposal({}) failed: header transactions_root: {}, controller calculate: {}",
                        block_height, hex::encode(&header.transactions_root), hex::encode(&transactions_root),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                // add remote proposal
                {
                    self.add_remote_proposal(&block_hash).await;
                }
                info!(
                    "check proposal({}) success: tx count: {}, quota: {}, block_hash: 0x{}",
                    block_height,
                    tx_count,
                    total_quota,
                    hex::encode(&block_hash)
                );
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
                {
                    self.clear_candidate().await;
                }
                self.try_sync_block().await;
            }
            _ => {}
        }

        ret
    }

    #[instrument(skip_all)]
    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<ConsensusConfiguration, StatusCodeEnum> {
        let status = self.get_status().await;

        if status.height >= height {
            let config = self.rpc_get_system_config().await?;
            return Ok(ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            });
        }

        let res = { self.commit_block(height, proposal, proof).await };

        match res {
            Ok((config, mut status)) => {
                status.address = Some(self.local_address.clone());
                self.set_status(status.clone()).await;
                self.broadcast_chain_status(status).await;
                self.try_sync_block().await;
                Ok(config)
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
                {
                    self.clear_candidate().await;
                }
                self.try_sync_block().await;
                Err(StatusCodeEnum::ProposalTooHigh)
            }
            Err(e) => Err(e),
        }
    }
}

// network
impl Chain {
    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<(), StatusCodeEnum> {
        debug!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusInitType => {
                let chain_status_init =
                    ChainStatusInit::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusInitType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let own_status = self.get_status().await;
                match chain_status_init.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                let node = chain_status_init
                                    .chain_status
                                    .clone()
                                    .ok_or(StatusCodeEnum::NoneChainStatus)?
                                    .address
                                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                                self.delete_global_status(&NodeAddress::from(&node)).await;
                                self.node_manager
                                    .set_ban_node(&NodeAddress::from(&node))
                                    .await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let status = chain_status_init
                    .chain_status
                    .ok_or(StatusCodeEnum::NoneChainStatus)?;
                let node = status
                    .address
                    .clone()
                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                let node_orign = NodeAddress::from(&node);

                match self
                    .node_manager
                    .set_node(&node_orign, status.clone())
                    .await
                {
                    Ok(None) => {
                        let chain_status_init = self.make_csi(own_status).await?;
                        self.unicast_chain_status_init(msg.origin, chain_status_init)
                            .await;
                    }
                    Ok(Some(_)) => {
                        if own_status.height > status.height {
                            self.unicast_chain_status(msg.origin, own_status).await;
                        }
                    }
                    Err(status_code) => {
                        if status_code == StatusCodeEnum::EarlyStatus
                            && own_status.height < status.height
                        {
                            {
                                self.clear_candidate().await;
                            }
                            let chain_status_init = self.make_csi(own_status).await?;
                            self.unicast_chain_status_init(msg.origin, chain_status_init)
                                .await;
                            self.try_update_global_status(&node_orign, status).await?;
                        }
                        return Err(status_code);
                    }
                }
                self.try_update_global_status(&node_orign, status).await?;
            }
            ControllerMsgType::ChainStatusInitRequestType => {
                let chain_status_init = self.make_csi(self.get_status().await).await?;

                self.unicast_chain_status_init(msg.origin, chain_status_init)
                    .await;
            }
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process ChainStatusType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                let own_status = self.get_status().await;
                let node = chain_status.address.clone().unwrap();
                let node_orign = NodeAddress::from(&node);
                match chain_status.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                self.delete_global_status(&node_orign).await;
                                self.node_manager.set_ban_node(&node_orign).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                match self
                    .node_manager
                    .check_address_origin(&node_orign, NodeAddress(msg.origin))
                    .await
                {
                    Ok(true) => {
                        self.node_manager
                            .set_node(&node_orign, chain_status.clone())
                            .await?;
                        self.try_update_global_status(&node_orign, chain_status)
                            .await?;
                    }
                    // give Ok or Err for process_network_msg is same
                    Err(StatusCodeEnum::AddressOriginCheckError) | Ok(false) => {
                        self.unicast_chain_status_init_req(msg.origin, own_status)
                            .await;
                    }
                    Err(e) => return Err(e),
                }
            }

            ControllerMsgType::ChainStatusRespondType => {
                let chain_status_respond =
                    ChainStatusRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                if let Some(respond) = chain_status_respond.respond {
                    match respond {
                        Respond::NotSameChain(node) => {
                            h160_address_check(Some(&node))?;
                            let node_orign = NodeAddress::from(&node);
                            warn!(
                                "process ChainStatusRespondType failed: remote check chain_status failed: NotSameChain. ban remote node. origin: {}", node_orign
                            );
                            self.delete_global_status(&node_orign).await;
                            self.node_manager.set_ban_node(&node_orign).await?;
                        }
                    }
                }
            }

            ControllerMsgType::SyncBlockType => {
                let sync_block_request =
                    SyncBlockRequest::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockType failed: decode failed",);
                        StatusCodeEnum::DecodeError
                    })?;

                info!(
                    "get SyncBlockRequest: from origin: {:x}, height: {} - {}",
                    msg.origin, sync_block_request.start_height, sync_block_request.end_height
                );
                self.task_sender
                    .send(Event::SyncBlockReq(sync_block_request, msg.origin))
                    .await
                    .unwrap();
            }

            ControllerMsgType::SyncBlockRespondType => {
                let sync_block_respond =
                    SyncBlockRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_block_respond::Respond;

                tokio::spawn(async move {
                    match sync_block_respond.respond {
                        // todo check origin
                        Some(Respond::MissBlock(node)) => {
                            let node_orign = NodeAddress::from(&node);
                            controller_clone.delete_global_status(&node_orign).await;
                            controller_clone
                                .node_manager
                                .set_misbehavior_node(&node_orign)
                                .await
                                .unwrap();
                        }
                        Some(Respond::Ok(sync_blocks)) => {
                            // todo handle error
                            match controller_clone
                                .handle_sync_blocks(sync_blocks.clone())
                                .await
                            {
                                Ok(_) => {
                                    if controller_clone
                                        .sync_manager
                                        .contains_block(
                                            controller_clone.get_status().await.height + 1,
                                        )
                                        .await
                                    {
                                        controller_clone
                                            .task_sender
                                            .send(Event::SyncBlock)
                                            .await
                                            .unwrap();
                                    }
                                }
                                Err(StatusCodeEnum::ProvideAddressError)
                                | Err(StatusCodeEnum::NoProvideAddress) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: message address error. origin: {:x}",
                                        msg.origin
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: {}. origin: {:x}",
                                        e.to_string(),
                                        msg.origin
                                    );
                                    let node = sync_blocks.address.as_ref().unwrap();
                                    let node_orign = NodeAddress::from(node);
                                    controller_clone
                                        .node_manager
                                        .set_misbehavior_node(&node_orign)
                                        .await
                                        .unwrap();
                                    controller_clone.delete_global_status(&node_orign).await;
                                }
                            }
                        }
                        None => {}
                    }
                });
            }

            ControllerMsgType::SyncTxType => {
                let sync_tx = SyncTxRequest::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SyncTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                tokio::spawn(async move {
                    if let Ok(raw_tx) = { controller_clone.chain_get_tx(&sync_tx.tx_hash).await } {
                        controller_clone
                            .unicast_sync_tx_respond(
                                msg.origin,
                                SyncTxRespond {
                                    respond: Some(Respond::Ok(raw_tx)),
                                },
                            )
                            .await;
                    }
                });
            }

            ControllerMsgType::SyncTxRespondType => {
                let sync_tx_respond = SyncTxRespond::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SyncTxRespondType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                match sync_tx_respond.respond {
                    Some(Respond::MissTx(node)) => {
                        let node_orign = NodeAddress::from(&node);
                        self.node_manager.set_misbehavior_node(&node_orign).await?;
                        self.delete_global_status(&node_orign).await;
                    }
                    Some(Respond::Ok(raw_tx)) => {
                        self.rpc_send_raw_transaction(raw_tx, false).await?;
                    }
                    None => {}
                }
            }

            ControllerMsgType::SendTxType => {
                let send_tx = RawTransaction::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.rpc_send_raw_transaction(send_tx, false).await?;
            }

            ControllerMsgType::SendTxsType => {
                let body = RawTransactions::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxsType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.batch_transactions(body, false).await?;
            }

            ControllerMsgType::Noop => {
                warn!("process Noop failed: unexpected");
                self.delete_global_status(&NodeAddress(msg.origin)).await;
                self.node_manager
                    .set_ban_node(&NodeAddress(msg.origin))
                    .await?;
            }
        }

        Ok(())
    }

    impl_broadcast!(
        broadcast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_multicast!(broadcast_chain_status, ChainStatus, "chain_status");

    // impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    // impl_multicast!(multicast_send_tx, RawTransaction, "send_tx");
    // impl_multicast!(multicast_send_txs, RawTransactions, "send_txs");
    impl_broadcast!(broadcast_send_txs, RawTransactions, "send_txs");
    // impl_multicast!(multicast_sync_tx, SyncTxRequest, "sync_tx");
    // impl_multicast!(multicast_sync_block, SyncBlockRequest, "sync_block");

    impl_unicast!(unicast_chain_status, ChainStatus, "chain_status");
    impl_unicast!(
        unicast_chain_status_init_req,
        ChainStatus,
        "chain_status_init_req"
    );
    impl_unicast!(
        unicast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_unicast!(unicast_sync_block, SyncBlockRequest, "sync_block");
    impl_unicast!(
        unicast_sync_block_respond,
        SyncBlockRespond,
        "sync_block_respond"
    );
    impl_unicast!(unicast_sync_tx, SyncTxRequest, "sync_tx");
    impl_unicast!(unicast_sync_tx_respond, SyncTxRespond, "sync_tx_respond");
    impl_unicast!(
        unicast_chain_status_respond,
        ChainStatusRespond,
        "chain_status_respond"
    );
}

// status
impl Chain {
    pub async fn get_global_status(&self) -> (NodeAddress, ChainStatus) {
        let rd = self.global_status.read().await;
        rd.clone()
    }

    pub async fn update_global_status(&self, node: NodeAddress, status: ChainStatus) {
        let mut wr = self.global_status.write().await;
        *wr = (node, status);
    }

    async fn delete_global_status(&self, node: &NodeAddress) -> bool {
        let res = {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            &gs.0 == node
        };
        if res {
            let mut wr = self.global_status.write().await;
            *wr = (NodeAddress(0), ChainStatus::default());
            true
        } else {
            false
        }
    }

    async fn try_update_global_status(
        &self,
        node: &NodeAddress,
        status: ChainStatus,
    ) -> Result<bool, StatusCodeEnum> {
        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        if status.height > old_status.1.height && status.height >= own_status.height {
            info!(
                "update global status: origin: {}, height: {}, hash: 0x{}",
                node,
                status.height,
                hex::encode(status.hash.clone().unwrap().hash)
            );
            let global_height = status.height;
            self.update_global_status(node.to_owned(), status).await;
            if global_height > own_status.height {
                self.try_sync_block().await;
            }
            if (!self.get_sync_state().await || global_height % self.config.force_sync_epoch == 0)
                && self
                    .sync_manager
                    .contains_block(own_status.height + 1)
                    .await
            {
                self.task_sender.send(Event::SyncBlock).await.unwrap();
            }

            return Ok(true);
        }

        // request block if own height behind remote's
        if status.height > own_status.height {
            self.try_sync_block().await;
        }

        Ok(false)
    }

    async fn init_status(
        &self,
        height: u64,
        config: SystemConfig,
    ) -> Result<ChainStatus, StatusCodeEnum> {
        let compact_block = get_compact_block(height).await?;

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash {
                hash: get_block_hash(compact_block.header.as_ref())?,
            }),
            address: Some(self.local_address.clone()),
        })
    }

    pub async fn get_status(&self) -> ChainStatus {
        let rd = self.current_status.read().await;
        rd.clone()
    }

    pub async fn set_status(&self, mut status: ChainStatus) {
        if h160_address_check(status.address.as_ref()).is_err() {
            status.address = Some(self.local_address.clone())
        }

        let mut wr = self.current_status.write().await;
        *wr = status;
    }

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, StatusCodeEnum> {
        h160_address_check(sync_blocks.address.as_ref())?;

        let own_height = self.get_status().await.height;
        Ok(self
            .sync_manager
            .insert_blocks(
                sync_blocks
                    .address
                    .ok_or(StatusCodeEnum::NoProvideAddress)?,
                sync_blocks.sync_blocks,
                own_height,
            )
            .await)
    }

    pub async fn try_sync_block(&self) {
        let (_, global_status) = self.get_global_status().await;
        // sync mode will return exclude global_height % self.config.force_sync_epoch == 0
        if self.get_sync_state().await && global_status.height % self.config.force_sync_epoch != 0 {
            return;
        }

        self.task_sender.send(Event::TrySyncBlock).await.unwrap();
    }

    pub async fn make_csi(
        &self,
        own_status: ChainStatus,
    ) -> Result<ChainStatusInit, StatusCodeEnum> {
        let mut chain_status_bytes = Vec::new();
        own_status.encode(&mut chain_status_bytes).map_err(|_| {
            warn!("make csi failed: encode ChainStatus failed");
            StatusCodeEnum::EncodeError
        })?;
        let msg_hash = hash_data(&chain_status_bytes);

        #[cfg(feature = "sm")]
        let crypto = crypto_sm::crypto::Crypto::new(&self.private_key_path);
        #[cfg(feature = "eth")]
        let crypto = crypto_eth::crypto::Crypto::new(&self.private_key_path);

        let signature = crypto.sign_message(&msg_hash)?;

        Ok(ChainStatusInit {
            chain_status: Some(own_status),
            signature,
        })
    }

    pub async fn get_sync_state(&self) -> bool {
        *self.is_sync.read().await
    }

    pub async fn set_sync_state(&self, state: bool) {
        if self.get_sync_state().await != state {
            *self.is_sync.write().await = state;
        }
        if !state {
            self.sync_manager.clear().await;
        }
    }

    pub async fn get_proposal(&self) -> Result<(u64, Vec<u8>), StatusCodeEnum> {
        if let Some(proposal) = self.own_proposal.read().await.clone() {
            Ok(proposal)
        } else {
            Err(StatusCodeEnum::NoCandidate)
        }
    }

    pub async fn add_remote_proposal(&self, block_hash: &[u8]) {
        self.candidates.write().await.insert(block_hash.to_vec());
    }

    pub async fn is_candidate(&self, block_hash: &[u8]) -> bool {
        self.candidates.read().await.contains(block_hash)
    }

    pub async fn is_own(&self, proposal_to_check: &[u8]) -> bool {
        if let Some((_, proposal_data)) = &self.own_proposal.read().await.clone() {
            proposal_data == proposal_to_check
        } else {
            false
        }
    }

    pub async fn add_proposal(
        &self,
        global_status: &ChainStatus,
        proposer: Vec<u8>,
    ) -> Result<(), StatusCodeEnum> {
        if self.need_sync(global_status).await {
            Err(StatusCodeEnum::NodeInSyncMode)
        } else if self.own_proposal.read().await.is_some() {
            // already have own proposal
            Ok(())
        } else {
            let prevhash = self.block_hash.read().await.clone();
            let height = *self.block_number.read().await + 1;

            let (tx_hash_list, quota) = {
                let mut pool = self.pool.write().await;
                let ret = pool.package(height);
                let (pool_len, pool_quota) = pool.pool_status();
                info!(
                    "package proposal({}): pool len: {}, pool quota: {}",
                    height, pool_len, pool_quota
                );
                ret
            };
            let tx_count = tx_hash_list.len();

            let mut transantion_data = Vec::new();
            for tx_hash in tx_hash_list.iter() {
                transantion_data.extend_from_slice(tx_hash);
            }
            let transactions_root = hash_data(&transantion_data);

            let header = BlockHeader {
                prevhash: prevhash.clone(),
                timestamp: unix_now(),
                height,
                transactions_root,
                proposer,
            };

            let compact_block = CompactBlock {
                version: 0,
                header: Some(header.clone()),
                body: Some(CompactBlockBody {
                    tx_hashes: tx_hash_list,
                }),
            };

            let mut block_header_bytes = Vec::new();
            header
                .encode(&mut block_header_bytes)
                .expect("add proposal failed: encode BlockHeader failed");

            let block_hash = hash_data(&block_header_bytes);

            let mut own_proposal = self.own_proposal.write().await;
            *own_proposal = Some((height, assemble_proposal(&compact_block, height).await?));
            self.candidates.write().await.insert(block_hash.clone());

            info!(
                "add proposal({}): tx count: {}, quota: {}, hash: 0x{}",
                height,
                tx_count,
                quota,
                hex::encode(&block_hash),
            );

            Ok(())
        }
    }

    pub async fn check_proposal(&self, height: u64) -> Result<(), StatusCodeEnum> {
        let block_number = *self.block_number.read().await + 1;
        if height != block_number {
            warn!(
                "check proposal({}) failed: get height: {}, need height: {}",
                height, height, block_number
            );
            if height < block_number {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > block_number {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn finalize_block(
        &self,
        mut block: Block,
        block_hash: Vec<u8>,
    ) -> Result<(), StatusCodeEnum> {
        let block_height = block
            .header
            .as_ref()
            .ok_or(StatusCodeEnum::NoneBlockHeader)?
            .height;

        // execute block, executed_blocks_hash == state_root
        {
            let (executed_blocks_status, executed_blocks_hash) = exec_block(block.clone()).await;

            info!(
                "execute block({}) {}: state_root: 0x{}. hash: 0x{}",
                block_height,
                executed_blocks_status,
                hex::encode(&executed_blocks_hash),
                hex::encode(&block_hash),
            );

            match executed_blocks_status {
                StatusCodeEnum::Success => {}
                StatusCodeEnum::ReenterBlock => {
                    warn!(
                        "ReenterBlock({}): status: {}, state_root: 0x{}",
                        block_height,
                        executed_blocks_status,
                        hex::encode(&executed_blocks_hash)
                    );
                    // The genesis block does not allow reenter
                    if block_height == 0 {
                        return Err(StatusCodeEnum::ReenterBlock);
                    }
                }
                status_code => {
                    return Err(status_code);
                }
            }

            // if state_root is not empty should verify it
            if block.state_root.is_empty() {
                block.state_root = executed_blocks_hash;
            } else if block.state_root != executed_blocks_hash {
                warn!("finalize block failed: check state_root error");
                return Err(StatusCodeEnum::StateRootCheckError);
            }
        }

        // persistence to storage
        {
            let block_bytes = {
                let mut buf = Vec::with_capacity(block.encoded_len());
                block.encode(&mut buf).map_err(|_| {
                    warn!("finalize block failed: encode Block failed");
                    StatusCodeEnum::EncodeError
                })?;
                let mut block_bytes = Vec::new();
                block_bytes.extend_from_slice(&block_hash);
                block_bytes.extend_from_slice(&buf);
                block_bytes
            };

            store_data(
                i32::from(Regions::AllBlockData) as u32,
                block_height.to_be_bytes().to_vec(),
                block_bytes,
            )
            .await
            .is_success()?;
            info!(
                "store AllBlockData({}) success: hash: 0x{}",
                block_height,
                hex::encode(&block_hash)
            );
        }

        // update auth pool and systemconfig
        // even empty block, we also need update current height of auth
        {
            if let Some(raw_txs) = block.body.clone() {
                for raw_tx in raw_txs.body {
                    if let Some(Tx::UtxoTx(utxo_tx)) = raw_tx.tx.clone() {
                        let res = {
                            let mut auth = self.auth.write().await;
                            auth.update_system_config(&utxo_tx)
                        };
                        if res {
                            // if sys_config changed, store utxo tx hash into global region
                            let lock_id = utxo_tx.transaction.as_ref().unwrap().lock_id;
                            store_data(
                                i32::from(Regions::Global) as u32,
                                lock_id.to_be_bytes().to_vec(),
                                utxo_tx.transaction_hash,
                            )
                            .await
                            .is_success()?;
                            match lock_id {
                                LOCK_ID_BLOCK_LIMIT | LOCK_ID_QUOTA_LIMIT => {
                                    let sys_config = self.get_system_config().await;
                                    let mut pool = self.pool.write().await;
                                    pool.set_block_limit(sys_config.block_limit);
                                    pool.set_quota_limit(sys_config.quota_limit);
                                }
                                _ => {}
                            }
                        }
                    };
                }
            }

            let tx_hash_list =
                get_tx_hash_list(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)?;
            self.auth
                .write()
                .await
                .insert_tx_hash(block_height, tx_hash_list.clone());
            self.pool.write().await.remove(&tx_hash_list);
            info!(
                "update auth and pool, tx_hash_list len {}",
                tx_hash_list.len()
            );
        }

        // success and print pool status
        {
            let (pool_len, pool_quota) = self.pool.read().await.pool_status();
            info!(
                "finalize block({}) success: pool len: {}, pool quota: {}. hash: 0x{}",
                block_height,
                pool_len,
                pool_quota,
                hex::encode(&block_hash)
            );
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCodeEnum> {
        let block_number = *self.block_number.read().await + 1;
        if height != block_number {
            warn!(
                "commit block({}) failed: get height: {}, need height: {}",
                height, height, block_number
            );
            if height < block_number {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > block_number {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        let proposal_inner = ProposalInner::decode(proposal).map_err(|_| {
            warn!(
                "commit block({}) failed: decode ProposalInner failed",
                height
            );
            StatusCodeEnum::DecodeError
        })?;

        if let Some(compact_block) = proposal_inner.proposal {
            let block_hash = get_block_hash(compact_block.header.as_ref())?;

            let prev_hash = compact_block.header.clone().unwrap().prevhash;

            let self_block_hash = self.block_hash.read().await.clone();

            if prev_hash != self_block_hash {
                warn!(
                    "commit block({}) failed: get prehash: 0x{}, correct prehash: 0x{}. hash: 0x{}",
                    height,
                    hex::encode(&prev_hash),
                    hex::encode(&self_block_hash),
                    hex::encode(&block_hash),
                );
                return Err(StatusCodeEnum::ProposalCheckError);
            }

            let mut tx_list = vec![];
            let tx_hashes = &compact_block
                .body
                .as_ref()
                .ok_or(StatusCodeEnum::NoneBlockBody)?
                .tx_hashes;

            for tx_hash in tx_hashes {
                if let Some(tx) = self.pool.read().await.pool_get_tx(tx_hash) {
                    tx_list.push(tx);
                } else {
                    return Err(StatusCodeEnum::NoneRawTx);
                }
            }

            let full_block = Block {
                version: 0,
                header: compact_block.header.clone(),
                body: Some(RawTransactions { body: tx_list }),
                proof: proof.to_vec(),
                state_root: vec![],
            };

            info!(
                "commit block({}): hash: 0x{}",
                height,
                hex::encode(&block_hash)
            );
            self.finalize_block(full_block, block_hash.clone()).await?;

            *self.block_number.write().await = height;
            *self.block_hash.write().await = block_hash.clone();

            // candidate_block need update
            self.clear_candidate().await;

            let config = self.get_system_config().await;

            return Ok((
                ConsensusConfiguration {
                    height,
                    block_interval: config.block_interval,
                    validators: config.validators,
                },
                ChainStatus {
                    version: config.version,
                    chain_id: config.chain_id,
                    height,
                    hash: Some(Hash { hash: block_hash }),
                    address: None,
                },
            ));
        }

        Err(StatusCodeEnum::NoForkTree)
    }

    pub async fn process_block(
        &self,
        block: Block,
    ) -> Result<(ConsensusConfiguration, ChainStatus), StatusCodeEnum> {
        let block_hash = get_block_hash(block.header.as_ref())?;
        let header = block.header.clone().unwrap();
        let height = header.height;

        let block_number = *self.block_number.read().await + 1;
        let self_block_hash = self.block_hash.read().await.clone();

        if height != block_number {
            warn!(
                "process block({}) failed: get height: {}, need height: {}",
                height, height, block_number
            );
            if height < block_number {
                return Err(StatusCodeEnum::ProposalTooLow);
            }
            if height > block_number {
                return Err(StatusCodeEnum::ProposalTooHigh);
            }
        }

        if header.prevhash != self_block_hash {
            warn!(
                "process block({}) failed: get prehash: 0x{}, correct prehash: 0x{}. hash: 0x{}",
                height,
                hex::encode(&header.prevhash),
                hex::encode(&self_block_hash),
                hex::encode(&block_hash)
            );
            return Err(StatusCodeEnum::BlockCheckError);
        }

        let compact_blk = extract_compact(block.clone());
        let proposal_bytes_for_check = assemble_proposal(&compact_blk, height).await?;

        let status = check_block(height, proposal_bytes_for_check, block.proof.clone()).await;
        if status != StatusCodeEnum::Success {
            return Err(status);
        }

        {
            let auth = self.auth.read().await;
            auth.check_transactions(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)?
        }

        check_transactions(block.body.as_ref().ok_or(StatusCodeEnum::NoneBlockBody)?)
            .is_success()?;

        self.finalize_block(block, block_hash.clone()).await?;

        *self.block_number.write().await = height;
        *self.block_hash.write().await = block_hash.clone();

        let config = self.get_system_config().await;

        Ok((
            ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            },
            ChainStatus {
                version: config.version,
                chain_id: config.chain_id,
                height,
                hash: Some(Hash { hash: block_hash }),
                address: None,
            },
        ))
    }

    pub async fn get_system_config(&self) -> SystemConfig {
        let rd = self.auth.read().await;
        rd.get_system_config()
    }

    pub async fn need_sync(&self, global_status: &ChainStatus) -> bool {
        if global_status.height > *self.block_number.read().await
            && self.candidates.read().await.is_empty()
        {
            debug!("sync mode");
            true
        } else {
            debug!("online mode");
            false
        }
    }

    pub async fn chain_get_tx(&self, tx_hash: &[u8]) -> Result<RawTransaction, StatusCodeEnum> {
        if let Some(raw_tx) = {
            let rd = self.pool.read().await;
            rd.pool_get_tx(tx_hash)
        } {
            Ok(raw_tx)
        } else {
            db_get_tx(tx_hash).await
        }
    }

    pub async fn clear_candidate(&self) {
        self.candidates.write().await.clear();
        *self.own_proposal.write().await = None;
    }
}
