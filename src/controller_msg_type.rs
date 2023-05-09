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

#[derive(Debug)]
pub enum ControllerMsgType {
    ChainStatusInitType,
    ChainStatusInitRequestType,
    ChainStatusType,
    ChainStatusRespondType,
    SyncBlockType,
    SyncBlockRespondType,
    SyncTxType,
    SyncTxRespondType,
    SendTxType,
    SendTxsType,
    Noop,
}

impl From<&str> for ControllerMsgType {
    fn from(s: &str) -> Self {
        match s {
            "chain_status_init" => Self::ChainStatusInitType,
            "chain_status_init_req" => Self::ChainStatusInitRequestType,
            "chain_status" => Self::ChainStatusType,
            "chain_status_respond" => Self::ChainStatusRespondType,
            "sync_block" => Self::SyncBlockType,
            "sync_block_respond" => Self::SyncBlockRespondType,
            "sync_tx" => Self::SyncTxType,
            "sync_tx_respond" => Self::SyncTxRespondType,
            "send_tx" => Self::SendTxType,
            "send_txs" => Self::SendTxsType,
            _ => Self::Noop,
        }
    }
}

impl ::std::fmt::Display for ControllerMsgType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<ControllerMsgType> for &str {
    fn from(t: ControllerMsgType) -> Self {
        match t {
            ControllerMsgType::ChainStatusInitType => "chain_status_init",
            ControllerMsgType::ChainStatusInitRequestType => "chain_status_init_req",
            ControllerMsgType::ChainStatusType => "chain_status",
            ControllerMsgType::ChainStatusRespondType => "chain_status_respond",
            ControllerMsgType::SyncBlockType => "sync_block",
            ControllerMsgType::SyncBlockRespondType => "sync_block_respond",
            ControllerMsgType::SyncTxType => "sync_tx",
            ControllerMsgType::SyncTxRespondType => "sync_tx_respond",
            ControllerMsgType::SendTxType => "send_tx",
            ControllerMsgType::SendTxsType => "send_txs",
            ControllerMsgType::Noop => "noop",
        }
    }
}
