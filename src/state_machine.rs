use statig::prelude::*;
use std::{fmt::Debug, ops::Deref};

use crate::{chain::Chain, protocol::sync_manager::SyncBlockRequest};

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
    TimerElapsed,
    // init event
    NetworkServiceReady,
    ExecutorServiceReady,
    StorageServiceReady,
    SystemConfigInitialized,
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
    initial = "State::initializing(false, false, false, false)",
    state(derive(Debug, Clone)),
    superstate(derive(Debug, Clone)),
    on_transition = "Self::on_transition",
    on_dispatch = "Self::on_dispatch"
)]
impl ControllerStateMachine {
    #[state(superstate = "waiting_for_initialization")]
    fn initializing(
        network: &mut bool,
        executor: &mut bool,
        storage: &mut bool,
        sys_config: &mut bool,
        event: &Event,
    ) -> Response<State> {
        match event {
            Event::NetworkServiceReady => {
                *network = true;
                Super
            }
            Event::ExecutorServiceReady => {
                *executor = true;
                Super
            }
            Event::StorageServiceReady => {
                *storage = true;
                Super
            }
            Event::SystemConfigInitialized => {
                *sys_config = true;
                Super
            }
            _ => Super,
        }
    }

    #[superstate]
    fn waiting_for_initialization(
        network: &bool,
        executor: &bool,
        storage: &bool,
        sys_config: &bool,
    ) -> Response<State> {
        match (network, executor, storage, sys_config) {
            (true, true, true, true) => Transition(State::increasing()),
            _ => Handled,
        }
    }

    #[state]
    async fn increasing(event: &Event) -> Response<State> {
        match event {
            Event::TimerElapsed => Transition(State::increasing()),
            _ => Super,
        }
    }

    #[state]
    async fn sync(event: &Event) -> Response<State> {
        match event {
            Event::TimerElapsed => Transition(State::sync()),
            _ => Super,
        }
    }
}

impl ControllerStateMachine {
    pub fn new(chain: Chain) -> Self {
        ControllerStateMachine(chain)
    }

    fn on_transition(&mut self, source: &State, target: &State) {
        println!("transitioned from `{source:?}` to `{target:?}`");
    }

    fn on_dispatch(&mut self, state: StateOrSuperstate<Self>, event: &Event) {
        println!("dispatching `{event:?}` to `{state:?}`");
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
