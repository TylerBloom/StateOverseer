use std::{marker::PhantomData, slice::Iter};

use crate::{
    FullOp, OpId, OpLog, OpSlice, OpSync, OverseenState, OverseerOperation, Rollback, SyncError,
    SyncStatus,
};

pub struct StateOverseer<T>
where
    T: OverseenState,
{
    state: T,
    ops: OpLog<<T as OverseenState>::Operation>,
    /// The last OpId of the last operation after a successful sync
    last_sync: Option<OpId>,
}

impl<T> StateOverseer<T>
where
    T: OverseenState + Default,
    <T as OverseenState>::Operation: OverseerOperation + Clone + PartialEq,
{
    /// Creates a tournament manager, tournament, and operations log
    pub fn new() -> Self {
        Self {
            state: T::default(),
            ops: OpLog::new(),
            last_sync: None,
        }
    }

    /// Read only accesses to tournaments don't need to be wrapped, so we can freely provide
    /// references to them
    pub fn get_state(&self) -> &T {
        &self.state
    }

    /// Returns the latest active operation id
    pub fn get_last_active_id(&self) -> Option<OpId> {
        self.ops
            .ops
            .iter()
            .rev()
            .find_map(|op| op.active.then(|| op.id))
    }

    /// Takes the manager, removes all unnecessary data for storage, and return the underlying
    /// tournament, consuming the manager in the process.
    pub fn extract(self) -> T {
        self.state
    }

    /// Gets a slice of the op log
    pub fn get_op_slice(&self, id: OpId) -> Option<OpSlice<<T as OverseenState>::Operation>> {
        self.ops.get_slice(id)
    }

    /// Gets a slice of the log starting at the operation of the last log.
    /// Primarily used by clients to track when they last synced with the server
    pub fn slice_from_last_sync(&self) -> OpSlice<<T as OverseenState>::Operation> {
        match self.last_sync {
            None => OpSlice {
                ops: self.ops.ops.clone(),
            },
            Some(id) => self.get_op_slice(id).unwrap(),
        }
    }

    /// Attempts to sync the given `OpSync` with the operations log. If syncing can occur, the op
    /// logs are merged and SyncStatus::Completed is returned.
    pub fn attempt_sync(
        &mut self,
        sy: OpSync<<T as OverseenState>::Operation>,
    ) -> SyncStatus<<T as OverseenState>::Operation> {
        let digest = self.ops.sync(sy);
        if digest.is_completed() {
            self.last_sync = self.ops.ops.last().map(|op| op.id)
        }
        digest
    }

    /// TODO:
    pub fn overwrite(
        &mut self,
        ops: OpSlice<<T as OverseenState>::Operation>,
    ) -> Result<(), SyncError<<T as OverseenState>::Operation>> {
        let digest = self.ops.overwrite(ops);
        if digest.is_ok() {
            self.last_sync = self.ops.ops.last().map(|op| op.id);
        }
        digest
    }

    /// Creates a rollback proposal but leaves the operations log unaffected.
    /// `id` should be the id of the last operation that is **removed**
    pub fn propose_rollback(&self, id: OpId) -> Option<Rollback<<T as OverseenState>::Operation>> {
        self.ops.create_rollback(id)
    }

    /// Takes an operation, ensures all idents are their Id variants, stores the operation, applies
    /// it to the tournament, and returns the result.
    /// NOTE: That an operation is always stored, regardless of the outcome
    pub fn apply_op(
        &mut self,
        op: <T as OverseenState>::Operation,
    ) -> Result<<T as OverseenState>::OpSuccess, <T as OverseenState>::OpFailure> {
        let digest = self.state.apply_op(&op);
        if digest.is_ok() {
            let f_op = FullOp::new(op);
            self.ops.ops.push(f_op);
        }
        digest
    }

    /// Returns an iterator over all the states of a tournament
    pub fn states(&self) -> StateIter<'_, T> {
        StateIter {
            state: T::default(),
            ops: self.ops.ops.iter(),
            shown_init: false,
        }
    }
}

/// An iterator over all the states of a tournament
pub struct StateIter<'a, T>
where
    T: OverseenState,
{
    state: T,
    ops: Iter<'a, FullOp<<T as OverseenState>::Operation>>,
    shown_init: bool,
}

impl<'a, T> Iterator for StateIter<'a, T>
where
    T: OverseenState + Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.shown_init {
            let op = self.ops.next()?;
            let _ = self.state.apply_op(&op.op); // Should always succeed
        } else {
            self.shown_init = true;
        }
        Some(self.state.clone())
    }
}
