#![allow(unused)]

pub mod manager;
pub mod sync;

use sync::{Blockage, OpSync, Rollback, RollbackError, SyncError, SyncStatus};
use uuid::Uuid;

pub trait OverseerOperation {
    fn blocks(&self, other: &Self) -> bool;

    fn implied_ordering(&self, other: &Self) -> bool {
        false
    }
}

pub trait OverseenState {
    type Operation;
    type OpSuccess;
    type OpFailure;

    fn apply_op(&mut self, op: &Self::Operation) -> Result<Self::OpSuccess, Self::OpFailure>;
}

pub type OpId = Uuid;

pub struct FullOp<T> {
    op: T,
    id: OpId,
    active: bool,
}

impl<T> FullOp<T> {
    pub fn new(op: T) -> Self {
        Self {
            op,
            id: Uuid::new_v4(),
            active: true,
        }
    }
}

impl<T> OverseerOperation for FullOp<T>
where
    T: OverseerOperation,
{
    fn blocks(&self, other: &Self) -> bool {
        self.op.blocks(&other.op)
    }

    fn implied_ordering(&self, other: &Self) -> bool {
        self.op.implied_ordering(&other.op)
    }
}

/// An ordered list of all operations applied to a tournament
pub struct OpLog<T> {
    pub(crate) ops: Vec<FullOp<T>>,
}

/// An ordered list of some of the operations applied to a tournament
pub struct OpSlice<T> {
    pub(crate) ops: Vec<FullOp<T>>,
}

impl<T> OpLog<T>
where
    T: OverseerOperation + Clone + PartialEq,
{
    /// Creates a new log
    pub fn new() -> Self {
        OpLog { ops: Vec::new() }
    }

    /// Adds an operation to the end of the OpLog
    pub fn add_op(&mut self, op: FullOp<T>) {
        self.ops.push(op);
    }

    /// Creates a slice of this log starting at the given index. `None` is returned if `index` is
    /// out of bounds.
    pub(crate) fn get_slice(&self, id: OpId) -> Option<OpSlice<T>> {
        self.get_slice_extra(id, 0)
    }

    /// Creates a slice of this log starting at the given index. `None` is returned if `index` is
    /// out of bounds.
    pub(crate) fn get_slice_extra(&self, id: OpId, mut extra: usize) -> Option<OpSlice<T>> {
        let mut end = false;
        let mut ops = Vec::new();
        for i_op in self.ops.iter().rev().cloned() {
            if end && extra == 0 {
                break;
            }
            if end {
                extra -= 1;
            }
            end |= i_op.id == id;
            ops.push(i_op);
        }
        if !end && extra != 0 {
            return None;
        }
        Some(OpSlice {
            ops: ops.into_iter().rev().collect(),
        })
    }

    pub(crate) fn slice_from_slice(&self, ops: &OpSlice<T>) -> Result<OpSlice<T>, SyncError<T>> {
        let op = match ops.start_op() {
            Some(op) => op,
            None => {
                return Err(SyncError::EmptySync);
            }
        };
        match self.get_slice(op.id) {
            Some(slice) => {
                if slice.start_op().unwrap() != op {
                    return Err(SyncError::RollbackFound(slice));
                }
                Ok(slice)
            }
            None => Err(SyncError::UnknownOperation(op)),
        }
    }

    /// Removes all elements in the log starting at the first index of the given slice. All
    /// operations in the slice are then appended to the end of the log.
    pub fn overwrite(&mut self, ops: OpSlice<T>) -> Result<(), SyncError<T>> {
        let slice = self.slice_from_slice(&ops)?;
        let id = slice.start_id().unwrap();
        let index = self.ops.iter().position(|o| o.id == id).unwrap();
        self.ops.truncate(index);
        self.ops.extend(ops.ops.into_iter());
        Ok(())
    }

    /// Creates a slice of the current log by starting at the end and moving back. All operations
    /// that cause the closure to return `true` will be dropped and `false` will be kept. An
    /// operation causes `None` to be returned will end the iteration, will not be in the slice,
    /// but kept in the log.
    pub fn create_rollback(&self, id: OpId) -> Option<Rollback<T>> {
        let mut ops = self.get_slice_extra(id, 1)?;
        ops.ops.iter_mut().skip(1).for_each(|op| op.active = false);
        Some(Rollback { ops })
    }

    /// Applies a rollback to this log.
    /// Err is returned if there is a different in between the length of the given slice and the
    /// corresponding slice of this log, and this log is not changed.
    /// Otherwise, the rollback is simply applied.
    ///
    /// NOTE: An OpSync is returned as the error data because the sender needs to have an
    /// up-to-date history before sendings a rollback.
    pub fn apply_rollback(&mut self, rollback: Rollback<T>) -> Result<(), RollbackError<T>> {
        let slice = self
            .slice_from_slice(&rollback.ops)
            .map_err(RollbackError::SliceError)?;
        if slice.ops.len() > rollback.ops.ops.len() {
            return Err(RollbackError::OutOfSync(OpSync { ops: slice }));
        }
        let mut r_op = rollback.ops.ops.iter();
        for i_op in slice.ops.iter() {
            let mut broke = false;
            while let Some(r) = r_op.next() {
                // If the id is unknown, the operation is unknow... so we continue.
                // Unknown, inactive ops ok to keep around. They can't affect anything
                if i_op.id == r.id {
                    broke = true;
                    break;
                }
            }
            if !broke {
                return Err(RollbackError::OutOfSync(OpSync { ops: slice }));
            }
        }
        // This should never return an Err
        self.overwrite(rollback.ops)
            .map_err(RollbackError::SliceError)
    }

    /// Attempts to sync the local log with a remote log.
    /// Returns Err if the starting op id of the given log can't be found in this log.
    /// Otherwise, Ok is returned and contains a SyncStatus
    pub fn sync(&mut self, other: OpSync<T>) -> SyncStatus<T> {
        let slice = match self.slice_from_slice(&other.ops) {
            Ok(s) => s,
            Err(e) => {
                return SyncStatus::SyncError(e);
            }
        };
        slice.merge(other.ops)
    }
}

impl<T> OpSlice<T>
where
    T: OverseerOperation + Clone + PartialEq,
{
    /// Creates a new slice
    pub fn new() -> Self {
        OpSlice { ops: Vec::new() }
    }

    /// Adds an operation to the end of the OpSlice
    pub fn add_op(&mut self, op: FullOp<T>) {
        self.ops.push(op);
    }

    /// Returns the index of the first stored operation.
    pub fn start_op(&self) -> Option<FullOp<T>> {
        self.ops.first().cloned()
    }

    /// Returns the index of the first stored operation.
    pub fn start_id(&self) -> Option<OpId> {
        self.ops.first().map(|o| o.id)
    }

    /// Takes the slice and strips all inactive operations. This is only needed in the unlikely
    /// scenerio where a client rollbacks without communicating with the server and then tries to
    /// sync with the server.
    pub fn squash(self) -> Self {
        Self {
            ops: self.ops.into_iter().filter(|o| o.active).collect(),
        }
    }

    /// Takes another op slice and attempts to merge it with this slice.
    ///
    /// If there are no blockages, the `Ok` varient is returned containing the rectified log and
    /// this log is updated.
    ///
    /// If there is a blockage, the `Err` varient is returned two partial logs, a copy of this log and the
    /// given log. The first operation of  but whose first operations are blocking.
    ///
    /// Promised invarient: If two slices can be merged without blockages, they will be meaningfully the
    /// identical; however, identical sequences are not the same. For example, if player A records
    /// their match result and then player B records their result for their (different) match, the
    /// order of these can be swapped without issue.
    ///
    /// The algorithm: For each operation in the given slice, this slice is walked start to finish
    /// until one of the following happens.
    ///     1) An operation identical to the first event in the given log is found. This operation
    ///        is removed from both logs and push onto the new log. We then move to the next
    ///        operation in the given log.
    ///     2) An operation that blocks the first operation in the given log is found. The new log
    ///        is applied, and the current logs are returned to be rectified.
    ///     3) The end of the sliced log is reached and the first operation of the given log is
    ///        removed and push onto the new log.
    /// If there are remaining elements in the sliced log, those are removed and pushed onto the
    /// new log.
    /// The new log is then returned.
    ///
    /// Every operation "knows" what it blocks.
    pub fn merge(self, other: OpSlice<T>) -> SyncStatus<T> {
        let mut agreed: Vec<FullOp<T>> = Vec::with_capacity(self.ops.len() + other.ops.len());
        let mut self_iter = self.ops.iter();
        let mut other_iter = self.ops.iter();
        while let Some(self_op) = self_iter.next() {
            // Our (the server's) rollbacks are ok
            if !self_op.active {
                agreed.push(self_op.clone());
                continue;
            }
            if let Some(other_op) = other_iter.next() {
                if !other_op.active {
                    // Their (the client's) rollbacks are not ok. They need to squash them or use
                    // our history.
                    return SyncStatus::SyncError(SyncError::RollbackFound(other));
                }
                if self_op.op == other_op.op {
                    agreed.push(self_op.clone());
                }
                for i_op in self_iter.clone() {
                    if i_op.op == other_op.op {
                        agreed.push(other_op.clone());
                        break;
                    } else if i_op.blocks(other_op) || other_op.blocks(i_op) {
                        // Blockage found!
                        return SyncStatus::InProgress(Blockage {
                            known: OpSlice {
                                ops: self_iter.cloned().collect(),
                            },
                            agreed: OpSlice { ops: agreed },
                            other: OpSlice {
                                ops: other_iter.cloned().collect(),
                            },
                            problem: (i_op.clone(), other_op.clone()),
                        });
                    }
                }
            } else {
                agreed.push(self_op.clone());
            }
        }
        SyncStatus::Completed(OpSync {
            ops: OpSlice { ops: agreed },
        })
    }
}

impl<T> Clone for FullOp<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            op: self.op.clone(),
            id: self.id,
            active: self.active,
        }
    }
}

impl<T> PartialEq for FullOp<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.op.eq(&other.op)
    }
}
