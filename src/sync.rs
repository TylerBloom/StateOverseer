use crate::{FullOp, OpSlice, OverseerOperation};

/// A struct to help resolve syncing op logs
pub struct OpSync<T> {
    pub(crate) ops: OpSlice<T>,
}

/// An enum to help track the progress of the syncing of two op logs
pub enum SyncStatus<T> {
    /// There was an error when attempting to initially sync
    SyncError(SyncError<T>),
    /// There are discrepancies in between the two logs that are being synced
    InProgress(Blockage<T>),
    /// The logs have been successfully syncs
    Completed(OpSync<T>),
}

/// An enum to that captures the error that might occur when sync op logs.
/// `UnknownOperation` encodes that first operation in an OpSlice is unknown
/// `RollbackFound` encode that a rollback has occured remotely but not locally and returns an
/// OpSlice that contains everything since that rollback. When recieved, this new log should
/// overwrite the local log
pub enum SyncError<T> {
    /// One of the log was empty
    EmptySync,
    /// The starting operation of the slice in unknown to the other log
    UnknownOperation(FullOp<T>),
    /// One of the logs contains a rollback that the other doesn't have
    RollbackFound(OpSlice<T>),
}

/// An enum that encodes that errors that can occur during a rollback
pub enum RollbackError<T> {
    /// The rollback slice has an unknown starting point
    SliceError(SyncError<T>),
    /// The log that doesn't contain the rollback contains operations that the rolled back log
    /// doesn't contain
    OutOfSync(OpSync<T>),
}

/// A struct to help resolve blockages
pub struct Blockage<T> {
    pub(crate) known: OpSlice<T>,
    pub(crate) agreed: OpSlice<T>,
    pub(crate) other: OpSlice<T>,
    pub(crate) problem: (FullOp<T>, FullOp<T>),
}

/// A struct used to communicate a rollback
pub struct Rollback<T> {
    pub(crate) ops: OpSlice<T>,
}

impl<T> Blockage<T>
where
    T: OverseerOperation + Clone + PartialEq,
{
    /// Returns the problematic pair of operations.
    pub fn problem(&self) -> &(FullOp<T>, FullOp<T>) {
        &self.problem
    }

    /// Resolves the current problem by keeping the given solution and deleting the other, consuming self.
    ///
    /// The `Ok` varient is returned if the given operation is one of the problematic operations.
    /// It contains the rectified logs.
    ///
    /// The `Err` varient is returned if the given operation isn't one of the problematic operations, containing `self`.
    pub fn pick_and_continue(mut self, op: &FullOp<T>) -> SyncStatus<T> {
        if op == &self.problem.0 {
            self.agreed.add_op(self.problem.0.clone());
        } else if op == &self.problem.1 {
            self.agreed.add_op(self.problem.1.clone());
        } else {
            return SyncStatus::InProgress(self);
        }
        self.attempt_resolution()
    }

    /// Resolves the current problem by ordering the problematic solutions, consuming self.
    ///
    /// The `Ok` varient is returned if the given operation is one of the problematic operations.
    /// It contains the rectified logs.
    ///
    /// The `Err` varient is returned if the given operation isn't one of the problematic operations, containing `self`.
    pub fn order_and_continue(mut self, first: &FullOp<T>) -> SyncStatus<T> {
        if first == &self.problem.0 {
            self.agreed.add_op(self.problem.0.clone());
            self.agreed.add_op(self.problem.1.clone());
        } else if first == &self.problem.1 {
            self.agreed.add_op(self.problem.1.clone());
            self.agreed.add_op(self.problem.0.clone());
        } else {
            return SyncStatus::InProgress(self);
        }
        self.attempt_resolution()
    }

    /// Resolves the current problem by applying exactly one operation and putting the other back
    /// in its slice, consuming self.
    pub fn push_and_continue(mut self, apply: &FullOp<T>) -> SyncStatus<T> {
        if apply == &self.problem.0 {
            self.agreed.add_op(self.problem.0.clone());
            self.other.ops.insert(0, self.problem.1.clone());
        } else if apply == &self.problem.1 {
            self.agreed.add_op(self.problem.1.clone());
            self.known.ops.insert(0, self.problem.0.clone());
        } else {
            return SyncStatus::InProgress(self);
        }
        self.attempt_resolution()
    }

    fn attempt_resolution(mut self) -> SyncStatus<T> {
        match self.known.merge(self.other) {
            SyncStatus::Completed(sync) => {
                self.agreed.ops.extend(sync.ops.ops.into_iter());
                SyncStatus::Completed(OpSync { ops: self.agreed })
            }
            SyncStatus::InProgress(mut block) => {
                self.agreed.ops.extend(block.agreed.ops.into_iter());
                block.agreed = self.agreed;
                SyncStatus::InProgress(block)
            }
            SyncStatus::SyncError(e) => match e {
                SyncError::RollbackFound(roll) => {
                    SyncStatus::SyncError(SyncError::RollbackFound(roll))
                }
                SyncError::UnknownOperation(_) => {
                    unreachable!("There should be no unknown starting operations during the resolution of a blockage.");
                }
                SyncError::EmptySync => {
                    unreachable!(
                        "There should be no empty syncs during the resolution of a blockage"
                    );
                }
            },
        }
    }
}

impl<T> SyncStatus<T> {
    pub fn is_completed(&self) -> bool {
        match self {
            SyncStatus::Completed(_) => true,
            _ => false,
        }
    }
}
