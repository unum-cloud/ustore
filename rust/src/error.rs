use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataStoreError {
	#[error("unknown data store error")]
	Unknown,
}
