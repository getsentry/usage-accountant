//! This is a library written to standardize the way we
//! account for shared resources used by each product in Rust.
//!
//! The problem we try to solve is to record the portion of a given
//! shared infra resource each feature use. An example of shared resource
//! is Relay, which is part of all ingestion pipelines. Recording this
//! ratio allows us to properly break down shared infra usage by
//! product feature.
//!
//! This api allows the application code to repeatedly record an amount
//! of resources used by a feature by choosing the unit of measure.
//! Every times the application code records usage, this refers to
//! a time period which is chosen by the application.
//!
//! Each time the application records an amount it provides these fields:
//!
//! - `resource_id` which identifies the physical resource. It could be
//!   a Kafka consumer, a portion of memory of a k8s pod, etc. This has
//!   to match the `shared_resource_id` assigned to the in its manifest.
//! - `app_feature` which identifies the product feature.
//! - `unit` which is the unit of measure of the amount recorded. It could
//!   be bytes, seconds, etc.
//! - `amount` which is the actual amount of the resource used.
//!
//! An instance of the usage-accountant accumulates usage data locally and
//! periodically flushes pre-aggregated data into Kafka. From there data
//! flows into Bigquery for analysis.
//! Accumulating data locally is critical to reduce the performance impact
//! of this library to a minimum and reduce the amount of Kafka messages.
//!
//! # Example
//!
//! ```
//! use sentry_usage_accountant::{KafkaConfig, UsageAccountant, UsageUnit};
//!
//! let kafka_config = KafkaConfig::new_producer_config(
//!     "localhost:9092",
//!     None
//! );
//! let mut accountant = UsageAccountant::new_with_kafka(
//!    kafka_config,
//!    None,
//!    None,
//! );
//! accountant.record(
//!    "generic_metrics_indexer_consumer",
//!    "transactions",
//!    100,
//!    UsageUnit::Bytes,
//! ).unwrap();
//! ```
//!

pub mod accountant;
mod accumulator;
mod producer;

pub use accountant::UsageAccountant;
#[doc(inline)]
pub use accumulator::UsageUnit;
pub use producer::{KafkaConfig, KafkaProducer, Producer};
