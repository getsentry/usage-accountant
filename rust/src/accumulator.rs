//! This module contains the structure that accumulates usage data
//! locally before flushing to Kafka.
//!
//! The accumulator pre-aggregates usage per timestamp based on
//! the granularity provided at instantiation.
//!

use std::fmt;
use std::collections::HashMap;
use chrono::{DateTime, Duration, Local, DurationRound};
use std::mem;

/// The unit of measures we support when recording usage.
/// more can be added.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UsageUnit {
    Milliseconds,
    Bytes,
    MillisecondsSec,
}

impl fmt::Display for UsageUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UsageUnit::Milliseconds => write!(f, "milliseconds"),
            UsageUnit::Bytes => write!(f, "bytes"),
            UsageUnit::MillisecondsSec => write!(f, "milliseconds_sec"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UsageKey {
    pub quantized_timestamp: DateTime<Local>,
    pub resource_id: String,
    pub app_feature: String,
    pub unit: UsageUnit
}

pub struct UsageAccumulator {
    usage_batch: HashMap<UsageKey, u64>,
    granularity_sec: u32,
    first_timestamp: Option<DateTime<Local>>,
}

impl UsageAccumulator {
    /// Constructs a new Accumulator. Here is where the granularity
    /// is provided.
    pub fn new(granularity_sec: Option<u32>) -> Self {
        Self {
            usage_batch: HashMap::new(),
            granularity_sec:  granularity_sec.unwrap_or(60),
            first_timestamp: None
        }
    }

    /// Records an amount of usage for a resource, app_feature, timestamp
    /// tuple.
    ///
    /// The timestamp provided is then quantized according to the
    /// granularity this structure is instantiated with so data is
    /// bucketed with fixed bucket sizes.
    /// The system timestamp should be passed in most cases.
    pub fn record(
        &mut self,
        usage_time: DateTime<Local>,
        resource_id: String,
        app_feature: String,
        amount: u64,
        usage_unit: UsageUnit,
    ) {
        let quantized_timestamp: DateTime<Local> = usage_time.duration_trunc(
            Duration::seconds(i64::from(self.granularity_sec))
        ).unwrap();

        if self.first_timestamp.is_none() {
            self.first_timestamp = Some(quantized_timestamp);
        }

        let key = UsageKey {
            quantized_timestamp,
            resource_id,
            app_feature,
            unit: usage_unit,
        };

        let mut curr_value = 0;
        if !self.usage_batch.contains_key(&key) {
            self.usage_batch.insert(key.clone(), 0);
        } else {
            curr_value = self.usage_batch[&key];
        }

        self.usage_batch.insert(key, curr_value + amount);
    }

    /// Returns true if the bucket is ready to be flushed.
    ///
    /// Ready to be flushed means that the bucket is not empty
    /// and at least `granularity_sec` seconds have passed since
    /// the first chunk of data was added.
    pub fn should_flush(&self, current_time: DateTime<Local>) -> bool {
        return
            self.first_timestamp.is_some() &&
            self.usage_batch.keys().len() > 0 &&
            current_time - self.first_timestamp.unwrap() > Duration::seconds(
                i64::from(self.granularity_sec)
            );
    }

    /// Return the current bucket and clears up the state.
    pub fn flush(&mut self) -> HashMap<UsageKey, u64> {
        self.first_timestamp = None;
        let mut ret_val: HashMap<UsageKey, u64> = HashMap::new();
        mem::swap(&mut self.usage_batch, &mut ret_val);
        ret_val
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone};
    use std::collections::HashMap;
    use super::{UsageUnit, UsageKey, UsageAccumulator};

    #[test]
    fn empty_batch() {
        let mut accumulator = UsageAccumulator::new(None);
        assert!(!accumulator.should_flush(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap()
        ));
        assert!(!accumulator.should_flush(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 16, 25).unwrap()
        ));

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }

    #[test]
    fn test_multiple_entries() {
        let mut accumulator = UsageAccumulator::new(None);
        accumulator.record(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap(),
            "genericmetrics_consumer".to_string(),
            "transactions".to_string(),
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 45).unwrap(),
            "genericmetrics_consumer".to_string(),
            "spans".to_string(),
            200,
            UsageUnit::Milliseconds,
        );

        assert!(!accumulator.should_flush(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap()
        ));
        assert!(accumulator.should_flush(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 16, 25).unwrap()
        ));
        let ret = accumulator.flush();
        let test_val = HashMap::from([
            (UsageKey{
                quantized_timestamp: Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                resource_id: "genericmetrics_consumer".to_string(),
                app_feature: "transactions".to_string(),
                unit: UsageUnit::Milliseconds,
            }, 100),
            (UsageKey{
                quantized_timestamp: Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                resource_id: "genericmetrics_consumer".to_string(),
                app_feature: "spans".to_string(),
                unit: UsageUnit::Milliseconds,
            }, 200),
        ]);
        assert_eq!(ret, test_val);

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }

    #[test]
    fn test_merge_entries() {
        let mut accumulator = UsageAccumulator::new(None);
        accumulator.record(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 25).unwrap(),
            "genericmetrics_consumer".to_string(),
            "transactions".to_string(),
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 45).unwrap(),
            "genericmetrics_consumer".to_string(),
            "transactions".to_string(),
            100,
            UsageUnit::Milliseconds,
        );
        accumulator.record(
            Local.with_ymd_and_hms(2023, 10, 8, 22, 16, 45).unwrap(),
            "genericmetrics_consumer".to_string(),
            "transactions".to_string(),
            100,
            UsageUnit::Milliseconds,
        );

        let ret = accumulator.flush();
        let test_val = HashMap::from([
            (UsageKey{
                quantized_timestamp: Local.with_ymd_and_hms(2023, 10, 8, 22, 15, 0).unwrap(),
                resource_id: "genericmetrics_consumer".to_string(),
                app_feature: "transactions".to_string(),
                unit: UsageUnit::Milliseconds,
            }, 200),
            (UsageKey{
                quantized_timestamp: Local.with_ymd_and_hms(2023, 10, 8, 22, 16, 0).unwrap(),
                resource_id: "genericmetrics_consumer".to_string(),
                app_feature: "transactions".to_string(),
                unit: UsageUnit::Milliseconds,
            }, 100),
        ]);
        assert_eq!(ret, test_val);

        let message = accumulator.flush();
        assert_eq!(message.keys().len(), 0);
    }
}
