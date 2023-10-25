use crate::accumulator::{UsageAccumulator, UsageUnit};
use crate::producer::{ClientError, KafkaConfig, KafkaProducer, Producer};
use chrono::{Duration, Local};
use serde_json::json;

static DEFAULT_TOPIC_NAME: &str = "shared-resources-usage";
/// This is the entry point for the library. It is in most cases
/// everything you need to instrument your application.
///
/// The UsageAccountant needs a `Producer` and a `UsageAccumulator`.
/// Data is stored in the accumulator and, periodically, it is
/// flushed into the Kafka topic via a consumer.
///
/// Accumulating data locally is critical to reduce the performance impact
/// of this library to a minimum and reduce the amount of Kafka messages.
/// This means that this structure should be instantiated rarely.
/// Possibly only once per application (or per thread).
///
/// Avoid creating a UsageAccountant every time some data needs to
/// be recorded.

pub struct UsageAccountant<'a> {
    accumulator: UsageAccumulator,
    producer: Box<dyn Producer + 'a>,
    topic: String,
}

impl<'a> UsageAccountant<'a> {
    /// Instantiates a UsageAccountant from a Kafka config object.
    /// This initialization method lets the `UsageAccountant` create
    /// the producer and own it.
    ///
    /// You should very rarely change topic name or granularity.
    pub fn new(
        producer_config: KafkaConfig,
        topic_name: Option<&str>,
        granularity_sec: Option<Duration>,
    ) -> UsageAccountant<'a> {
        UsageAccountant::new_with_producer(
            Box::new(KafkaProducer::new(producer_config)),
            topic_name,
            granularity_sec,
        )
    }

    /// Leaves the responsibility to provide a producer to the
    /// client. Most of the times you should not need to use this.
    pub fn new_with_producer(
        producer: Box<dyn Producer + 'a>,
        topic_name: Option<&str>,
        granularity_sec: Option<Duration>,
    ) -> UsageAccountant<'a> {
        let topic = topic_name.unwrap_or(DEFAULT_TOPIC_NAME).to_string();

        UsageAccountant {
            accumulator: UsageAccumulator::new(granularity_sec),
            producer,
            topic,
        }
    }

    /// Records an mount of usage for a resource, and app_feature.
    ///
    /// It flushes the batch if that is ready to be flushed.
    /// The timestamp used is the system timestamp.
    pub fn record(
        &mut self,
        resource_id: &str,
        app_feature: &str,
        amount: u64,
        unit: UsageUnit,
    ) -> Result<(), ClientError> {
        let current_time = Local::now();
        self.accumulator
            .record(current_time, resource_id, app_feature, amount, unit);
        if self.accumulator.should_flush(current_time) {
            self.flush()?;
        }
        Ok(())
    }

    /// Forces a flush of the existing batch.
    ///
    /// This method should be called manually only when the application
    /// is about to shut down or the `UsageAccountant` is about to be
    /// destroyed.
    pub fn flush(&mut self) -> Result<(), ClientError> {
        let flushed_content = self.accumulator.flush();
        for (key, amount) in flushed_content {
            let message = json!({
                "timestamp": key.quantized_timestamp.timestamp(),
                "shared_resource_id": key.resource_id,
                "app_feature": key.app_feature,
                "usage_unit": key.unit,
                "amount": amount,
            });

            self.producer
                .send(self.topic.as_str(), message.to_string().as_bytes())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::accumulator::UsageUnit;
    use super::super::producer::DummyProducer;
    use super::UsageAccountant;
    use serde_json::Value;
    use std::cell::RefCell;
    use std::str::from_utf8;

    #[test]
    fn test_empty_batch() {
        let messages = RefCell::new(Vec::new());
        let producer = DummyProducer {
            messages: &messages,
        };
        let mut accountant = UsageAccountant::new_with_producer(Box::new(producer), None, None);

        let res = accountant.flush();
        assert!(res.is_ok());
        assert_eq!(messages.borrow().len(), 0);
    }

    #[test]
    fn test_three_messages() {
        let messages = RefCell::new(Vec::new());
        let producer = DummyProducer {
            messages: &messages,
        };
        let mut accountant = UsageAccountant::new_with_producer(Box::new(producer), None, None);

        let res1 = accountant.record("resource_1", "transactions", 100, UsageUnit::Bytes);
        assert!(res1.is_ok());
        let res2 = accountant.record("resource_1", "spans", 200, UsageUnit::Bytes);
        assert!(res2.is_ok());

        let res = accountant.flush();
        assert!(res.is_ok());
        let messages_vec = messages.borrow();
        assert_eq!(messages_vec.len(), 2);

        let topic = messages_vec[0].0.clone();
        assert_eq!(topic, "shared-resources-usage");
        let payload1 = from_utf8(&(messages_vec[0].1)).unwrap();
        let m1: Value = serde_json::from_str(payload1).unwrap();
        assert_eq!(m1["shared_resource_id"], "resource_1".to_string());
        assert_eq!(m1["app_feature"], "transactions".to_string());
        assert_eq!(m1["usage_unit"], "bytes".to_string());
        assert_eq!(m1["amount"], 100);

        let payload2 = from_utf8(&(messages_vec[1].1)).unwrap();
        let m2: Value = serde_json::from_str(payload2).unwrap();
        assert_eq!(m2["shared_resource_id"], "resource_1".to_string());
        assert_eq!(m2["app_feature"], "spans".to_string());
        assert_eq!(m2["usage_unit"], "bytes".to_string());
        assert_eq!(m2["amount"], 200);

        let res = accountant.flush();
        assert!(res.is_ok());
        // Messages are still the same we had before the previous step.
        assert_eq!(messages.borrow().len(), 2);
    }
}
