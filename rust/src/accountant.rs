use crate::accumulator::{UsageAccumulator, UsageUnit};
use crate::producer::{ClientError, KafkaConfig, KafkaProducer, Producer};
use chrono::{Duration, Utc};
use serde::Serialize;
use std::ops::Drop;

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

pub struct UsageAccountant<P: Producer> {
    accumulator: UsageAccumulator,
    producer: P,
    topic: String,
}

#[derive(Serialize)]
struct Message {
    timestamp: i64,
    shared_resource_id: String,
    app_feature: String,
    usage_unit: UsageUnit,
    amount: u64,
}

impl UsageAccountant<KafkaProducer> {
    /// Instantiates a UsageAccountant from a Kafka config object.
    /// This initialization method lets the `UsageAccountant` create
    /// the producer and own it.
    ///
    /// You should very rarely change topic name or granularity.
    pub fn new_with_kafka(
        producer_config: KafkaConfig,
        topic_name: Option<&str>,
        granularity: Option<Duration>,
    ) -> UsageAccountant<KafkaProducer> {
        UsageAccountant::new(KafkaProducer::new(producer_config), topic_name, granularity)
    }
}

impl<P: Producer> UsageAccountant<P> {
    /// Instantiates a UsageAccountant by leaving the responsibility
    /// to provide a producer to the client.
    pub fn new(producer: P, topic_name: Option<&str>, granularity: Option<Duration>) -> Self {
        let topic = topic_name.unwrap_or(DEFAULT_TOPIC_NAME).to_string();

        UsageAccountant {
            accumulator: UsageAccumulator::new(granularity),
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
        let current_time = Utc::now();
        self.accumulator
            .record(current_time, resource_id, app_feature, amount, unit);
        if self.accumulator.should_flush(current_time) {
            self.flush()?;
        }
        Ok(())
    }

    /// Forces a flush of the existing batch.
    ///
    /// This method is called automatically when the Accountant
    /// goes out of scope.
    fn flush(&mut self) -> Result<(), ClientError> {
        let flushed_content = self.accumulator.flush();
        for (key, amount) in flushed_content {
            let message = Message {
                timestamp: key.quantized_timestamp.timestamp(),
                shared_resource_id: key.resource_id,
                app_feature: key.app_feature,
                usage_unit: key.unit,
                amount,
            };

            if let Ok(msg) = serde_json::to_string(&message) {
                self.producer.send(self.topic.as_str(), msg.as_bytes())?;
            }
        }
        Ok(())
    }
}

impl<P: Producer> Drop for UsageAccountant<P> {
    fn drop(&mut self) {
        _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::super::accumulator::UsageUnit;
    use super::super::producer::DummyProducer;
    use super::UsageAccountant;
    use serde_json::Value;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::str::from_utf8;

    #[test]
    fn test_empty_batch() {
        let messages = Rc::new(RefCell::new(Vec::new()));
        let producer = DummyProducer {
            messages: Rc::clone(&messages),
        };
        let mut accountant = UsageAccountant::new(producer, None, None);

        let res = accountant.flush();
        assert!(res.is_ok());
        assert_eq!(messages.borrow().len(), 0);
    }

    #[test]
    fn test_three_messages() {
        let messages = Rc::new(RefCell::new(Vec::new()));
        let producer = DummyProducer {
            messages: Rc::clone(&messages),
        };
        let mut accountant = UsageAccountant::new(producer, None, None);

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
        assert_eq!(m1["usage_unit"], "bytes".to_string());

        let payload2 = from_utf8(&(messages_vec[1].1)).unwrap();
        let m2: Value = serde_json::from_str(payload2).unwrap();
        assert_eq!(m2["shared_resource_id"], "resource_1".to_string());
        assert_eq!(m2["usage_unit"], "bytes".to_string());

        // The messages will not necessarily be in order.
        assert_ne!(m1["app_feature"], m2["app_feature"]);
        if m1["app_feature"] == "transactions" {
            assert_eq!(m2["app_feature"], "spans".to_string())
        } else {
            assert_eq!(m2["app_feature"], "transactions".to_string());
            assert_eq!(m1["app_feature"], "spans".to_string());
        }
        assert_ne!(m1["amount"], m2["amount"]);

        let res = accountant.flush();
        assert!(res.is_ok());
        // Messages are still the same we had before the previous step.
        assert_eq!(messages.borrow().len(), 2);
    }
}
