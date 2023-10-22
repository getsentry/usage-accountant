use crate::accumulator::{UsageUnit, UsageAccumulator};
use crate::producer::{ClientError, KafkaConfig, Producer, KafkaProducer};
use chrono::Local;
use serde_json::json;

static DEFAULT_TOPIC_NAME: &str = "shared-resources-usage";
pub struct UsageAccountant<'a> {
    accumulator: UsageAccumulator,
    producer: Box<dyn Producer + 'a>,
    topic: String,
}

impl<'a> UsageAccountant<'a> {
    pub fn new(
        producer_config: KafkaConfig,
        topic_name: Option<String>,
        granularity_sec: Option<u32>,
    ) -> UsageAccountant<'a> {
        UsageAccountant::new_with_producer(
            Box::new(KafkaProducer::new(producer_config)),
            topic_name,
            granularity_sec,
        )
    }

    pub fn new_with_producer(
        producer: Box<dyn Producer + 'a>,
        topic_name: Option<String>,
        granularity_sec: Option<u32>,
    ) -> UsageAccountant<'a> {
        let topic = match topic_name {
            None => DEFAULT_TOPIC_NAME.to_string(),
            Some(name) => name,
        };

        UsageAccountant {
            accumulator: UsageAccumulator::new(granularity_sec),
            producer,
            topic,
        }
    }

    pub fn record(
        &mut self,
        resource_id: String,
        app_feature: String,
        amount: u64,
        unit: UsageUnit,
    ) -> Result<(), ClientError> {
        let current_time = Local::now();
        self.accumulator.record(current_time, resource_id, app_feature, amount, unit);
        if self.accumulator.should_flush(current_time) {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(
        &mut self,
    ) -> Result<(), ClientError> {
        let flushed_content = self.accumulator.flush();
        for (key, amount) in flushed_content {
            let message = json!({
                "timestamp": key.quantized_timestamp.timestamp(),
                "shared_resource_id": key.resource_id,
                "app_feature": key.app_feature,
                "usage_unit": key.unit.to_string(),
                "amount": amount,
            });

            self.producer.send(
                self.topic.clone(),
                message.to_string().as_bytes(),
            )?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::UsageAccountant;
    use super::super::accumulator::UsageUnit;
    use super::super::producer::DummyProducer;
    use std::cell::RefCell;
    use serde_json::Value;
    use std::str::from_utf8;

    #[test]
    fn test_empty_batch() {
        let messages = RefCell::new(Vec::new());
        let producer = DummyProducer{messages: &messages};
        let mut accountant = UsageAccountant::new_with_producer(
            Box::new(producer),
            None,
            None,
        );

        let res = accountant.flush();
        assert!(res.is_ok());
        assert_eq!(messages.borrow().len(), 0);
    }

    #[test]
    fn test_three_messages() {
        let messages = RefCell::new(Vec::new());
        let producer = DummyProducer{
            messages: &messages,
        };
        let mut accountant = UsageAccountant::new_with_producer(
            Box::new(producer),
            None,
            None,
        );

        let res1 = accountant.record(
            "resource_1".to_string(),
            "transactions".to_string(),
            100, UsageUnit::Bytes);
        assert!(res1.is_ok());
        let res2 = accountant.record(
            "resource_1".to_string(),
            "spans".to_string(),
            200, UsageUnit::Bytes);
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
