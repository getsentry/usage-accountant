use crate::accumulator::{UsageUnit, UsageAccumulator};
use crate::producer::{ClientError, KafkaConfig, Producer, KafkaProducer};
use chrono::{Local};
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
                message.as_str().unwrap().as_bytes(),
            )?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    /*use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Message;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{ThreadedProducer, NoCustomPartitioner,
        DefaultProducerContext, BaseRecord};

    #[test]
    fn test_base() {
        const TOPIC: &str = "test_topic";
        let mock_cluster = MockCluster::new(3).unwrap();
        mock_cluster
            .create_topic(TOPIC, 32, 3)
            .expect("Failed to create topic");

        let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        let mut i = 0_usize;
        loop {
            producer
                .send(
                    BaseRecord::to(TOPIC)
                        .key(&i.to_string())
                        .payload("dummy")
                ).unwrap();
            i += 1;
            if i > 10 {
                break;
            }
        }

    }*/
}
