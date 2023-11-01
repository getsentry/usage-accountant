//! This module provides an abstraction over a Kafka producer in
//! order to allow client code to instantiate the producer
//! implementation they want without depending on the rdkafka
//! ThreadedProducer.
//!
//! It also simplify unit tests.

use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::ClientContext;
#[cfg(test)]
use std::cell::RefCell;
use std::collections::HashMap;
#[cfg(test)]
use std::rc::Rc;
use thiserror::Error;
use tracing::{event, Level};

/// This structure wraps the parameters to initialize a producer.
/// This struct is there in order not to expose the rdkafka
/// details outside.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    config_map: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new_producer_config(
        bootstrap_servers: &str,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("bootstrap.servers".to_string(), bootstrap_servers.into());

        let config = Self { config_map };

        apply_override_params(config, override_params)
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(item: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in item.config_map.iter() {
            config_obj.set(key, val);
        }
        config_obj
    }
}

fn apply_override_params<V>(
    mut config: KafkaConfig,
    override_params: Option<HashMap<String, V>>,
) -> KafkaConfig
where
    V: Into<String>,
{
    if let Some(params) = override_params {
        for (param, value) in params {
            config.config_map.insert(param, value.into());
        }
    }
    config
}

struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                event!(Level::DEBUG, "Message produced.")
            }
            Err((kafka_err, _)) => {
                event!(Level::ERROR, "Message production failed. {}", kafka_err)
            }
        }
    }
}

/// Kafka producer errors.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Failed to send a kafka message.
    #[error("failed to send kafka message")]
    SendFailed(#[source] rdkafka::error::KafkaError),

    /// Failed to create a kafka producer because of the invalid configuration.
    #[error("failed to create kafka producer: invalid kafka config")]
    InvalidConfig(#[source] rdkafka::error::KafkaError),
}

/// A basic Kafka Producer trait.
///
/// We do not neet to set headers or key for this data.
pub trait Producer {
    fn send(&mut self, topic_name: &str, payload: &[u8]) -> Result<(), ClientError>;
}

pub struct KafkaProducer {
    producer: ThreadedProducer<CaptureErrorContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> KafkaProducer {
        let producer_config: RdKafkaConfig = config.into();
        KafkaProducer {
            producer: producer_config
                .create_with_context(CaptureErrorContext)
                .expect("Producer creation error"),
        }
    }
}

impl Producer for KafkaProducer {
    fn send(&mut self, topic_name: &str, payload: &[u8]) -> Result<(), ClientError> {
        let record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic_name).payload(payload);
        self.producer
            .send(record)
            .map_err(|(error, _message)| ClientError::SendFailed(error))
    }
}

#[cfg(test)]
pub(crate) struct DummyProducer {
    pub messages: Rc<RefCell<Vec<(String, Vec<u8>)>>>,
}

#[cfg(test)]
impl Producer for DummyProducer {
    fn send(&mut self, topic_name: &str, payload: &[u8]) -> Result<(), ClientError> {
        self.messages
            .borrow_mut()
            .push((topic_name.to_string(), payload.to_vec()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::DummyProducer;
    use super::KafkaConfig;
    use super::Producer;
    use rdkafka::config::ClientConfig as RdKafkaConfig;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    #[test]
    fn test_build_producer_configuration() {
        let config = KafkaConfig::new_producer_config(
            "localhost:9092",
            Some(HashMap::from([(
                "queued.max.messages.kbytes".to_string(),
                "1000000".to_string(),
            )])),
        );

        let rdkafka_config: RdKafkaConfig = config.into();
        assert_eq!(
            rdkafka_config.get("queued.max.messages.kbytes"),
            Some("1000000")
        );
    }

    #[test]
    fn test_dummy_producer() {
        let messages = Rc::new(RefCell::new(Vec::new()));
        let mut producer = DummyProducer {
            messages: Rc::clone(&messages),
        };
        let res = producer.send("topic", "message".as_bytes());
        assert!(res.is_ok());

        assert_eq!(producer.messages.borrow().len(), 1);
    }
}
