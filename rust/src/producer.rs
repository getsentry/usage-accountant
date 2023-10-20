/**
 * - create trait
 * - create mock
 * - create context
 * - implement the threading one
 */

use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::{ClientContext, Message};
use rdkafka::producer::{BaseRecord, ThreadedProducer};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    config_map: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new_config<V>(
        bootstrap_servers: V,
        override_params: Option<HashMap<String, V>>,
    ) -> Self
    where
        V: Into<String>,
    {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("bootstrap.servers".to_string(), bootstrap_servers.into());

        let config = Self { config_map };

        apply_override_params(config, override_params)
    }

    pub fn new_producer_config<V>(
        bootstrap_servers: V,
        override_params: Option<HashMap<String, String>>,
    ) -> Self
    where
        V: Into<String>,
    {
        let config = KafkaConfig::new_config(bootstrap_servers, None);

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
    match override_params {
        Some(params) => {
            for (param, value) in params {
                config.config_map.insert(param, value.into());
            }
        }
        None => {}
    }
    config
}

pub struct CaptureErrorContext;

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        // TODO: Do something useful here
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

pub trait Producer {
    fn send(&mut self, topic_name: String, payload: &[u8]) -> Result<(), ClientError>;
}

pub struct DummyProducer {
    messages: Vec<(String, Vec<u8>)>
}

impl Producer for DummyProducer {
    fn send(&mut self, topic_name: String, payload: &[u8]) -> Result<(), ClientError> {
        self.messages.push((topic_name.clone(), payload.to_vec()));
        Ok(())
    }
}

pub struct KafkaProducer {
    producer: ThreadedProducer<CaptureErrorContext>,
}

impl KafkaProducer {
    pub fn new(config: KafkaConfig) -> KafkaProducer{
        let producer_config: RdKafkaConfig = config.into();
        KafkaProducer {
            producer: producer_config
                .create_with_context(CaptureErrorContext)
                .expect("Producer creation error")
        }
    }
}

impl Producer for KafkaProducer {
    fn send(&mut self, topic_name: String, payload: &[u8]) -> Result<(), ClientError> {
        let record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic_name.as_str()).payload(payload);
        self.producer.send(record).map_err(|(error, _message)| {
            ClientError::SendFailed(error)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaConfig;
    use super::DummyProducer;
    use super::Producer;
    use rdkafka::config::ClientConfig as RdKafkaConfig;
    use std::collections::HashMap;

    #[test]
    fn test_build_producer_configuration() {
        let config = KafkaConfig::new_producer_config(
            "localhost:9092".to_string(),
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
        let mut producer = DummyProducer{messages: Vec::new()};
        let res = producer.send("topic".to_string(), "message".as_bytes());
        assert!(res.is_ok());

        assert_eq!(producer.messages.len(), 1);
    }
}
