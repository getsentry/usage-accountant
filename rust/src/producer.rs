/**
 * - create config structure
 * - create trait
 * - create mock
 * - create context
 * - implement the threading one
 */

use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::KafkaConfig;
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
}
