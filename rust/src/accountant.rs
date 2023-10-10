





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
