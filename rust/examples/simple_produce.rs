extern crate sentry_usage_accountant;

use clap::Parser;
use sentry_usage_accountant::{KafkaConfig, KafkaProducer, Producer};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka broker server in the host:port form
    #[arg(short, long)]
    bootstrap_server: String,

    /// Kafka topic to produce onto
    #[arg(short, long)]
    topic: String,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let kafka_config = KafkaConfig::new_producer_config(args.bootstrap_server.as_str(), None);
    let mut producer = KafkaProducer::new(kafka_config);

    let _ = producer.send("test_topic", "{a:1}".as_bytes());
}
