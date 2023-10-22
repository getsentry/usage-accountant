extern crate sentry_usage_accountant;

use clap::Parser;
use sentry_usage_accountant::accountant::UsageAccountant;
use sentry_usage_accountant::accumulator::UsageUnit;
use sentry_usage_accountant::producer::KafkaConfig;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka broker server in the host:port form
    #[arg(short, long)]
    bootstrap_server: String,
}

fn main() {
    let args = Args::parse();

    let kafka_config = KafkaConfig::new_producer_config(args.bootstrap_server, None);
    let mut accountant = UsageAccountant::new(
        kafka_config,
        None,
        None,
    );

    accountant.record(
        "my_resource".to_string(),
        "my_feature".to_string(),
        100,
        UsageUnit::Bytes,
    ).unwrap();
    accountant.record(
        "my_resource".to_string(),
        "my_feature".to_string(),
        100,
        UsageUnit::Bytes,
    ).unwrap();
    accountant.record(
        "my_resource".to_string(),
        "another_feature".to_string(),
        100,
        UsageUnit::Bytes,
    ).unwrap();
    accountant.record(
        "my_resource".to_string(),
        "yet_another_feature".to_string(),
        100,
        UsageUnit::Bytes,
    ).unwrap();
    accountant.flush().unwrap();
}
