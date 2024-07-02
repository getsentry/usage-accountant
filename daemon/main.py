import argparse
import asyncio
import json
from aiokafka import AIOKafkaProducer

from fetchers import FetcherFactory
from periodic import PeriodicRunner
from config import Config


async def main(
    config_path: str, kafka_servers: str, kafka_topic: str, max_queue_size: int = 0
):
    cfg = Config.from_file(config_path)
    queue = asyncio.Queue(maxsize=max_queue_size)
    runners = []

    for fcfg in cfg:
        fetcher = FetcherFactory(fcfg["type"], **fcfg["args"])
        runner = PeriodicRunner(fetcher, queue, fcfg["period"])
        runners.append(runner)
        runner.start()

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_servers,
        loop=asyncio.get_running_loop(),
    )

    while item := await queue.get():
        await producer.send(kafka_topic, json.dumps(item))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetcher")

    # Add arguments
    parser.add_argument("--config_path", type=str, help="Path to config file")
    parser.add_argument("--kafka_servers", type=str, help="Kafka servers")
    parser.add_argument("--kafka_topic", type=str, help="Kafka topic name")
    parser.add_argument(
        "--max_queue_size", type=int, help="Maximum queue size", default=0
    )

    # Parse arguments from command line
    args = parser.parse_args()
    asyncio.run(
        main(
            args.config_path, args.kafka_servers, args.kafka_topic, args.max_queue_size
        )
    )
