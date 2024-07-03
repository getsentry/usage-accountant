import sys
import signal
import argparse
import asyncio

from fetchers import FetcherFactory
from periodic import PeriodicRunner
from config import Config
from usageaccountant import UsageAccumulator


async def main(config_path: str, max_queue_size: int = 0, dry_run: bool = False):
    config = Config.from_file(config_path)
    queue = asyncio.Queue(maxsize=max_queue_size)
    runners = []

    for fcfg in config.get_fetchers():
        fetcher = FetcherFactory(fcfg.type, **fcfg.args)
        runner = PeriodicRunner(fetcher, queue, fcfg.period)
        runners.append(runner)
        runner.start()

    try:
        if not dry_run:
            ua = UsageAccumulator(
                topic_name=config.get_kafka_topic(),
                kafka_config=config.get_kafka_config(),
            )
            signal.signal(signal.SIGTERM, lambda: ua.flush())
    except KeyError as e:
        print("Configuration is invalid: ", e)
        sys.exit(1)

    while item := await queue.get():
        if dry_run:
            print(item)
        else:
            ua.record(*item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetcher")

    parser.add_argument("--config", type=str, help="Path to config file")
    parser.add_argument(
        "--max_queue_size", type=int, help="Maximum queue size", default=0
    )
    parser.add_argument("--dry_run", action="store_true", help="Enable verbose mode")

    args = parser.parse_args()
    asyncio.run(main(args.config, args.max_queue_size, args.dry_run))
