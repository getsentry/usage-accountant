import asyncio


class PeriodicRunner:
    def __init__(self, obj, queue, interval):
        self.obj = obj
        self.interval = interval
        self.queue = queue

    async def _run(self):
        results = await self.obj.get()
        asyncio.gather(*[self.queue.put(item) for item in results])
        await asyncio.sleep(self.interval)
        asyncio.create_task(self._run())

    def start(self):
        asyncio.create_task(self._run())
