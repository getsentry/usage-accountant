import asyncio


class PeriodicRunner:
    def __init__(self, obj, queue, interval):
        self.obj = obj
        self.interval = interval
        self.queue = queue

    async def _run(self):
        result = await self.obj.get()
        await self.queue.put(result)
        await asyncio.sleep(self.interval)
        asyncio.create_task(self._run())

    def start(self):
        asyncio.create_task(self._run())
