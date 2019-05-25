import asyncio


async def test():
    reader, writer = await asyncio.open_connection('127.0.0.1', 8888)
    data = await reader.read(100)

asyncio.run(test())
