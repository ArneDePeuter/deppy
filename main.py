import asyncio
import json
from deppy import Deppy, Cache


async def fetch_data():
    print("Fetching data...")
    await asyncio.sleep(1)
    return [1, 2, 3]


def process_data(data):
    return [x * 2 for x in data]


async def combine_data(item, multiplier=2):
    await asyncio.sleep(0.5)
    return item * multiplier

deppy = Deppy()

fetch_data_node = deppy.node(fetch_data, cache=Cache(max_uses=1, ttl=10))
process_data_node = deppy.node(process_data)
combine_data_node = deppy.node(combine_data)

process_data_node.data(fetch_data_node)
combine_data_node.item(process_data_node, loop=True)


async def main():
    print("Executing dependency graph...")
    result = await deppy.execute()
    print("Graph execution complete!")
    print("Result:")
    for node, value in result.items():
        print(f"\t {node}: {json.dumps(value)}")

if __name__ == "__main__":
    asyncio.run(main())
