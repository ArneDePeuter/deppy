import asyncio
import json
from deppy import Deppy, Cache


async def fetch_data():
    return [1, 2, 3]


def process_item(item):
    return [1] * item


def calculate_item(item):
    return item * 2


deppy = Deppy()

fetch_data_node = deppy.node(fetch_data)
process_item_node = deppy.node(process_item)
calculate_item_node = deppy.node(calculate_item)

process_item_node.item(fetch_data_node, loop=True)
calculate_item_node.item(process_item_node, loop=True)


async def main():
    print("Executing dependency graph...")
    result = await deppy.execute()
    print("Graph execution complete!")
    print("Result:")
    print(json.dumps(result.dump(str_keys=True)))

if __name__ == "__main__":
    asyncio.run(main())
