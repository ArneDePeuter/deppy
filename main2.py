import asyncio
import json
from deppy import Deppy, Cache


def l():
    return [1, 2, 3]


def item1(data):
    return data * 2


def item2(data):
    return data * 3


def item3(data1, data2):
    return data1, data2


deppy = Deppy()

l_node = deppy.node(l)
item1_node = deppy.node(item1)
item2_node = deppy.node(item2)
item3_node = deppy.node(item3)

item1_node.data(l_node, loop=True)
item2_node.data(item1_node)
item3_node.data1(item1_node)
item3_node.data2(item2_node)


async def main():
    result = await deppy.execute()
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
