import asyncio
import json
from deppy import Deppy, Cache


def l():
    return [1, 2, 3]


def l2():
    return [4, 5, 6]


def add1(data):
    return data + 1

def add2(data):
    return data + 2

def mul2(data1, data2):
    return data1 * data2


deppy = Deppy()

l_node = deppy.node(l)
l2_node = deppy.node(l2)
add1_node = deppy.node(add1)
add2_node = deppy.node(add2)
mul2_node = deppy.node(mul2)

add1_node.data(l_node, loop=True)
add2_node.data(l2_node, loop=True)
mul2_node.data1(add1_node).data2(add2_node)


async def main():
    result = await deppy.execute()
    import json
    print(json.dumps(result.dump(str_keys=True)))


if __name__ == "__main__":
    asyncio.run(main())
