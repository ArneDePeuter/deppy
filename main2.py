import asyncio
import json
from deppy import Deppy, Cache


def l():
    return [1, 2, 3]


def coeff():
    return 2


def other(data):
    return data * 3


def item1(data):
    return data * 2


def item2(data):
    return data * 3


def item3(data1, data2, coeff):
    return data1, data2, coeff


deppy = Deppy()

l_node = deppy.node(l)
item1_node = deppy.node(item1)
item2_node = deppy.node(item2)
item3_node = deppy.node(item3)
coeff_node = deppy.node(coeff)
other_node = deppy.node(other)

item1_node.data(l_node, loop=True)
other_node.data(l_node, loop=True)
item2_node.data(item1_node)
item3_node.data1(item1_node).data2(item2_node).coeff(coeff_node)

deppy.dot("main2.dot")


async def main():
    result = await deppy.execute()
    import json
    print(json.dumps(result.dump(str_keys=True)))


if __name__ == "__main__":
    asyncio.run(main())
