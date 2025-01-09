from deppy import Deppy, Cache


deppy = Deppy()


def a():
    print("Computing a")
    return 2


def l1():
    return [1, 2, 3]


def l2():
    return [4, 5, 6]


def calculate(value1, value2, coeff):
    return (value1 + value2) * coeff


def multiply(value1, value2):
    return value1 * value2


# Build the graph
a_node = deppy.node(a, cache=Cache(ttl=5))
l1_node = deppy.node(l1)
l2_node = deppy.node(l2)
calculate_node = deppy.node(calculate)
multiply_node = deppy.node(multiply)

calculate_node.value1(l1_node, loop=True)
calculate_node.value2(l2_node, loop=True)
calculate_node.coeff(a_node)
multiply_node.value1(calculate_node, loop=True)
multiply_node.value2(a_node)

# Execute the graph
result = multiply_node.execute()
print(result)

