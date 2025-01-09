import asyncio
from deppy import Deppy

# Instantiate the dependency resolver
deppy = Deppy()

@deppy.node
async def a():
    print("Executing Node A")
    await asyncio.sleep(1)
    return 2

@deppy.node
def b():
    print("Executing Node B")
    return [1, 2, 3]

@deppy.node
async def c(val1, val2):
    print(f"Executing Node C with val1={val1} and val2={val2}")
    await asyncio.sleep(0.5)
    return val1 * val2

# Add loop variables
c.val1(a)
c.val2(b, loop=True)

async def main():
    print("Starting Execution")
    results = await deppy.execute()
    print("Execution Results:", results)

asyncio.run(main())
