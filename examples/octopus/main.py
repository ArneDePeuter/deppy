import os
import json
import asyncio

from deppy.helpers.wrappers import StatedKwargs

from octopus_api import OctopusApi
from octopus_deppy import OctopusDeppy


async def main():
    if not os.path.exists("./output"):
        os.makedirs("./output")

    stated_kwargs = StatedKwargs("./output/state.json")
    octopus_api = OctopusApi(
        base_url="https://service.inaras.be/octopus-rest-api/v1",
        initial_modified_timestamp="2000-01-01 00:00:00.000",
        stated_kwargs=stated_kwargs
    )
    deppy = OctopusDeppy(
        api=octopus_api,
        software_house_uuid=os.getenv("SOFTWARE_HOUSE_UUID"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        locale_id=1
    )
    deppy.dot("./output/octopus_workflow.dot")

    async with octopus_api:
        with stated_kwargs:
            result = await deppy.execute()

    result.dot("./output/result.dot")
    with open("./output/result.json", "w") as f:
        f.write(json.dumps(result.dump(), indent=4, default=str))


if __name__ == "__main__":
    asyncio.run(main())
