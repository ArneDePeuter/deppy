import os
import json
import asyncio
import httpx

from deppy.wrappers.stated_kwargs import StatedKwargs

from octopus_api import OctopusApi
from octopus_deppy import OctopusDeppy


async def main():
    if not os.path.exists("./output"):
        os.makedirs("./output")

    async with httpx.AsyncClient(base_url="https://service.inaras.be/octopus-rest-api/v1") as client:
        stated_kwargs = StatedKwargs("./output/state.json")
        deppy = OctopusDeppy(
            api=OctopusApi(
                client=client,
                initial_modified_timestamp="2000-01-01 00:00:00.000",
                stated_kwargs=stated_kwargs
            ),
            software_house_uuid=os.getenv("SOFTWARE_HOUSE_UUID"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            locale_id=1
        )
        deppy.dot("./output/octopus_workflow.dot")

        with stated_kwargs:
            result = await deppy.execute()

        result.dot("./output/result.dot")
        with open("./output/result.json", "w") as f:
            f.write(json.dumps(result.dump(), indent=4, default=str))


if __name__ == "__main__":
    asyncio.run(main())
