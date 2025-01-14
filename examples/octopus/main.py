import os
import json
import asyncio

from octopus import Octopus
from octopus_api import OctopusApi


async def main():
    if not os.path.exists("./output"):
        os.makedirs("./output")

    octopus = Octopus(
        api=OctopusApi(
            base_url="https://service.inaras.be/octopus-rest-api/v1",
            initial_modified_timestamp="2000-01-01 00:00:00.000",
            state_file="./output/state.json"
        ),
        software_house_uuid=os.getenv("SOFTWARE_HOUSE_UUID"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        locale_id=1
    )
    octopus.dot("./output/octopus_workflow.dot")

    async with octopus:
        result = await octopus.execute()

    result.dot("./output/result.dot")
    with open("./output/result.json", "w") as f:
        f.write(json.dumps(result.dump(), indent=4, default=str))


if __name__ == "__main__":
    asyncio.run(main())
