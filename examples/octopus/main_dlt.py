from deppy.helpers.DLT import blueprint_to_source
from octopus import Octopus
import dlt


def main():
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_octopus",
        destination="duckdb",
        dataset_name="octopus",
        progress="log"
    )

    source = blueprint_to_source(Octopus)
    info = pipeline.run(source)
    print(info)


if __name__ == "__main__":
    main()
