from deppy.helpers.DLT import deppy_to_source
from octopus import Octopus
import dlt


def main():
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_octopus",
        destination="duckdb",
        dataset_name="octopus",
        progress="log"
    )

    octopus_deppy = Octopus()
    source = deppy_to_source(octopus_deppy)
    info = pipeline.run(source)
    print(info)


if __name__ == "__main__":
    main()
