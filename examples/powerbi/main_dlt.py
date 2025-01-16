from deppy.helpers.DLT import blueprint_to_source
from powerbi import PowerBI
import dlt


def main():
    pipeline = dlt.pipeline(
        pipeline_name="powerbi_octopus",
        destination="duckdb",
        dataset_name="octopus",
        progress="log",
    )

    source = blueprint_to_source(PowerBI)
    info = pipeline.run(source)
    print(info)


if __name__ == "__main__":
    main()
