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

    source = deppy_to_source(
        Octopus,
        secrets={"software_house_uuid", "user", "password"},
        exclude_for_storing={"locale_id", "bookyear_id", "dossier_id"}
    )
    info = pipeline.run(source)
    print(info)


if __name__ == "__main__":
    main()
