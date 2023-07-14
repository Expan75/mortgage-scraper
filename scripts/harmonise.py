import os
import json
import pathlib
import argparse
import pandas as pd
from tqdm import tqdm


project_dir = str(
    pathlib.Path(os.path.dirname(os.path.realpath(__file__))).parent.resolve()
)
input_dir = os.path.join(project_dir, "data")


def cli():
    parser = argparse.ArgumentParser(
        prog="mortgage scraper output harmoniser",
        description="script tool for harmonising csv outputs",
    )
    parser.add_argument("-o", "--output", required=True, type=str)
    parser.add_argument("-c", "--cleanup", action="store_true", default=False)
    parser.add_argument("-l", "--limit", default=None, type=int)
    parser.add_argument("-d", "--delimiter", default=",", type=str)
    args = parser.parse_args()
    return args


def harmonise(read_filepath: str, write_filepath: str, delimiter: str):
    column_map = {
        # common fields for all
        "url": "url",
        "ltv": "ltv",
        "asset_value": "asset_value",
        "loan_amount": "loan_amount",
        "period": "interest_term_months",
        "scraped_at": "scraped_time",
    }

    df = pd.read_csv(read_filepath)
    export_cols = [*set(column_map.values()), "json", "bank"]
    export_df = pd.DataFrame(columns=export_cols)

    # attach raw scrape body as json
    export_df.json = df.apply(lambda x: x.to_json(), axis=1)
    export_df.bank = read_filepath.split("/")[-1].split("_")[0]

    df = df.rename(columns=column_map)
    for col in set(df.columns) & set(export_df.columns):
        export_df[col] = df[col].copy()

    # apply misc. data cleaning and ordering
    column_order = list(
        set(
            [
                "bank",
                "offered_interest_rate",
                "ltv",
                "asset_value",
                "loan_amount",
                "interest_term_months",
                "scraped_time",
                "url",
                "json",
                *[c for c in sorted(export_df.columns)],
            ]
        )
    )
    # append ontooutput file
    export_df[column_order].to_csv(
        write_filepath, index=False, mode="a+", sep=delimiter
    )


if __name__ == "__main__":
    args = cli()

    output_path = pathlib.Path(args.output)
    parent_dir_path = output_path.parent

    *_, filename = output_path.parts
    assert ".csv" in filename, "output needs to be a csv filename"

    print(f"exporting to {args.output}")

    os.makedirs(parent_dir_path, exist_ok=True)

    for file in tqdm(os.listdir(input_dir)[: args.limit]):
        filepath = os.path.join(input_dir, file)
        harmonise(filepath, args.output, args.delimiter)

    # destructively removes old data artifacts harmonised into single csv by script
    if args.cleanup:
        print(f"cleaning up {input_dir}")
        for file in os.listdir(input_dir):
            filepath = os.path.join(input_dir, file)
            os.remove(filepath)
