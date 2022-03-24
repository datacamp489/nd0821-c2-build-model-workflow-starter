#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    # Download input artifact
    logger.info(f"Downloading {args.input_artifact} from Weights & Biases")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info(f"Reading file into dataframe")
    df = pd.read_csv(artifact_local_path)
    # Price cutoff
    logger.info(f"Removing data with prices between {args.min_price} and {args.max_price}")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    logger.info(f"Converting last_review column to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])
    # Upload
    df.to_csv("clean_sample.csv", index=False)
    logger.info(f"Uploading {args.output_artifact} from Weights & Biases")
    artifact = wandb.Artifact(args.output_artifact, type=args.output_type, description=args.output_description)
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact (e.g. 'sample.csv:latest')",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact (e.g. clean_sample.csv)",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price (used as a cutoff value)",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price (used as a cutoff value)",
        required=True
    )


    args = parser.parse_args()

    go(args)
