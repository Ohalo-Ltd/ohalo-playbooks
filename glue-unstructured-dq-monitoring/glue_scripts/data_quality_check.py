import logging
import sys
import traceback

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job arguments
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "DATABASE_NAME", "TABLE_NAME", "S3_BUCKET"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def main():
    try:
        # Read data from Glue catalog
        logger.info(f"Reading data from {args['DATABASE_NAME']}.{args['TABLE_NAME']}")

        datasource = glueContext.create_dynamic_frame.from_catalog(
            database=args["DATABASE_NAME"], table_name=args["TABLE_NAME"]
        )

        if datasource.count() == 0:
            logger.warning("No data found in the table")
            return

        # Convert to DataFrame for easier processing
        df = datasource.toDF()

        logger.info(f"Processing {df.count()} records")

        # Show schema for debugging (first few levels)
        logger.info("DataFrame schema:")
        df.printSchema()

        # Show sample data to understand structure
        logger.info("Sample data (first 3 records):")
        df.select("fileName", "datasource", "labels").show(3, truncate=False)

        # Check if labels column exists and is not empty
        labels_sample = df.select("labels").filter(F.size(F.col("labels")) > 0).limit(5)
        logger.info("Sample labels structure:")
        labels_sample.show(truncate=False)

        # Handle the case where labels might be empty for some files
        df_with_valid_labels = df.filter(
            F.col("labels").isNotNull() & (F.size(F.col("labels")) > 0)
        )

        logger.info(f"Files with valid labels: {df_with_valid_labels.count()}")

        if df_with_valid_labels.count() == 0:
            logger.warning("No files with labels found")
            return

        # Data Quality Check: Find files with "CONFIDENTIAL" label but not "GATED"
        # First, let's explode the labels array to work with individual labels
        df_with_labels = df_with_valid_labels.select(
            "*", F.explode(F.col("labels")).alias("label_exploded")
        ).select(
            "*", F.col("label_exploded.name").alias("label_name")  # Updated field name
        )

        logger.info("Sample exploded labels:")
        df_with_labels.select("fileName", "label_name").show(10, truncate=False)

        # Group back by file to get all labels for each file
        # Updated to handle new datasource structure
        file_labels = df_with_labels.groupBy(
            "fileId",
            "fileName",
            "path",
            "size",
            "mimeType",
            "createdAt",
            "lastModifiedAt",
            "contentSha256",
        ).agg(
            F.collect_list("label_name").alias("all_labels"),
            F.first("datasource.id").alias("datasourceId"),
            F.first("datasource.name").alias("datasourceName"),
        )

        logger.info("Sample file labels:")
        file_labels.select("fileName", "datasourceName", "all_labels").show(
            5, truncate=False
        )

        # Find files with CONFIDENTIAL but without GATED
        confidential_files = file_labels.filter(
            F.array_contains(F.col("all_labels"), "CONFIDENTIAL")
            & ~F.array_contains(F.col("all_labels"), "GATED")
        )

        violation_count = confidential_files.count()

        if violation_count > 0:
            logger.warning(
                f"🚨 Found {violation_count} files with CONFIDENTIAL label but without GATED label"
            )

            # Show violation details in logs
            logger.warning("Violation details:")
            confidential_files.select(
                "fileName", "path", "datasourceName", "fileId", "all_labels"
            ).show(50, truncate=False)

            # Log each violation individually for easy reading
            violations_list = confidential_files.collect()
            for i, violation in enumerate(violations_list, 1):
                logger.warning(f"VIOLATION {i}:")
                logger.warning(f"  File: {violation['fileName']}")
                logger.warning(f"  Path: {violation['path']}")
                logger.warning(f"  Datasource: {violation['datasourceName']}")
                logger.warning(f"  Labels: {violation['all_labels']}")
                logger.warning(f"  Size: {violation['size']} bytes")
                logger.warning("---")

            # Create detailed violation report for S3 (optional)
            try:
                violation_details = confidential_files.select(
                    F.col("fileName"),
                    F.col("path"),
                    F.col("datasourceName"),
                    F.col("datasourceId"),
                    F.col("fileId"),
                    F.col("size"),
                    F.col("createdAt"),
                    F.col("all_labels"),
                    F.lit("CONFIDENTIAL file without GATED label").alias(
                        "violation_type"
                    ),
                    F.current_timestamp().alias("check_timestamp"),
                )

                # Write violations to S3 for review
                s3_bucket = args["S3_BUCKET"]
                violations_output_path = f"s3://{s3_bucket}/violations/"
                violation_details.coalesce(1).write.mode("overwrite").json(
                    violations_output_path
                )
                logger.warning(f"📁 Violations written to: {violations_output_path}")
            except Exception as e:
                logger.warning(f"Could not write violations to S3: {str(e)}")
                logger.warning("Continuing without writing violations file...")

        else:
            logger.info(
                "✅ No data quality violations found - all CONFIDENTIAL files have GATED label"
            )

        # Additional statistics
        total_files = df.count()
        files_with_labels = df_with_valid_labels.count()
        files_without_labels = total_files - files_with_labels
        confidential_files_total = file_labels.filter(
            F.array_contains(F.col("all_labels"), "CONFIDENTIAL")
        ).count()
        gated_files_total = file_labels.filter(
            F.array_contains(F.col("all_labels"), "GATED")
        ).count()

        logger.info("📊 Data Quality Check Summary:")
        logger.info(f"   - Total files: {total_files}")
        logger.info(f"   - Files with labels: {files_with_labels}")
        logger.info(f"   - Files without labels: {files_without_labels}")
        logger.info(f"   - Files with CONFIDENTIAL label: {confidential_files_total}")
        logger.info(f"   - Files with GATED label: {gated_files_total}")
        logger.info(f"   - Violations found: {violation_count}")

        # Show breakdown of all label types for reference
        logger.info("📋 Label distribution:")
        all_labels_flat = (
            df_with_labels.groupBy("label_name").count().orderBy(F.desc("count"))
        )
        all_labels_flat.show(20, truncate=False)

        # Write summary stats to S3 (optional)
        try:
            summary_data = [
                {"metric": "total_files", "value": total_files},
                {"metric": "files_with_labels", "value": files_with_labels},
                {"metric": "files_without_labels", "value": files_without_labels},
                {"metric": "confidential_files", "value": confidential_files_total},
                {"metric": "gated_files", "value": gated_files_total},
                {"metric": "violations_found", "value": violation_count},
            ]

            summary_df = spark.createDataFrame(summary_data)
            # Use the S3 bucket passed from Terraform
            s3_bucket = args["S3_BUCKET"]
            summary_output_path = f"s3://{s3_bucket}/summary/"
            summary_df.coalesce(1).write.mode("overwrite").json(summary_output_path)
            logger.info(f"📈 Summary written to: {summary_output_path}")
        except Exception as e:
            logger.warning(f"Could not write summary to S3: {str(e)}")
            logger.warning("Continuing without writing summary file...")

    except Exception as e:
        logger.error(f"❌ Error in data quality check: {str(e)}")
        logger.error(traceback.format_exc())
        raise e


if __name__ == "__main__":
    main()

job.commit()
