import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class S3BronzeUploader:
    def __init__(self, bucket_name: str, aws_access_key: str = None,
                 aws_secret_key: str = None, region: str = 'ap-south-1'):
        self.bucket_name = bucket_name
        
        # Strip whitespace to prevent subtle credential bugs
        access_key = aws_access_key.strip() if aws_access_key else None
        secret_key = aws_secret_key.strip() if aws_secret_key else None
        
        if access_key and secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            logger.info("Using explicit AWS credentials")
        else:
            # FALLBACK: Use IAM role, env vars, or ~/.aws/credentials
            self.s3_client = boto3.client('s3', region_name=region)
            logger.info("Using default AWS credential chain")

    def upload_csv(self, df: pd.DataFrame, table_name: str,
                   partition_date: str, filename: str = None) -> str:
        if df.empty:
            logger.warning(f"Empty DataFrame for {table_name}, skipping upload")
            return None

        if filename is None:
            timestamp = datetime.now().strftime('%H%M%S')
            filename = f"{table_name}_{timestamp}.csv"

        partition_path = f"Bronze/{table_name}/date={partition_date}/{filename}"

        # Use BytesIO for proper binary handling
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Reset buffer position

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=partition_path,
                Body=csv_buffer.getvalue()
            )
            s3_uri = f"s3://{self.bucket_name}/{partition_path}"
            logger.info(f"Uploaded {len(df)} rows to {s3_uri}")
            return s3_uri
        except Exception as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise
    def read_csv(self, s3_prefix: str, **pandas_kwargs) -> pd.DataFrame:
        """
        Read all CSV files under an S3 prefix and return a single DataFrame.

        Example:
            read_csv("Bronze/products")
            read_csv("Bronze/customers")
        """

        if not s3_prefix.endswith("/"):
            s3_prefix += "/"

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix
            )

            if "Contents" not in response:
                raise FileNotFoundError(
                    f"No files found under s3://{self.bucket_name}/{s3_prefix}"
                )

            csv_files = [
                obj["Key"]
                for obj in response["Contents"]
                if obj["Key"].endswith(".csv")
            ]

            if not csv_files:
                raise FileNotFoundError(
                    f"No CSV files found under s3://{self.bucket_name}/{s3_prefix}"
                )

            dataframes = []

            for key in sorted(csv_files):
                logger.info(f"Reading {key}")

                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=key
                )

                df = pd.read_csv(
                    BytesIO(response["Body"].read()),
                    **pandas_kwargs
                )

                dataframes.append(df)

            final_df = pd.concat(dataframes, ignore_index=True)

            logger.info(
                f"Loaded {len(final_df)} rows from {len(csv_files)} CSV files."
            )

            return final_df

        except Exception as e:
            logger.error(f"Failed to read CSVs from {s3_prefix}: {e}")
            raise