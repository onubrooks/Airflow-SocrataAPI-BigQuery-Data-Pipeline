from typing import Any, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import tempfile
import requests
import pandas as pd



class WebToGCSOperator(BaseOperator):
    """
      Move data from webserver link to a gcs bucket
    """

    template_fields: Sequence[str] = (
        "base_endpoint",
        "service",
        "destination_bucket",
    )

    def __init__(
            self,
            *,
            destination_bucket:  Optional[str] = None,
            service: str = None,
            years: Sequence[str] = None,
            months: Sequence[str] = None,
            base_endpoint: str = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/',
            gcp_conn_id: str = "google_cloud_default",
            gzip: bool = False,
            mime_type: str = "text/csv",
            delegete_to: Optional[str] = None,
            impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.destination_bucket = self._format_bucket_name(destination_bucket)
        self.service = service
        self.years = years
        self.months = months
        self.base_endpoint = base_endpoint
        self.gcp_conn_id = gcp_conn_id
        self.gzip = gzip
        self.mime_type = mime_type
        self.delegete_to = delegete_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Any):
        gcs_hook = GCSHook(
             gcp_conn_id = self.gcp_conn_id,
             delegate_to = self.delegete_to,
             impersonation_chain = self.impersonation_chain
        )
        self._web_to_gcs(gcs_hook, self.service)
                    

    def _web_to_gcs(self, gcs_hook: GCSHook, service) -> None:

        """function to download and copy file to gcs bucket """

        file_name = f"{service}_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv.gz"
        
        endpoint = f"{self.base_endpoint}{service}/{service}_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv.gz"
        
        destination_path = f"{service}_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv"

        self.log.info("Execute downloading of file from %s to gs://%s//%s",
                    endpoint,
                    self.destination_bucket,
                    destination_path
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
                r = requests.get(endpoint)


                open(f'{tmpdirname}/{destination_path}', 'wb').write(r.content)
                self.log.info(f"File written to temp directory: {tmpdirname}/{destination_path}")

                # read it back into a parquet file
                df = pd.read_csv(f'{tmpdirname}/{destination_path}', encoding='utf-8')
                print(df.head())
                file_name=destination_path
                file_name = file_name.replace('.csv.gz', '.csv')

                df.to_csv(f'{tmpdirname}/{file_name}', index=False) #engine='pyarrow')
                self.log.info(f"Parquet: {file_name}")
                local_file_name = f'{tmpdirname}/{file_name}'

                # upload it to gcs using GCS hooks
                gcs_hook.upload(
                     bucket_name=self.destination_bucket,
                     object_name=f"{self.service}/{file_name}",
                     filename=local_file_name,
                     mime_type=self.mime_type,
                     gzip=False,
                )

                self.log.info("Loaded file from %s to gs://%s//%s",
                    endpoint,
                    self.destination_bucket,
                    f"{self.service}/{file_name}"
                )
        
        """r = requests.get(endpoint)

        # Write the data to a temporary file
        temp_file = 'temp.csv.gz'
        with open(temp_file, 'wb') as f:
            f.write(r.content)

        # Read the data from the temporary file using Pandas
        df = pd.read_csv(temp_file, encoding='utf-8')
        self.log.info(df.head(10))
        file_name=destination_path
        file_name = file_name.replace('.csv.gz', '.csv')

        # yellow_tripdata_2021-01.csv
        df.to_csv(f'{file_name}') #engine='pyarrow')
        self.log.info(f"Parquet: {file_name}")
        local_file_name = f'{file_name}'

        # upload it to gcs using GCS hooks
        gcs_hook.upload(
                bucket_name=self.destination_bucket,
                object_name=f"{service}/{file_name}",
                filename=temp_file,
                mime_type=self.mime_type,
                gzip=False,
        )

        self.log.info("Loaded file from %s to gs://%s//%s",
            endpoint,
            self.destination_bucket,
            f"{service}/{file_name}"
        )

        # Delete the temporary file
        import os
        os.remove(temp_file)
        os.remove(local_file_name)"""

    @staticmethod
    def _format_bucket_name(name: str) -> str:
         bucket =  name if not name.startswith("gs://") else name[5:]
         return bucket.strip("/")