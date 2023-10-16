from typing import Any, Dict, Optional, Sequence, Union

import tempfile
import urllib
import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from web.operators.socrata.socrata_hook import SocrataHook

class SocrataToGCSOperator(BaseOperator):
    """
    Export files to Google Cloud Storage from Socrata

    :num_pages: The number of pages to download
    :param items_per_page: The number of items per page
    :param endpoint: The endpoint to be called
    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
        If a wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param socrata_conn_id: The Socrata connection id. The name or identifier for
        establishing a connection to Socrata
    :param method: The HTTP method to use, default = "GET"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param mime_type: The mime-type string
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "data",
        "destination_path",
        "destination_bucket",
        "impersonation_chain",
    )
    template_fields_renderers = {"headers": "json", "data": "py"}
    ui_color = "#f4a460"

    def  __init__(
		self,
        *,
        items_per_page: int,
        num_pages: int,
        endpoint: str,
		destination_bucket: str,
		destination_path: Optional[str] = None,
		gcp_conn_id: str = "google_cloud_default",
		socrata_conn_id: str = "socrata_conn_id",
		method: str = "GET",
		data: Any = None,
		extra_options: Optional[Dict[str, Any]] = None,
		gzip: bool = False,
		mime_type: str = "csv",
		delegate_to: Optional[str] = None,
		impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwagrs,
	) -> None:
        super().__init__(**kwagrs)
        self.items_per_page = items_per_page
        self.num_pages = num_pages
        self.endpoint = endpoint
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.socrata_conn_id = socrata_conn_id
        self.method = method
        self.data = data
        self.extra_options = extra_options
        self.gzip = gzip
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain


    def execute(self, context: Any):
         """Run the Operator Basically to do all the complex works"""
         gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

         socrata_hook = SocrataHook(self.method, self.socrata_conn_id)
         self._run_task(gcs_hook, socrata_hook)


    def _run_task(self, gcs_hook: GCSHook, socrata_hook: SocrataHook) -> None:
        """Helper function to copy single files from socrata to GCS """

        headers= {"Content-Type": "application/json"}
        
        socrata_df = self.get_socrata_data_as_dataframe(socrata_hook, headers)
        self.log.info(socrata_df.head(10))
        socrata_df['file_date'] = pd.to_datetime(socrata_df['file_date'])
            
        with tempfile.TemporaryDirectory() as tmpdirname:    
            # parquet_file = f'{self.destination_path}.csv'
            # socrata_df.to_parquet(f'{tmpdirname}/{parquet_file}', engine='pyarrow')
            csv_file = f'{self.destination_path}.csv'
            socrata_df.to_csv(f'{tmpdirname}/{csv_file}', index=False)
            
            gcs_hook.upload(
                    bucket_name=self.destination_bucket,
                    object_name=csv_file,
                    filename=f'{tmpdirname}/{csv_file}',
                    mime_type="text/csv",
                    gzip=False,
            )
            self.log.info(f"Loaded CSV: {csv_file}")

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
    
    def get_socrata_data_as_dataframe(self, socrata_hook: SocrataHook, headers: Dict[str, str]) -> pd.DataFrame:
        """Helper function to get data from Socrata and return as a dataframe"""
        main_df = pd.DataFrame()

        for page in range(self.num_pages):
            # calculate pagination offset and limit parameters
            offset = page * self.items_per_page
            PARAMS_DICT = {
                '$limit': self.items_per_page,
                '$offset': offset,
            }
            PARAMS = urllib.parse.urlencode(PARAMS_DICT, quote_via=urllib.parse.quote)
            endpoint = f'{self.endpoint}{PARAMS}'
            self.log.info(
                "Executing export of file from %s to gs://%s/%s",
                endpoint,
                self.destination_bucket,
                self.destination_path,
            )
            response = socrata_hook.run(
                endpoint, headers=headers
            )

            if response.status_code not in range(200,299):
                self.log("Error from request response")
            else:
                self.log.info("Request was successful with %s", response.status_code)

                data = response.json()
                temp_df = pd.DataFrame(data)
                main_df = pd.concat([main_df,temp_df],ignore_index=True)
                self.log.info(f"Dataframe has {main_df.shape[0]} columns")
        return main_df


