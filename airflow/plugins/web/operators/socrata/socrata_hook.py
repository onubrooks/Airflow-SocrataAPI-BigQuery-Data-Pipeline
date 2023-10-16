from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests_oauthlib import OAuth2Session

class SocrataHook(HttpHook):

    """
    Interact with Socrata APIs.

    :param method: the API method to be called
    :param socrata_conn_id: The Socrata connection id. The name or identifier for
        establishing a connection to Socrata that has the base API url
        and optional authentication credentials. Default params and headers
        can also be specified in the Extra field in json format.
    """

    def __init__(
            self,
            method: str = "POST",
            socrata_conn_id: str =  "socrata_conn_id",
            **kwargs,
    ) -> None:
        super().__init__(method,**kwargs)
        self.socrata_conn_id = socrata_conn_id
        self.base_url: str = ""

    def get_conn(self, headers: Optional[Dict[Any, Any]] = None) -> OAuth2Session:

        """
        Returns OAuth2 session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        """
        session = OAuth2Session()
        conn = self.get_connection(self.socrata_conn_id)
        self.extras = conn.extra_dejson.copy()

        if conn.host and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host

        self.log.info(f"Base url is: {self.base_url}")

        #session.headers["Authorization"] = "OAuth {}".format(self.extras['access_token'])
        session.headers["X-App-Token"] = self.extras['app_token']

        if headers:
            session.headers.update(headers)

        self.log.info(f"final headers used: {session.headers}")
        return session

    def url_from_endpoint(self, endpoint: Optional[str]) -> str:
        """Overwrite parent `url_from_endpoint` method"""
        return endpoint