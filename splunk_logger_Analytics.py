# Databricks notebook source
import requests
import urllib3

# To Disable InsecureRequestWarning. Splunk server uses self signed ssl certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------


class SplunkLogger:
    """
    Helper class for implementing Splunk logging via HTTP
    """

    def __init__(
        self,
        index,
        meta_data,
        token,
        splunk_host="splunk.prod.internal.forwarders.tools.cso-hp.com",
        port=8088,
    ):
        """
        Parameters
        ----------
        index : str
            Splunk index
        meta_data : dict
            Splunk meta data such as source, sourcetype and host
        token : str
            Splunk HEC token
        splunk_host : str, optional
            Splunk host name (default is splunk.prod.internal.forwarders.tools.cso-hp.com)
        port : int, optional
            Splunk port  (default is 8088)
        """

        self.url = "https://{}:{}/services/collector/event/1.0".format(
            splunk_host, port
        )
        self.payload = {}
        self.index = index
        self.token = token
        self.meta_data = meta_data
        self._build_payload()

    def _get_auth_header(self):
        """Constructs Authorization Header with HEC token

        Returns
        -------
          string
              Authorization Header for splunk request
        """

        return {"Authorization": "Splunk " + self.token}

    def _build_payload(self):
        """Constructs http payload with meta data

        Returns
        -------
          dict
              Payload with splunk index and meta data
        """

        self.payload.update({"index": self.index})

        if "source" in self.meta_data:
            self.payload.update({"source": self.meta_data["source"]})
        if "sourcetype" in self.meta_data:
            self.payload.update({"sourcetype": self.meta_data["sourcetype"]})
        if "host" in self.meta_data:
            self.payload.update({"host": self.meta_data["host"]})

    def splunk_url(self):
        """
        Returns
        -------
          str
              Full splunk URL to log events
        """

        return self.url

    def log_event(self, log_data, retry=0):
        """Logs events to splunk server

        Parameters
        ----------
        log_data : dict
            Event data to be logged to splunk
        """

        try:
            if retry > 0:
                dic_log_data = json.loads(log_data)
                dic_log_data["splunk_exception_count"] = retry
                log_data = json.dumps(dic_log_data)
                print("{retry} times Exception occurred while logging to splunk")

            self.payload.update({"event": log_data})
            resp = requests.post(
                self.url,
                headers=self._get_auth_header(),
                json=self.payload,
                verify=False,
            )
            resp.raise_for_status()
        except Exception as e:
            if retry > 3:
                print("Exception Occurred", e)
                raise
            self.log_event(log_data, retry + 1)
