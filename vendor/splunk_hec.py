class splunk_hec:
    def __init__(
        self,
        token,
        hec_server,
        hec_port="8088",
        use_hec_tls=True,
        hec_tls_verify=True,
        logger=None,
        input_type="json",
    ):
        from splunk_http_event_collector import http_event_collector

        self.params = {}
        self.client = http_event_collector(
            token=token,
            http_event_server=hec_server,
            http_event_port=hec_port,
            http_event_server_ssl=str(use_hec_tls).lower() in ("1", "true", "yes"),
            input_type=input_type,
        )

    def set_request_params(self, params):
        self.params = params or {}

    def send_event(self, payload):
        event = {"event": payload, **self.params}
        return self.client.sendEvent(event)
