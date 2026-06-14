import sys
from pathlib import Path
from realityscan_sdk.client import RealityScanClient

client = RealityScanClient(
            base_url="http://" + "RS_HOST" + "RS_PORT",
            client_id="test-client1",
            app_token="Test"
)

#connect
client._node.connectuser