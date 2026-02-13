import logging
import os
import sys
from concurrent import futures

import grpc
from dotenv import load_dotenv
from grpc_reflection.v1alpha import reflection

from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2 import (
    DESCRIPTOR,
)
from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2_grpc import (
    add_CloudProviderServicer_to_server,
)
from verda_cloud_provider.provider import VerdaCloudProvider
from verda_cloud_provider.settings import AppConfig
from verda_cloud_provider.utils.logging import setup_logging
from verda_cloud_provider.utils.parse_args import parse_args

logger = logging.getLogger(__name__)

load_dotenv()


def serve(config_path: str, port: int):

    cfg = AppConfig.load(config_path)

    # Initialize server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    try:
        provider = VerdaCloudProvider(app_config=cfg)
    except Exception as e:
        logging.fatal(f"Failed to initialize provider: {e}")
        sys.exit(1)

    add_CloudProviderServicer_to_server(provider, server)

    SERVICE_NAMES = (
        DESCRIPTOR.services_by_name["CloudProvider"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    bind_address = f"[::]:{port}"

    tls_cert = os.environ.get("TLS_CERT_FILE")
    tls_key = os.environ.get("TLS_KEY_FILE")
    tls_ca = os.environ.get("TLS_CA_FILE")

    if tls_cert and tls_key and tls_ca:
        logging.info("mTLS enabled: loading server credentials")
        try:
            with open(tls_key, "rb") as f:
                private_key = f.read()
            with open(tls_cert, "rb") as f:
                certificate_chain = f.read()
            with open(tls_ca, "rb") as f:
                ca_cert = f.read()

            creds = grpc.ssl_server_credentials(
                [(private_key, certificate_chain)],
                root_certificates=ca_cert,
                require_client_auth=True,
            )
            server.add_secure_port(bind_address, creds)
            logging.info(f"Starting Verda Cloud Provider (mTLS) on {bind_address}")
        except Exception as e:
            logging.fatal(f"Failed to load TLS credentials: {e}")
            sys.exit(1)
    else:
        logging.warning("mTLS not configured: running in insecure mode")
        server.add_insecure_port(bind_address)
        logging.info(f"Starting Verda Cloud Provider (insecure) on {bind_address}")

    server.start()
    server.wait_for_termination()


def main():
    args = parse_args()

    setup_logging(args.log_level)

    serve(config_path=args.config, port=args.port)


if __name__ == "__main__":
    main()
