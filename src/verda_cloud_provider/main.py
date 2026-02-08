import logging
import sys
from concurrent import futures

import grpc
from dotenv import load_dotenv
from grpc_reflection.v1alpha import reflection

from verda_cloud_provider.gen.externalgrpc import (
    externalgrpc_pb2,
    externalgrpc_pb2_grpc,
)
from verda_cloud_provider.provider import VerdaCloudProvider
from verda_cloud_provider.utils.logging import setup_logging
from verda_cloud_provider.utils.parse_args import parse_args

logger = logging.getLogger(__name__)

load_dotenv()


def serve(config_path: str, port: int):
    # Initialize server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    try:
        provider = VerdaCloudProvider(config_path=config_path)
    except Exception as e:
        logging.fatal(f"Failed to initialize provider: {e}")
        sys.exit(1)

    externalgrpc_pb2_grpc.add_CloudProviderServicer_to_server(provider, server)

    SERVICE_NAMES = (
        externalgrpc_pb2.DESCRIPTOR.services_by_name["CloudProvider"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    bind_address = f"[::]:{port}"
    server.add_insecure_port(bind_address)
    logging.info(f"Starting Verda Cloud Provider on port {port}...")

    server.start()
    server.wait_for_termination()


def main():
    args = parse_args()

    setup_logging(args.log_level)

    serve(config_path=args.config, port=args.port)


if __name__ == "__main__":
    main()
