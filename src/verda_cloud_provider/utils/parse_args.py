import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Verda Cloud Provider for Kubernetes Cluster Autoscaler"
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to the configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "-p", "--port", type=int, default=8086, help="Port to listen on (default: 8086)"
    )
    return parser.parse_args()
