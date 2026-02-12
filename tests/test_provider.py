# tests/test_provider_e2e.py
import time

import grpc
import pytest

from verda_cloud_provider.gen.externalgrpc import (
    externalgrpc_pb2,
    externalgrpc_pb2_grpc,
)


@pytest.fixture(scope="session")
def grpc_stub():
    # assumes server already running on localhost:8086
    channel = grpc.insecure_channel("localhost:8086")
    stub = externalgrpc_pb2_grpc.CloudProviderStub(channel)

    # wait briefly for connectivity
    for _ in range(10):
        try:
            stub.NodeGroups(externalgrpc_pb2.NodeGroupsRequest())
            break
        except grpc.RpcError:
            time.sleep(0.5)
    else:
        pytest.skip("Could not connect to verda-cloud-provider on localhost:8086")

    return stub


def test_nodegroups_and_target_size(grpc_stub):
    resp = grpc_stub.NodeGroups(externalgrpc_pb2.NodeGroupsRequest())
    assert len(resp.nodeGroups) > 0

    for group in resp.nodeGroups:
        size_resp = grpc_stub.NodeGroupTargetSize(
            externalgrpc_pb2.NodeGroupTargetSizeRequest(id=group.id)
        )
        print(f"{group.id} target size: {size_resp.targetSize}")
        assert size_resp.targetSize >= 0


def test_nodegroup_nodes(grpc_stub):
    groups = grpc_stub.NodeGroups(externalgrpc_pb2.NodeGroupsRequest())
    for group in groups.nodeGroups:
        nodes_resp = grpc_stub.NodeGroupNodes(
            externalgrpc_pb2.NodeGroupNodesRequest(id=group.id)
        )
        print(f"{group.id} has {len(nodes_resp.instances)} instances")
        # Just sanity check types
        for inst in nodes_resp.instances:
            assert inst.id.startswith("verda://") or inst.id == ""


def test_gpulpabel_and_refresh_and_cleanup(grpc_stub):
    gpu_label_resp = grpc_stub.GPULabel(externalgrpc_pb2.GPULabelRequest())
    assert gpu_label_resp.label == "verda.com/gpu"

    # Refresh should succeed without exception
    grpc_stub.Refresh(externalgrpc_pb2.RefreshRequest())

    # Cleanup is a no-op but should succeed
    grpc_stub.Cleanup(externalgrpc_pb2.CleanupRequest())


@pytest.mark.verda_live
def test_increase_and_delete_cycle(grpc_stub):
    group_id = "worker-cpu-1"  # adjust to match your config

    # Get initial size
    size_before = grpc_stub.NodeGroupTargetSize(
        externalgrpc_pb2.NodeGroupTargetSizeRequest(id=group_id)
    ).targetSize

    # Increase by 1
    grpc_stub.NodeGroupIncreaseSize(
        externalgrpc_pb2.NodeGroupIncreaseSizeRequest(id=group_id, delta=1)
    )

    # Give provider some time to call Verda and update state
    time.sleep(10)

    size_after = grpc_stub.NodeGroupTargetSize(
        externalgrpc_pb2.NodeGroupTargetSizeRequest(id=group_id)
    ).targetSize
    assert size_after >= size_before + 1

    # List nodes and delete one (best-effort)
    nodes_resp = grpc_stub.NodeGroupNodes(
        externalgrpc_pb2.NodeGroupNodesRequest(id=group_id)
    )

    if nodes_resp.instances:
        inst = nodes_resp.instances[0]

        node = externalgrpc_pb2.ExternalGrpcNode(
            # providerID is what your provider implementation uses first to resolve instance id
            providerID=inst.id,
            # optional; used only as a fallback lookup path in your provider
            name="",
        )

        grpc_stub.NodeGroupDeleteNodes(
            externalgrpc_pb2.NodeGroupDeleteNodesRequest(id=group_id, nodes=[node])
        )
