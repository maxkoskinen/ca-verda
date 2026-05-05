import datetime

from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NodeGroup(_message.Message):
    __slots__ = ("id", "minSize", "maxSize", "debug")
    ID_FIELD_NUMBER: _ClassVar[int]
    MINSIZE_FIELD_NUMBER: _ClassVar[int]
    MAXSIZE_FIELD_NUMBER: _ClassVar[int]
    DEBUG_FIELD_NUMBER: _ClassVar[int]
    id: str
    minSize: int
    maxSize: int
    debug: str
    def __init__(self, id: _Optional[str] = ..., minSize: _Optional[int] = ..., maxSize: _Optional[int] = ..., debug: _Optional[str] = ...) -> None: ...

class ExternalGrpcNode(_message.Message):
    __slots__ = ("providerID", "name", "labels", "annotations")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class AnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PROVIDERID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    providerID: str
    name: str
    labels: _containers.ScalarMap[str, str]
    annotations: _containers.ScalarMap[str, str]
    def __init__(self, providerID: _Optional[str] = ..., name: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ..., annotations: _Optional[_Mapping[str, str]] = ...) -> None: ...

class NodeGroupsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeGroupsResponse(_message.Message):
    __slots__ = ("nodeGroups",)
    NODEGROUPS_FIELD_NUMBER: _ClassVar[int]
    nodeGroups: _containers.RepeatedCompositeFieldContainer[NodeGroup]
    def __init__(self, nodeGroups: _Optional[_Iterable[_Union[NodeGroup, _Mapping]]] = ...) -> None: ...

class NodeGroupForNodeRequest(_message.Message):
    __slots__ = ("node",)
    NODE_FIELD_NUMBER: _ClassVar[int]
    node: ExternalGrpcNode
    def __init__(self, node: _Optional[_Union[ExternalGrpcNode, _Mapping]] = ...) -> None: ...

class NodeGroupForNodeResponse(_message.Message):
    __slots__ = ("nodeGroup",)
    NODEGROUP_FIELD_NUMBER: _ClassVar[int]
    nodeGroup: NodeGroup
    def __init__(self, nodeGroup: _Optional[_Union[NodeGroup, _Mapping]] = ...) -> None: ...

class PricingNodePriceRequest(_message.Message):
    __slots__ = ("node", "startTimestamp", "endTimestamp")
    NODE_FIELD_NUMBER: _ClassVar[int]
    STARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ENDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    node: ExternalGrpcNode
    startTimestamp: _timestamp_pb2.Timestamp
    endTimestamp: _timestamp_pb2.Timestamp
    def __init__(self, node: _Optional[_Union[ExternalGrpcNode, _Mapping]] = ..., startTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., endTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PricingNodePriceResponse(_message.Message):
    __slots__ = ("price",)
    PRICE_FIELD_NUMBER: _ClassVar[int]
    price: float
    def __init__(self, price: _Optional[float] = ...) -> None: ...

class PricingPodPriceRequest(_message.Message):
    __slots__ = ("pod_bytes", "startTimestamp", "endTimestamp")
    POD_BYTES_FIELD_NUMBER: _ClassVar[int]
    STARTTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ENDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    pod_bytes: bytes
    startTimestamp: _timestamp_pb2.Timestamp
    endTimestamp: _timestamp_pb2.Timestamp
    def __init__(self, pod_bytes: _Optional[bytes] = ..., startTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., endTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PricingPodPriceResponse(_message.Message):
    __slots__ = ("price",)
    PRICE_FIELD_NUMBER: _ClassVar[int]
    price: float
    def __init__(self, price: _Optional[float] = ...) -> None: ...

class GPULabelRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GPULabelResponse(_message.Message):
    __slots__ = ("label",)
    LABEL_FIELD_NUMBER: _ClassVar[int]
    label: str
    def __init__(self, label: _Optional[str] = ...) -> None: ...

class GetAvailableGPUTypesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetAvailableGPUTypesResponse(_message.Message):
    __slots__ = ("gpuTypes",)
    class GpuTypesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _any_pb2.Any
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
    GPUTYPES_FIELD_NUMBER: _ClassVar[int]
    gpuTypes: _containers.MessageMap[str, _any_pb2.Any]
    def __init__(self, gpuTypes: _Optional[_Mapping[str, _any_pb2.Any]] = ...) -> None: ...

class CleanupRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CleanupResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RefreshRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RefreshResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeGroupTargetSizeRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class NodeGroupTargetSizeResponse(_message.Message):
    __slots__ = ("targetSize",)
    TARGETSIZE_FIELD_NUMBER: _ClassVar[int]
    targetSize: int
    def __init__(self, targetSize: _Optional[int] = ...) -> None: ...

class NodeGroupIncreaseSizeRequest(_message.Message):
    __slots__ = ("delta", "id")
    DELTA_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    delta: int
    id: str
    def __init__(self, delta: _Optional[int] = ..., id: _Optional[str] = ...) -> None: ...

class NodeGroupIncreaseSizeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeGroupDeleteNodesRequest(_message.Message):
    __slots__ = ("nodes", "id")
    NODES_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[ExternalGrpcNode]
    id: str
    def __init__(self, nodes: _Optional[_Iterable[_Union[ExternalGrpcNode, _Mapping]]] = ..., id: _Optional[str] = ...) -> None: ...

class NodeGroupDeleteNodesResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeGroupDecreaseTargetSizeRequest(_message.Message):
    __slots__ = ("delta", "id")
    DELTA_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    delta: int
    id: str
    def __init__(self, delta: _Optional[int] = ..., id: _Optional[str] = ...) -> None: ...

class NodeGroupDecreaseTargetSizeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeGroupNodesRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class NodeGroupNodesResponse(_message.Message):
    __slots__ = ("instances",)
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    instances: _containers.RepeatedCompositeFieldContainer[Instance]
    def __init__(self, instances: _Optional[_Iterable[_Union[Instance, _Mapping]]] = ...) -> None: ...

class Instance(_message.Message):
    __slots__ = ("id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: InstanceStatus
    def __init__(self, id: _Optional[str] = ..., status: _Optional[_Union[InstanceStatus, _Mapping]] = ...) -> None: ...

class InstanceStatus(_message.Message):
    __slots__ = ("instanceState", "errorInfo")
    class InstanceState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        unspecified: _ClassVar[InstanceStatus.InstanceState]
        instanceRunning: _ClassVar[InstanceStatus.InstanceState]
        instanceCreating: _ClassVar[InstanceStatus.InstanceState]
        instanceDeleting: _ClassVar[InstanceStatus.InstanceState]
    unspecified: InstanceStatus.InstanceState
    instanceRunning: InstanceStatus.InstanceState
    instanceCreating: InstanceStatus.InstanceState
    instanceDeleting: InstanceStatus.InstanceState
    INSTANCESTATE_FIELD_NUMBER: _ClassVar[int]
    ERRORINFO_FIELD_NUMBER: _ClassVar[int]
    instanceState: InstanceStatus.InstanceState
    errorInfo: InstanceErrorInfo
    def __init__(self, instanceState: _Optional[_Union[InstanceStatus.InstanceState, str]] = ..., errorInfo: _Optional[_Union[InstanceErrorInfo, _Mapping]] = ...) -> None: ...

class InstanceErrorInfo(_message.Message):
    __slots__ = ("errorCode", "errorMessage", "instanceErrorClass")
    ERRORCODE_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    INSTANCEERRORCLASS_FIELD_NUMBER: _ClassVar[int]
    errorCode: str
    errorMessage: str
    instanceErrorClass: int
    def __init__(self, errorCode: _Optional[str] = ..., errorMessage: _Optional[str] = ..., instanceErrorClass: _Optional[int] = ...) -> None: ...

class NodeGroupTemplateNodeInfoRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class NodeGroupTemplateNodeInfoResponse(_message.Message):
    __slots__ = ("nodeBytes",)
    NODEBYTES_FIELD_NUMBER: _ClassVar[int]
    nodeBytes: bytes
    def __init__(self, nodeBytes: _Optional[bytes] = ...) -> None: ...

class NodeGroupAutoscalingOptions(_message.Message):
    __slots__ = ("scaleDownUtilizationThreshold", "scaleDownGpuUtilizationThreshold", "zeroOrMaxNodeScaling", "ignoreDaemonSetsUtilization", "scaleDownUnneededDuration", "scaleDownUnreadyDuration", "MaxNodeProvisionDuration")
    SCALEDOWNUTILIZATIONTHRESHOLD_FIELD_NUMBER: _ClassVar[int]
    SCALEDOWNGPUUTILIZATIONTHRESHOLD_FIELD_NUMBER: _ClassVar[int]
    ZEROORMAXNODESCALING_FIELD_NUMBER: _ClassVar[int]
    IGNOREDAEMONSETSUTILIZATION_FIELD_NUMBER: _ClassVar[int]
    SCALEDOWNUNNEEDEDDURATION_FIELD_NUMBER: _ClassVar[int]
    SCALEDOWNUNREADYDURATION_FIELD_NUMBER: _ClassVar[int]
    MAXNODEPROVISIONDURATION_FIELD_NUMBER: _ClassVar[int]
    scaleDownUtilizationThreshold: float
    scaleDownGpuUtilizationThreshold: float
    zeroOrMaxNodeScaling: bool
    ignoreDaemonSetsUtilization: bool
    scaleDownUnneededDuration: _duration_pb2.Duration
    scaleDownUnreadyDuration: _duration_pb2.Duration
    MaxNodeProvisionDuration: _duration_pb2.Duration
    def __init__(self, scaleDownUtilizationThreshold: _Optional[float] = ..., scaleDownGpuUtilizationThreshold: _Optional[float] = ..., zeroOrMaxNodeScaling: bool = ..., ignoreDaemonSetsUtilization: bool = ..., scaleDownUnneededDuration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., scaleDownUnreadyDuration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., MaxNodeProvisionDuration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class NodeGroupAutoscalingOptionsRequest(_message.Message):
    __slots__ = ("id", "defaults")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEFAULTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    defaults: NodeGroupAutoscalingOptions
    def __init__(self, id: _Optional[str] = ..., defaults: _Optional[_Union[NodeGroupAutoscalingOptions, _Mapping]] = ...) -> None: ...

class NodeGroupAutoscalingOptionsResponse(_message.Message):
    __slots__ = ("nodeGroupAutoscalingOptions",)
    NODEGROUPAUTOSCALINGOPTIONS_FIELD_NUMBER: _ClassVar[int]
    nodeGroupAutoscalingOptions: NodeGroupAutoscalingOptions
    def __init__(self, nodeGroupAutoscalingOptions: _Optional[_Union[NodeGroupAutoscalingOptions, _Mapping]] = ...) -> None: ...
