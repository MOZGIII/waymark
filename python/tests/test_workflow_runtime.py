import asyncio
from typing import Annotated

from proto import messages_pb2 as pb2
from rappel import registry as action_registry
from rappel.actions import action, serialize_error_payload, serialize_result_payload
from rappel.dependencies import Depend
from rappel.workflow_runtime import NodeExecutionResult, execute_node


@action
async def multiply(value: int) -> int:
    return value * 2


@action
async def exception_handler() -> str:
    return "handled"


async def provide_suffix() -> str:
    return "suffix"


@action
async def with_dependency(value: int, suffix: Annotated[str, Depend(provide_suffix)]) -> str:
    return f"{value}-{suffix}"


def _build_resolved_dispatch() -> pb2.WorkflowNodeDispatch:
    if action_registry.get("multiply") is None:
        action_registry.register("multiply", multiply)
    node = pb2.WorkflowDagNode(
        id="node_multiply",
        action="multiply",
        module=__name__,
    )
    node.produces.append("value")
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    resolved = pb2.WorkflowArguments()
    entry = resolved.arguments.add()
    entry.key = "value"
    entry.value.primitive.int_value = 10
    dispatch.resolved_kwargs.CopyFrom(resolved)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    return dispatch


def _build_exception_dispatch(include_error: bool) -> pb2.WorkflowNodeDispatch:
    if action_registry.get("exception_handler") is None:
        action_registry.register("exception_handler", exception_handler)
    node = pb2.WorkflowDagNode(
        id="node_handler",
        action="exception_handler",
        module=__name__,
    )
    node.produces.append("value")
    node.guard = "__workflow_exceptions.get('node_source') is not None"
    edge = node.exception_edges.add()
    edge.source_node_id = "node_source"
    edge.exception_type = "RuntimeError"
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    if include_error:
        payload = serialize_error_payload("source", RuntimeError("boom"))
    else:
        payload = serialize_result_payload("noop")
    entry = dispatch.context.add()
    entry.variable = ""
    entry.workflow_node_id = "node_source"
    entry.payload.CopyFrom(payload)
    return dispatch


def _build_dependency_dispatch() -> pb2.WorkflowNodeDispatch:
    if action_registry.get("with_dependency") is None:
        action_registry.register("with_dependency", with_dependency)
    node = pb2.WorkflowDagNode(
        id="node_dependency",
        action="with_dependency",
        module=__name__,
    )
    node.produces.append("value")
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    resolved = pb2.WorkflowArguments()
    entry = resolved.arguments.add()
    entry.key = "value"
    entry.value.primitive.int_value = 3
    dispatch.resolved_kwargs.CopyFrom(resolved)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    return dispatch


def test_execute_node_uses_resolved_kwargs() -> None:
    payload = _build_resolved_dispatch()
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == 20


def test_execute_node_handles_exception_when_edge_matches() -> None:
    payload = _build_exception_dispatch(include_error=True)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == "handled"


def test_execute_node_resolves_dependencies() -> None:
    payload = _build_dependency_dispatch()
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == "3-suffix"
