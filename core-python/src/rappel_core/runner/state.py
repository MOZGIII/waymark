"""Execution-time DAG state with unrolled nodes and symbolic values."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from proto import ast_pb2 as ir

from ..dag import (
    DAG,
    ActionCallNode,
    AggregatorNode,
    AssignmentNode,
    DAGNode,
    EdgeType,
    FnCallNode,
    JoinNode,
    ReturnNode,
    assert_never,
)


class RunnerStateError(Exception):
    """Raised when the runner state cannot be updated safely."""


@dataclass(frozen=True)
class ActionCallSpec:
    action_name: str
    module_name: Optional[str]
    kwargs: dict[str, "ValueExpr"]


@dataclass(frozen=True)
class LiteralValue:
    value: Any


@dataclass(frozen=True)
class VariableValue:
    name: str


@dataclass(frozen=True)
class ActionResultValue:
    node_id: str
    action_name: str
    iteration_index: Optional[int] = None
    result_index: Optional[int] = None

    def label(self) -> str:
        label = self.action_name
        if self.iteration_index is not None:
            label = f"{label}[{self.iteration_index}]"
        if self.result_index is not None:
            label = f"{label}[{self.result_index}]"
        return label


@dataclass(frozen=True)
class BinaryOpValue:
    left: "ValueExpr"
    op: ir.BinaryOperator
    right: "ValueExpr"


@dataclass(frozen=True)
class UnaryOpValue:
    op: ir.UnaryOperator
    operand: "ValueExpr"


@dataclass(frozen=True)
class ListValue:
    elements: tuple["ValueExpr", ...]


@dataclass(frozen=True)
class DictEntryValue:
    key: "ValueExpr"
    value: "ValueExpr"


@dataclass(frozen=True)
class DictValue:
    entries: tuple[DictEntryValue, ...]


@dataclass(frozen=True)
class IndexValue:
    object: "ValueExpr"
    index: "ValueExpr"


@dataclass(frozen=True)
class DotValue:
    object: "ValueExpr"
    attribute: str


@dataclass(frozen=True)
class FunctionCallValue:
    name: str
    args: tuple["ValueExpr", ...]
    kwargs: dict[str, "ValueExpr"]
    global_function: Optional[ir.GlobalFunction] = None


@dataclass(frozen=True)
class SpreadValue:
    collection: "ValueExpr"
    loop_var: str
    action: ActionCallSpec


ValueExpr = (
    LiteralValue
    | VariableValue
    | ActionResultValue
    | BinaryOpValue
    | UnaryOpValue
    | ListValue
    | DictValue
    | IndexValue
    | DotValue
    | FunctionCallValue
    | SpreadValue
)


class NodeStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ExecutionNode:
    node_id: str
    node_type: str
    label: str
    status: NodeStatus
    template_id: Optional[str] = None
    iteration_index: Optional[int] = None
    parent_id: Optional[str] = None
    targets: list[str] = field(default_factory=list)
    action: Optional[ActionCallSpec] = None
    value_expr: Optional[ValueExpr] = None


@dataclass
class ExecutionEdge:
    source: str
    target: str
    edge_type: EdgeType = EdgeType.STATE_MACHINE


@dataclass
class VariableUpdate:
    node_id: str
    targets: list[str]
    value: ValueExpr


class RunnerState:
    """Track queued/executed DAG nodes with an unrolled, symbolic state.

    This state only records nodes that are actually queued or executed, so
    control-flow constructs like loops become explicit iteration nodes. Values
    are stored as symbolic expressions so action results can be replayed without
    needing the concrete payloads immediately.
    """

    def __init__(self, dag: Optional[DAG] = None, *, link_queued_nodes: bool = True) -> None:
        self._dag = dag
        self.nodes: dict[str, ExecutionNode] = {}
        self.edges: list[ExecutionEdge] = []
        self.variables: dict[str, ValueExpr] = {}
        self.variable_sources: dict[str, str] = {}
        self.ready_queue: list[str] = []
        self.timeline: list[str] = []
        self.variable_updates: list[VariableUpdate] = []
        self._counters: dict[str, int] = {}
        self._link_queued_nodes = link_queued_nodes
        self._edge_keys: set[tuple[str, str, EdgeType]] = set()

    def queue_template_node(
        self,
        template_id: str,
        *,
        iteration_index: Optional[int] = None,
        instance_suffix: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> ExecutionNode:
        if self._dag is None:
            raise RunnerStateError("runner state has no DAG template")
        template = self._dag.nodes.get(template_id)
        if template is None:
            raise RunnerStateError(f"template node not found: {template_id}")

        node_id = self._build_execution_id(template_id, iteration_index, instance_suffix)
        node = ExecutionNode(
            node_id=node_id,
            node_type=template.node_type,
            label=template.label,
            status=NodeStatus.QUEUED,
            template_id=template_id,
            iteration_index=iteration_index,
            parent_id=parent_id,
            targets=self._node_targets(template),
        )
        if isinstance(template, ActionCallNode):
            node.action = self._action_spec_from_node(template)

        self._register_node(node)
        self._apply_template_node(node, template)
        return node

    def queue_node(
        self,
        node_type: str,
        label: str,
        *,
        node_id: Optional[str] = None,
        template_id: Optional[str] = None,
        targets: Optional[Sequence[str]] = None,
        iteration_index: Optional[int] = None,
        instance_suffix: Optional[str] = None,
        parent_id: Optional[str] = None,
        action: Optional[ActionCallSpec] = None,
        value_expr: Optional[ValueExpr] = None,
    ) -> ExecutionNode:
        base_id = node_id or template_id or self._next_id(node_type)
        node_id = self._build_execution_id(base_id, iteration_index, instance_suffix)
        node = ExecutionNode(
            node_id=node_id,
            node_type=node_type,
            label=label,
            status=NodeStatus.QUEUED,
            template_id=template_id,
            iteration_index=iteration_index,
            parent_id=parent_id,
            targets=list(targets or []),
            action=action,
            value_expr=value_expr,
        )
        self._register_node(node)
        return node

    def queue_action(
        self,
        action_name: str,
        *,
        targets: Optional[Sequence[str]] = None,
        kwargs: Optional[Mapping[str, ValueExpr]] = None,
        module_name: Optional[str] = None,
        iteration_index: Optional[int] = None,
        instance_suffix: Optional[str] = None,
    ) -> ActionResultValue:
        spec = ActionCallSpec(
            action_name=action_name,
            module_name=module_name,
            kwargs=dict(kwargs or {}),
        )
        node = self.queue_node(
            node_type="action_call",
            label=f"@{action_name}()",
            targets=targets,
            iteration_index=iteration_index,
            instance_suffix=instance_suffix,
            action=spec,
        )
        for value in spec.kwargs.values():
            self._record_data_flow_from_value(node.node_id, value)
        result = self._assign_action_results(node, action_name, targets, iteration_index)
        node.value_expr = result
        return result

    def queue_action_call(
        self,
        action: ir.ActionCall,
        *,
        targets: Optional[Sequence[str]] = None,
        iteration_index: Optional[int] = None,
        instance_suffix: Optional[str] = None,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ActionResultValue:
        spec = self._action_spec_from_ir(action, local_scope)
        node = self.queue_node(
            node_type="action_call",
            label=f"@{spec.action_name}()",
            targets=targets,
            iteration_index=iteration_index,
            instance_suffix=instance_suffix,
            action=spec,
        )
        for kwarg in action.kwargs:
            if kwarg.HasField("value"):
                self._record_data_flow_from_expr(node.node_id, kwarg.value, local_scope)
        result = self._assign_action_results(node, spec.action_name, targets, iteration_index)
        node.value_expr = result
        return result

    def record_assignment(
        self,
        *,
        targets: Sequence[str],
        expr: ir.Expr,
        node_id: Optional[str] = None,
        label: Optional[str] = None,
    ) -> ExecutionNode:
        exec_node_id = node_id or self._next_id("assignment")
        value_expr = self._expr_to_value(expr)
        self._record_data_flow_from_expr(exec_node_id, expr)
        return self.record_assignment_value(
            targets=targets,
            value_expr=value_expr,
            node_id=exec_node_id,
            label=label,
        )

    def record_assignment_value(
        self,
        *,
        targets: Sequence[str],
        value_expr: ValueExpr,
        node_id: Optional[str] = None,
        label: Optional[str] = None,
    ) -> ExecutionNode:
        exec_node_id = node_id or self._next_id("assignment")
        node = self.queue_node(
            node_type="assignment",
            label=label or "assignment",
            node_id=exec_node_id,
            targets=targets,
            value_expr=value_expr,
        )
        self._record_data_flow_from_value(exec_node_id, value_expr)
        self._assign_targets(node.node_id, targets, value_expr)
        return node

    def mark_running(self, node_id: str) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.RUNNING

    def mark_completed(self, node_id: str) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.COMPLETED
        if node_id in self.ready_queue:
            self.ready_queue.remove(node_id)

    def mark_failed(self, node_id: str) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.FAILED
        if node_id in self.ready_queue:
            self.ready_queue.remove(node_id)

    def add_edge(
        self, source: str, target: str, edge_type: EdgeType = EdgeType.STATE_MACHINE
    ) -> None:
        self._register_edge(ExecutionEdge(source=source, target=target, edge_type=edge_type))

    def _register_node(self, node: ExecutionNode) -> None:
        if node.node_id in self.nodes:
            raise RunnerStateError(f"execution node already queued: {node.node_id}")
        self.nodes[node.node_id] = node
        self.ready_queue.append(node.node_id)
        if self._link_queued_nodes and self.timeline:
            self._register_edge(ExecutionEdge(source=self.timeline[-1], target=node.node_id))
        self.timeline.append(node.node_id)

    def _register_edge(self, edge: ExecutionEdge) -> None:
        key = (edge.source, edge.target, edge.edge_type)
        if key in self._edge_keys:
            return
        self._edge_keys.add(key)
        self.edges.append(edge)

    def _get_node(self, node_id: str) -> ExecutionNode:
        node = self.nodes.get(node_id)
        if node is None:
            raise RunnerStateError(f"execution node not found: {node_id}")
        return node

    def _next_id(self, prefix: str) -> str:
        count = self._counters.get(prefix, 0) + 1
        self._counters[prefix] = count
        return f"{prefix}_{count}"

    def _build_execution_id(
        self,
        base_id: str,
        iteration_index: Optional[int],
        instance_suffix: Optional[str],
    ) -> str:
        if instance_suffix and iteration_index is not None:
            raise RunnerStateError("provide either instance_suffix or iteration_index")
        suffix = instance_suffix
        if suffix is None and iteration_index is not None:
            suffix = f"iter_{iteration_index}"
        if suffix:
            return f"{base_id}[{suffix}]"
        return base_id

    def _node_targets(self, node: DAGNode) -> list[str]:
        if isinstance(
            node,
            (
                AssignmentNode,
                ActionCallNode,
                FnCallNode,
                JoinNode,
                AggregatorNode,
                ReturnNode,
            ),
        ):
            if node.targets:
                return list(node.targets)
            if node.target:
                return [node.target]
        return []

    def _apply_template_node(self, exec_node: ExecutionNode, template: DAGNode) -> None:
        if isinstance(template, AssignmentNode):
            if template.assign_expr is None:
                return
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_expr(exec_node.node_id, template.assign_expr)
            self._assign_targets(exec_node.node_id, self._node_targets(template), value_expr)
            return

        if isinstance(template, ActionCallNode):
            for expr in template.kwarg_exprs.values():
                self._record_data_flow_from_expr(exec_node.node_id, expr)
            exec_node.value_expr = self._assign_action_results(
                exec_node,
                template.action_name,
                template.targets or ([template.target] if template.target else None),
                exec_node.iteration_index,
            )
            return

        if isinstance(template, FnCallNode) and template.assign_expr is not None:
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_expr(exec_node.node_id, template.assign_expr)
            self._assign_targets(exec_node.node_id, self._node_targets(template), value_expr)
            return

        if isinstance(template, ReturnNode) and template.assign_expr is not None:
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_expr(exec_node.node_id, template.assign_expr)
            target = template.target or "result"
            self._assign_targets(exec_node.node_id, [target], value_expr)

    def _assign_action_results(
        self,
        node: ExecutionNode,
        action_name: str,
        targets: Optional[Sequence[str]],
        iteration_index: Optional[int],
    ) -> ActionResultValue:
        result_ref = ActionResultValue(
            node_id=node.node_id,
            action_name=action_name,
            iteration_index=iteration_index,
        )
        if not targets:
            return result_ref

        target_list = list(targets)
        if len(target_list) == 1:
            self._assign_targets(node.node_id, target_list, result_ref)
            return result_ref

        for idx, target in enumerate(target_list):
            ref = ActionResultValue(
                node_id=node.node_id,
                action_name=action_name,
                iteration_index=iteration_index,
                result_index=idx,
            )
            self._assign_targets(node.node_id, [target], ref)
        return result_ref

    def _assign_targets(
        self,
        node_id: str,
        targets: Sequence[str],
        value: ValueExpr,
    ) -> None:
        targets_list = list(targets)
        if not targets_list:
            return

        if len(targets_list) == 1:
            self._store_variable(node_id, targets_list, value)
            return

        if isinstance(value, ListValue):
            if len(value.elements) != len(targets_list):
                raise RunnerStateError("tuple unpacking mismatch")
            for target, item in zip(targets_list, value.elements, strict=True):
                self._store_variable(node_id, [target], item)
            return

        if isinstance(value, ActionResultValue):
            for idx, target in enumerate(targets_list):
                ref = ActionResultValue(
                    node_id=value.node_id,
                    action_name=value.action_name,
                    iteration_index=value.iteration_index,
                    result_index=idx,
                )
                self._store_variable(node_id, [target], ref)
            return

        if isinstance(value, FunctionCallValue):
            for idx, target in enumerate(targets_list):
                index_expr = IndexValue(
                    object=value,
                    index=LiteralValue(idx),
                )
                self._store_variable(node_id, [target], index_expr)
            return

        raise RunnerStateError("tuple unpacking mismatch")

    def _store_variable(self, node_id: str, targets: Sequence[str], value: ValueExpr) -> None:
        for target in targets:
            self.variables[target] = value
            self.variable_sources[target] = node_id
        self.variable_updates.append(
            VariableUpdate(node_id=node_id, targets=list(targets), value=value)
        )

    def _record_data_flow_from_expr(
        self,
        node_id: str,
        expr: ir.Expr,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> None:
        source_ids: set[str] = set()
        for name in self._expr_variable_names(expr):
            if local_scope is not None and name in local_scope:
                source_ids.update(self._value_source_nodes(local_scope[name]))
                continue
            source = self.variable_sources.get(name)
            if source is not None:
                source_ids.add(source)
        self._record_data_flow_edges(node_id, source_ids)

    def _record_data_flow_from_value(self, node_id: str, value: ValueExpr) -> None:
        source_ids = self._value_source_nodes(value)
        self._record_data_flow_edges(node_id, source_ids)

    def _record_data_flow_edges(self, node_id: str, source_ids: set[str]) -> None:
        for source_id in source_ids:
            if source_id == node_id:
                continue
            self._register_edge(
                ExecutionEdge(source=source_id, target=node_id, edge_type=EdgeType.DATA_FLOW)
            )

    def _value_source_nodes(self, value: ValueExpr) -> set[str]:
        if isinstance(value, ActionResultValue):
            return {value.node_id}
        if isinstance(value, VariableValue):
            source = self.variable_sources.get(value.name)
            return {source} if source is not None else set()
        if isinstance(value, BinaryOpValue):
            return self._value_source_nodes(value.left) | self._value_source_nodes(value.right)
        if isinstance(value, UnaryOpValue):
            return self._value_source_nodes(value.operand)
        if isinstance(value, ListValue):
            sources: set[str] = set()
            for item in value.elements:
                sources.update(self._value_source_nodes(item))
            return sources
        if isinstance(value, DictValue):
            sources = set()
            for entry in value.entries:
                sources.update(self._value_source_nodes(entry.key))
                sources.update(self._value_source_nodes(entry.value))
            return sources
        if isinstance(value, IndexValue):
            return self._value_source_nodes(value.object) | self._value_source_nodes(value.index)
        if isinstance(value, DotValue):
            return self._value_source_nodes(value.object)
        if isinstance(value, FunctionCallValue):
            sources = set()
            for arg in value.args:
                sources.update(self._value_source_nodes(arg))
            for arg in value.kwargs.values():
                sources.update(self._value_source_nodes(arg))
            return sources
        if isinstance(value, SpreadValue):
            sources = self._value_source_nodes(value.collection)
            for arg in value.action.kwargs.values():
                sources.update(self._value_source_nodes(arg))
            return sources
        return set()

    def _expr_variable_names(self, expr: ir.Expr) -> set[str]:
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal" | None:
                return set()
            case "variable":
                return {expr.variable.name}
            case "binary_op":
                return self._expr_variable_names(expr.binary_op.left) | self._expr_variable_names(
                    expr.binary_op.right
                )
            case "unary_op":
                return self._expr_variable_names(expr.unary_op.operand)
            case "list":
                names: set[str] = set()
                for item in expr.list.elements:
                    names.update(self._expr_variable_names(item))
                return names
            case "dict":
                names = set()
                for entry in expr.dict.entries:
                    names.update(self._expr_variable_names(entry.key))
                    names.update(self._expr_variable_names(entry.value))
                return names
            case "index":
                return self._expr_variable_names(expr.index.object) | self._expr_variable_names(
                    expr.index.index
                )
            case "dot":
                return self._expr_variable_names(expr.dot.object)
            case "function_call":
                names = set()
                for arg in expr.function_call.args:
                    names.update(self._expr_variable_names(arg))
                for kw in expr.function_call.kwargs:
                    if kw.HasField("value"):
                        names.update(self._expr_variable_names(kw.value))
                return names
            case "action_call":
                names = set()
                for kw in expr.action_call.kwargs:
                    if kw.HasField("value"):
                        names.update(self._expr_variable_names(kw.value))
                return names
            case "parallel_expr":
                names = set()
                for call in expr.parallel_expr.calls:
                    call_kind = call.WhichOneof("kind")
                    match call_kind:
                        case "action":
                            for kw in call.action.kwargs:
                                if kw.HasField("value"):
                                    names.update(self._expr_variable_names(kw.value))
                        case "function":
                            for arg in call.function.args:
                                names.update(self._expr_variable_names(arg))
                            for kw in call.function.kwargs:
                                if kw.HasField("value"):
                                    names.update(self._expr_variable_names(kw.value))
                        case None:
                            continue
                        case _:
                            assert_never(call_kind)
                return names
            case "spread_expr":
                names = self._expr_variable_names(expr.spread_expr.collection)
                for kw in expr.spread_expr.action.kwargs:
                    if kw.HasField("value"):
                        names.update(self._expr_variable_names(kw.value))
                return names
            case _:
                assert_never(kind)

    def _expr_to_value(
        self,
        expr: ir.Expr,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ValueExpr:
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return LiteralValue(self._literal_value(expr.literal))
            case "variable":
                name = expr.variable.name
                if local_scope is not None and name in local_scope:
                    return local_scope[name]
                if name in self.variables:
                    return self.variables[name]
                return VariableValue(name)
            case "binary_op":
                left = self._expr_to_value(expr.binary_op.left, local_scope)
                right = self._expr_to_value(expr.binary_op.right, local_scope)
                return self._binary_op_value(expr.binary_op.op, left, right)
            case "unary_op":
                operand = self._expr_to_value(expr.unary_op.operand, local_scope)
                return self._unary_op_value(expr.unary_op.op, operand)
            case "list":
                elements = tuple(
                    self._expr_to_value(item, local_scope) for item in expr.list.elements
                )
                return ListValue(elements=elements)
            case "dict":
                entries = tuple(
                    DictEntryValue(
                        key=self._expr_to_value(entry.key, local_scope),
                        value=self._expr_to_value(entry.value, local_scope),
                    )
                    for entry in expr.dict.entries
                )
                return DictValue(entries=entries)
            case "index":
                obj_value = self._expr_to_value(expr.index.object, local_scope)
                idx_value = self._expr_to_value(expr.index.index, local_scope)
                return self._index_value(obj_value, idx_value)
            case "dot":
                obj_value = self._expr_to_value(expr.dot.object, local_scope)
                return DotValue(object=obj_value, attribute=expr.dot.attribute)
            case "function_call":
                args = tuple(
                    self._expr_to_value(arg, local_scope) for arg in expr.function_call.args
                )
                kwargs = {
                    kw.name: self._expr_to_value(kw.value, local_scope)
                    for kw in expr.function_call.kwargs
                }
                global_fn = (
                    expr.function_call.global_function
                    if expr.function_call.global_function
                    else None
                )
                return FunctionCallValue(
                    name=expr.function_call.name,
                    args=args,
                    kwargs=kwargs,
                    global_function=global_fn,
                )
            case "action_call":
                return self.queue_action_call(expr.action_call, local_scope=local_scope)
            case "parallel_expr":
                calls = [
                    self._call_to_value(call, local_scope) for call in expr.parallel_expr.calls
                ]
                return ListValue(elements=tuple(calls))
            case "spread_expr":
                return self._spread_expr_value(expr.spread_expr, local_scope)
            case None:
                return LiteralValue(None)
            case _:
                assert_never(kind)

    def _call_to_value(
        self,
        call: ir.Call,
        local_scope: Optional[Mapping[str, ValueExpr]],
    ) -> ValueExpr:
        kind = call.WhichOneof("kind")
        match kind:
            case "action":
                return self.queue_action_call(call.action, local_scope=local_scope)
            case "function":
                return self._expr_to_value(
                    ir.Expr(function_call=call.function),
                    local_scope,
                )
            case None:
                return LiteralValue(None)
            case _:
                assert_never(kind)

    def _spread_expr_value(
        self,
        spread: ir.SpreadExpr,
        local_scope: Optional[Mapping[str, ValueExpr]],
    ) -> ValueExpr:
        collection = self._expr_to_value(spread.collection, local_scope)
        if isinstance(collection, ListValue):
            results: list[ValueExpr] = []
            for idx, item in enumerate(collection.elements):
                results.append(
                    self.queue_action_call(
                        spread.action,
                        iteration_index=idx,
                        local_scope={spread.loop_var: item},
                    )
                )
            return ListValue(elements=tuple(results))

        action_spec = self._action_spec_from_ir(spread.action)
        return SpreadValue(
            collection=collection,
            loop_var=spread.loop_var,
            action=action_spec,
        )

    def _binary_op_value(
        self,
        op: ir.BinaryOperator,
        left: ValueExpr,
        right: ValueExpr,
    ) -> ValueExpr:
        if op == ir.BinaryOperator.BINARY_OP_ADD:
            if isinstance(left, ListValue) and isinstance(right, ListValue):
                return ListValue(elements=left.elements + right.elements)
        if isinstance(left, LiteralValue) and isinstance(right, LiteralValue):
            folded = self._fold_literal_binary(op, left.value, right.value)
            if folded is not None:
                return LiteralValue(folded)
        return BinaryOpValue(left=left, op=op, right=right)

    def _unary_op_value(self, op: ir.UnaryOperator, operand: ValueExpr) -> ValueExpr:
        if isinstance(operand, LiteralValue):
            folded = self._fold_literal_unary(op, operand.value)
            if folded is not None:
                return LiteralValue(folded)
        return UnaryOpValue(op=op, operand=operand)

    def _index_value(self, obj_value: ValueExpr, idx_value: ValueExpr) -> ValueExpr:
        if isinstance(obj_value, ListValue) and isinstance(idx_value, LiteralValue):
            if isinstance(idx_value.value, int) and 0 <= idx_value.value < len(obj_value.elements):
                return obj_value.elements[idx_value.value]
        return IndexValue(object=obj_value, index=idx_value)

    def _action_spec_from_node(self, node: ActionCallNode) -> ActionCallSpec:
        kwargs = {name: self._expr_to_value(expr) for name, expr in node.kwarg_exprs.items()}
        return ActionCallSpec(
            action_name=node.action_name,
            module_name=node.module_name,
            kwargs=kwargs,
        )

    def _action_spec_from_ir(
        self,
        action: ir.ActionCall,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ActionCallSpec:
        kwargs = {kw.name: self._expr_to_value(kw.value, local_scope) for kw in action.kwargs}
        module_name = action.module_name if action.HasField("module_name") else None
        return ActionCallSpec(
            action_name=action.action_name,
            module_name=module_name,
            kwargs=kwargs,
        )

    @staticmethod
    def _literal_value(lit: ir.Literal) -> Any:
        kind = lit.WhichOneof("value")
        match kind:
            case "int_value":
                return int(lit.int_value)
            case "float_value":
                return float(lit.float_value)
            case "string_value":
                return lit.string_value
            case "bool_value":
                return bool(lit.bool_value)
            case "is_none":
                return None
            case None:
                return None
            case _:
                assert_never(kind)

    @staticmethod
    def _fold_literal_binary(
        op: ir.BinaryOperator,
        left: Any,
        right: Any,
    ) -> Optional[Any]:
        try:
            match op:
                case ir.BinaryOperator.BINARY_OP_ADD:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left + right
                    if isinstance(left, str) and isinstance(right, str):
                        return left + right
                case ir.BinaryOperator.BINARY_OP_SUB:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left - right
                case ir.BinaryOperator.BINARY_OP_MUL:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left * right
                case ir.BinaryOperator.BINARY_OP_DIV:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left / right
                case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left // right
                case ir.BinaryOperator.BINARY_OP_MOD:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left % right
                case _:
                    return None
        except Exception:
            return None
        return None

    @staticmethod
    def _fold_literal_unary(op: ir.UnaryOperator, operand: Any) -> Optional[Any]:
        try:
            match op:
                case ir.UnaryOperator.UNARY_OP_NEG:
                    if isinstance(operand, (int, float)):
                        return -operand
                case ir.UnaryOperator.UNARY_OP_NOT:
                    return not operand
                case _:
                    return None
        except Exception:
            return None
        return None


def format_value(expr: ValueExpr) -> str:
    return _format_value(expr, 0)


def _format_value(expr: ValueExpr, parent_prec: int) -> str:
    if isinstance(expr, LiteralValue):
        return _format_literal(expr.value)
    if isinstance(expr, VariableValue):
        return expr.name
    if isinstance(expr, ActionResultValue):
        return expr.label()
    if isinstance(expr, BinaryOpValue):
        op_str, prec = _binary_operator(expr.op)
        left = _format_value(expr.left, prec)
        right = _format_value(expr.right, prec + 1)
        rendered = f"{left} {op_str} {right}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, UnaryOpValue):
        op_str, prec = _unary_operator(expr.op)
        operand = _format_value(expr.operand, prec)
        rendered = f"{op_str}{operand}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, ListValue):
        items = ", ".join(_format_value(item, 0) for item in expr.elements)
        return f"[{items}]"
    if isinstance(expr, DictValue):
        entries = ", ".join(
            f"{_format_value(entry.key, 0)}: {_format_value(entry.value, 0)}"
            for entry in expr.entries
        )
        return f"{{{entries}}}"
    if isinstance(expr, IndexValue):
        prec = _precedence("index")
        obj = _format_value(expr.object, prec)
        idx = _format_value(expr.index, 0)
        rendered = f"{obj}[{idx}]"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, DotValue):
        prec = _precedence("dot")
        obj = _format_value(expr.object, prec)
        rendered = f"{obj}.{expr.attribute}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, FunctionCallValue):
        args = [_format_value(arg, 0) for arg in expr.args]
        for name, value in expr.kwargs.items():
            args.append(f"{name}={_format_value(value, 0)}")
        return f"{expr.name}({', '.join(args)})"
    if isinstance(expr, SpreadValue):
        collection = _format_value(expr.collection, 0)
        args = [f"{name}={_format_value(value, 0)}" for name, value in expr.action.kwargs.items()]
        call = f"@{expr.action.action_name}({', '.join(args)})"
        return f"spread {collection}:{expr.loop_var} -> {call}"
    assert_never(expr)


def _binary_operator(op: ir.BinaryOperator) -> tuple[str, int]:
    match op:
        case ir.BinaryOperator.BINARY_OP_OR:
            return "or", 10
        case ir.BinaryOperator.BINARY_OP_AND:
            return "and", 20
        case ir.BinaryOperator.BINARY_OP_EQ:
            return "==", 30
        case ir.BinaryOperator.BINARY_OP_NE:
            return "!=", 30
        case ir.BinaryOperator.BINARY_OP_LT:
            return "<", 30
        case ir.BinaryOperator.BINARY_OP_LE:
            return "<=", 30
        case ir.BinaryOperator.BINARY_OP_GT:
            return ">", 30
        case ir.BinaryOperator.BINARY_OP_GE:
            return ">=", 30
        case ir.BinaryOperator.BINARY_OP_IN:
            return "in", 30
        case ir.BinaryOperator.BINARY_OP_NOT_IN:
            return "not in", 30
        case ir.BinaryOperator.BINARY_OP_ADD:
            return "+", 40
        case ir.BinaryOperator.BINARY_OP_SUB:
            return "-", 40
        case ir.BinaryOperator.BINARY_OP_MUL:
            return "*", 50
        case ir.BinaryOperator.BINARY_OP_DIV:
            return "/", 50
        case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
            return "//", 50
        case ir.BinaryOperator.BINARY_OP_MOD:
            return "%", 50
        case ir.BinaryOperator.BINARY_OP_UNSPECIFIED:
            return "?", 0
        case _:
            assert_never(op)


def _unary_operator(op: ir.UnaryOperator) -> tuple[str, int]:
    match op:
        case ir.UnaryOperator.UNARY_OP_NEG:
            return "-", 60
        case ir.UnaryOperator.UNARY_OP_NOT:
            return "not ", 60
        case ir.UnaryOperator.UNARY_OP_UNSPECIFIED:
            return "?", 0
        case _:
            assert_never(op)


def _precedence(kind: str) -> int:
    match kind:
        case "index" | "dot":
            return 80
        case _:
            return 0


def _format_literal(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, str):
        return json.dumps(value)
    return str(value)
