"""Replay variable values from a runner state snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from proto import ast_pb2 as ir

from ..dag import assert_never
from .state import (
    ActionResultValue,
    BinaryOpValue,
    DictValue,
    DotValue,
    FunctionCallValue,
    IndexValue,
    ListValue,
    LiteralValue,
    RunnerState,
    SpreadValue,
    UnaryOpValue,
    ValueExpr,
    VariableValue,
)


class ReplayError(Exception):
    """Raised when replay cannot reconstruct variable values."""


@dataclass(frozen=True)
class ReplayResult:
    variables: dict[str, Any]


def replay_variables(
    state: RunnerState,
    action_results: Mapping[str, Any],
) -> ReplayResult:
    env: dict[str, Any] = {}

    if state.variable_updates:
        for update in state.variable_updates:
            value = _evaluate_value(update.value, env, action_results)
            _assign_targets(env, update.targets, value)
    else:
        for name, expr in state.variables.items():
            env[name] = _evaluate_value(expr, env, action_results)

    return ReplayResult(variables=env)


def _assign_targets(env: dict[str, Any], targets: Sequence[str], value: Any) -> None:
    targets_list = list(targets)
    if not targets_list:
        return

    if len(targets_list) == 1:
        env[targets_list[0]] = value
        return

    if isinstance(value, (list, tuple)):
        if len(value) != len(targets_list):
            raise ReplayError("tuple unpacking mismatch")
        for target, item in zip(targets_list, value, strict=True):
            env[target] = item
        return

    for target in targets_list:
        env[target] = value


def _evaluate_value(
    expr: ValueExpr,
    env: Mapping[str, Any],
    action_results: Mapping[str, Any],
) -> Any:
    if isinstance(expr, LiteralValue):
        return expr.value
    if isinstance(expr, VariableValue):
        if expr.name in env:
            return env[expr.name]
        raise ReplayError(f"variable not found: {expr.name}")
    if isinstance(expr, ActionResultValue):
        return _resolve_action_result(expr, action_results)
    if isinstance(expr, BinaryOpValue):
        left = _evaluate_value(expr.left, env, action_results)
        right = _evaluate_value(expr.right, env, action_results)
        return _apply_binary(expr.op, left, right)
    if isinstance(expr, UnaryOpValue):
        operand = _evaluate_value(expr.operand, env, action_results)
        return _apply_unary(expr.op, operand)
    if isinstance(expr, ListValue):
        return [_evaluate_value(item, env, action_results) for item in expr.elements]
    if isinstance(expr, DictValue):
        return {
            _evaluate_value(entry.key, env, action_results): _evaluate_value(
                entry.value, env, action_results
            )
            for entry in expr.entries
        }
    if isinstance(expr, IndexValue):
        obj = _evaluate_value(expr.object, env, action_results)
        idx = _evaluate_value(expr.index, env, action_results)
        return obj[idx]
    if isinstance(expr, DotValue):
        obj = _evaluate_value(expr.object, env, action_results)
        if isinstance(obj, dict):
            if expr.attribute in obj:
                return obj[expr.attribute]
            raise ReplayError(f"dict has no key '{expr.attribute}'")
        try:
            return object.__getattribute__(obj, expr.attribute)
        except AttributeError as exc:
            raise ReplayError(f"attribute '{expr.attribute}' not found") from exc
    if isinstance(expr, FunctionCallValue):
        return _evaluate_function_call(expr, env, action_results)
    if isinstance(expr, SpreadValue):
        raise ReplayError("cannot replay unresolved spread expression")
    assert_never(expr)


def _resolve_action_result(expr: ActionResultValue, action_results: Mapping[str, Any]) -> Any:
    if expr.node_id not in action_results:
        raise ReplayError(f"missing action result for {expr.node_id}")
    value = action_results[expr.node_id]
    if expr.result_index is None:
        return value
    try:
        return value[expr.result_index]
    except Exception as exc:  # noqa: BLE001
        raise ReplayError(
            f"action result for {expr.node_id} has no index {expr.result_index}"
        ) from exc


def _evaluate_function_call(
    expr: FunctionCallValue,
    env: Mapping[str, Any],
    action_results: Mapping[str, Any],
) -> Any:
    args = [_evaluate_value(arg, env, action_results) for arg in expr.args]
    kwargs = {
        name: _evaluate_value(value, env, action_results) for name, value in expr.kwargs.items()
    }

    if (
        expr.global_function
        and expr.global_function != ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED
    ):
        return _evaluate_global_function(expr.global_function, args, kwargs)

    raise ReplayError(f"cannot replay non-global function call: {expr.name}")


def _evaluate_global_function(
    global_function: ir.GlobalFunction,
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
) -> Any:
    match global_function:
        case ir.GlobalFunction.GLOBAL_FUNCTION_RANGE:
            return list(range(*args))
        case ir.GlobalFunction.GLOBAL_FUNCTION_LEN:
            if args:
                return len(args[0])
            if "items" in kwargs:
                return len(kwargs["items"])
            raise ReplayError("len() missing argument")
        case ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE:
            if args:
                return list(enumerate(args[0]))
            if "items" in kwargs:
                return list(enumerate(kwargs["items"]))
            raise ReplayError("enumerate() missing argument")
        case ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION:
            if args:
                return _is_exception_value(args[0])
            if "value" in kwargs:
                return _is_exception_value(kwargs["value"])
            raise ReplayError("isexception() missing argument")
        case ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
            raise ReplayError("global function unspecified")
        case _:
            assert_never(global_function)


def _apply_binary(op: ir.BinaryOperator, left: Any, right: Any) -> Any:
    match op:
        case ir.BinaryOperator.BINARY_OP_OR:
            return left or right
        case ir.BinaryOperator.BINARY_OP_AND:
            return left and right
        case ir.BinaryOperator.BINARY_OP_EQ:
            return left == right
        case ir.BinaryOperator.BINARY_OP_NE:
            return left != right
        case ir.BinaryOperator.BINARY_OP_LT:
            return left < right
        case ir.BinaryOperator.BINARY_OP_LE:
            return left <= right
        case ir.BinaryOperator.BINARY_OP_GT:
            return left > right
        case ir.BinaryOperator.BINARY_OP_GE:
            return left >= right
        case ir.BinaryOperator.BINARY_OP_IN:
            return left in right
        case ir.BinaryOperator.BINARY_OP_NOT_IN:
            return left not in right
        case ir.BinaryOperator.BINARY_OP_ADD:
            return left + right
        case ir.BinaryOperator.BINARY_OP_SUB:
            return left - right
        case ir.BinaryOperator.BINARY_OP_MUL:
            return left * right
        case ir.BinaryOperator.BINARY_OP_DIV:
            return left / right
        case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
            return left // right
        case ir.BinaryOperator.BINARY_OP_MOD:
            return left % right
        case ir.BinaryOperator.BINARY_OP_UNSPECIFIED:
            raise ReplayError("binary operator unspecified")
        case _:
            assert_never(op)


def _apply_unary(op: ir.UnaryOperator, operand: Any) -> Any:
    match op:
        case ir.UnaryOperator.UNARY_OP_NEG:
            return -operand
        case ir.UnaryOperator.UNARY_OP_NOT:
            return not operand
        case ir.UnaryOperator.UNARY_OP_UNSPECIFIED:
            raise ReplayError("unary operator unspecified")
        case _:
            assert_never(op)


def _is_exception_value(value: Any) -> bool:
    if isinstance(value, BaseException):
        return True
    if isinstance(value, dict) and "type" in value and "message" in value:
        return True
    return False
