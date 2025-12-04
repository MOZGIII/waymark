"""Tests for IR builder functionality."""


from proto import ast_pb2 as ir


class TestAsyncioSleepDetection:
    """Test that asyncio.sleep is detected and converted to @sleep action."""

    def _find_sleep_action(self, program: ir.Program) -> ir.ActionCall | None:
        """Find a @sleep action call in the program."""
        for fn in program.functions:
            for stmt in fn.body.statements:
                if stmt.HasField("action_call"):
                    if stmt.action_call.action_name == "sleep":
                        return stmt.action_call
        return None

    def _get_duration_kwarg(self, action_call: ir.ActionCall) -> ir.Kwarg | None:
        """Get the duration kwarg from a sleep action."""
        for kw in action_call.kwargs:
            if kw.name == "duration":
                return kw
        return None

    def test_asyncio_dot_sleep_pattern(self) -> None:
        """Test: import asyncio; asyncio.sleep(1)"""
        from tests.fixtures_sleep.sleep_import_asyncio import SleepImportAsyncioWorkflow

        program = SleepImportAsyncioWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 1

    def test_from_asyncio_import_sleep_pattern(self) -> None:
        """Test: from asyncio import sleep; sleep(2)"""
        from tests.fixtures_sleep.sleep_from_import import SleepFromImportWorkflow

        program = SleepFromImportWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 2

    def test_from_asyncio_import_sleep_as_alias_pattern(self) -> None:
        """Test: from asyncio import sleep as async_sleep; async_sleep(3)"""
        from tests.fixtures_sleep.sleep_aliased_import import SleepAliasedImportWorkflow

        program = SleepAliasedImportWorkflow.workflow_ir()

        sleep_action = self._find_sleep_action(program)
        assert sleep_action is not None, "Expected @sleep action in IR"

        duration = self._get_duration_kwarg(sleep_action)
        assert duration is not None, "Expected duration kwarg"
        assert duration.value.HasField("literal"), "Expected literal value"
        assert duration.value.literal.int_value == 3
