from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class Tests(StrEnum):
    NOT_NULL = "not_null.sql"
    UNIQUE = "unique.sql"
    ACCEPTED_VALUES = "accepted_values.sql"
    EXPRESSION_IS_TRUE = "expression_is_true.sql"


class BaseDataTestOperator(BaseOperator, ABC):
    template_fields = ["query"]
    template_ext = [".sql"]

    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        query: Tests,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id: str = postgres_conn_id
        self.params: dict[str, Any] = {
            "table_name": table_name,
        }
        self.query = query

    @abstractmethod
    def _handle_result(self, result: tuple) -> Any:
        pass

    def execute(self, context: Context) -> Any:
        postgres_hook = PostgresHook(self.postgres_conn_id)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.query)
                result: tuple = cursor.fetchone()
                self._handle_result(result)


class NotNullTestOperator(BaseDataTestOperator):
    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        column_name: str,
        query: Tests = Tests.NOT_NULL,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            *args,
            postgres_conn_id=postgres_conn_id,
            table_name=table_name,
            query=query,
            **kwargs,
        )
        self.params["column_name"] = column_name
        self._fail_message = "nulls"

    def _handle_result(self, result: tuple) -> Any:
        table_name: str = self.params["table_name"]
        column_name: str = self.params["column_name"]
        if result:
            self.log.error(
                "Table '%s' in column '%s' have %s",
                table_name,
                column_name,
                self._fail_message,
            )
            raise AirflowException
        self.log.info(
            "Table '%s' in column '%s' have no %s",
            table_name,
            column_name,
            self._fail_message,
        )


class UniqueTestOperator(NotNullTestOperator):
    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        column_name: str,
        query: Tests = Tests.UNIQUE,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            *args,
            postgres_conn_id=postgres_conn_id,
            table_name=table_name,
            column_name=column_name,
            query=query,
            **kwargs,
        )
        self._fail_message = "duplicates"


class AcceptedValuesTestOperator(NotNullTestOperator):
    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        column_name: str,
        accepted_values: tuple[Any, ...],
        query: Tests = Tests.ACCEPTED_VALUES,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            *args,
            postgres_conn_id=postgres_conn_id,
            table_name=table_name,
            column_name=column_name,
            query=query,
            **kwargs,
        )
        self.params["accepted_values"] = accepted_values
        self._fail_message = f"other values than {accepted_values}"


class ExpressionIsTrueTestOperator(BaseDataTestOperator):
    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        query: Tests = Tests.EXPRESSION_IS_TRUE,
        expression: str,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            *args,
            postgres_conn_id=postgres_conn_id,
            table_name=table_name,
            query=query,
            **kwargs,
        )
        self.params["expression"] = expression

    def _handle_result(self, result: tuple) -> Any:
        table_name: str = self.params["table_name"]
        expression: str = self.params["expression"]
        if result:
            self.log.error(
                "Table '%s' have records where expression '%s' is not True",
                table_name,
                expression,
            )
            raise AirflowException
        self.log.info(
            "Table '%s' have no records where expression '%s is not True",
            table_name,
            expression,
        )
