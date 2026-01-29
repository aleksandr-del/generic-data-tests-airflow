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


class BaseDataTestOperator(BaseOperator, ABC):
    template_fields = ["query"]
    template_ext = [".sql"]

    def __init__(
        self,
        *args: tuple[Any, ...],
        postgres_conn_id: str,
        table_name: str,
        column_name: str,
        query: Tests,
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id: str = postgres_conn_id
        self.params: dict[str, Any] = {
            "table_name": table_name,
            "column_name": column_name,
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
            column_name=column_name,
            query=query,
            **kwargs,
        )
        self.fail_message = "nulls"

    def _handle_result(self, result: tuple) -> Any:
        table_name: str = self.params["table_name"]
        column_name: str = self.params["column_name"]
        if result:
            self.log.error(
                "Table '%s' in column '%s' have %s",
                table_name,
                column_name,
                self.fail_message,
            )
            raise AirflowException
        self.log.info(
            "Table '%s' in column '%s' have no %s",
            table_name,
            column_name,
            self.fail_message,
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
        self.fail_message = "duplicates"
