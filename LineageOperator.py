import json
import re

import sqlparse
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sql_metadata import Parser

from src.rulles.xcom_rulles import DagRulles as Rulles


def extract_tables(query):
    """
    Extrai as tabelas do SQL utilizando a biblioteca sql_metadata.

    Args:
        query (str): Consulta SQL.

    Returns:
        list: Lista de dicionários contendo informações sobre as tabelas.

    """
    parser = Parser(query)
    tables = parser.tables

    result = [
        {"full_table": table, "table_name": table.rsplit(".", 1)[-1]}
        for table in tables
    ]
    return result


def find_last_value(text, search):
    """
    Encontra a última posição de um valor em uma string.

    Args:
        text (str): Texto de entrada.
        search (str): Valor a ser encontrado.

    Returns:
        int: Índice da última ocorrência do valor na string.

    """
    for i in range(len(text) - 1, -1, -1):
        if text[i] in search:
            return i

    return None


def get_table_structure(table_name, cte_tables):
    """
    Obtém a estrutura da tabela e suas sub-tabelas.

    Args:
        table_name (str): Nome da tabela.
        cte_tables (dict): Dicionário contendo as sub-tabelas.

    Returns:
        list: Lista de dicionários representando a estrutura da tabela.

    """
    table_structure = []
    if table_name in cte_tables:
        for subt in cte_tables[table_name]:
            sub_structure = get_table_structure(subt, cte_tables)
            table_structure.append(
                {"table": subt, "subtables": sub_structure}
                if sub_structure
                else {"table": subt}
            )
    return table_structure


def get_list_of_start_ctes(text):
    """
    Obtém a lista de posições de início das CTEs (Common Table Expressions) em uma consulta SQL.

    Args:
        text (str): Consulta SQL.

    Returns:
        list: Lista de posições de início das CTEs.

    """
    return [match.start() for match in re.finditer(r"\bAS\b\s*\(", text, re.IGNORECASE)]


def remove_cte(query):
    """
    Remove as CTEs de uma consulta SQL.

    Args:
        query (str): Consulta SQL.

    Returns:
        str: Consulta SQL sem as CTEs.

    """
    parsed = sqlparse.parse(query)
    statements = parsed[0].tokens
    cte_keywords = ["WITH", "SELECT"]

    select_query = ""
    inside_cte = False

    for token in statements:
        if token.value.upper() in cte_keywords:
            inside_cte = not inside_cte

        if not inside_cte:
            select_query += token.value

    return select_query.strip() if select_query.strip() != "" else query


class ExtractLineage(BaseOperator):
    """
    Um operador personalizado do Apache Airflow que extrai informações de lineage (linhagem)
    a partir de uma consulta SQL.

    Args:
        query_string (str): Consulta SQL a ser analisada.
        final_table (str): Nome da tabela final da linhagem.
        taskid (str): ID da tarefa no Airflow (opcional).

    Attributes:
        query_string (str): Consulta SQL a ser analisada.
        final_table (str): Nome da tabela final da linhagem.
        taskid (str): ID da tarefa no Airflow.

    Raises:
        Exception: Lança uma exceção em caso de erro na análise do SQL.

    """
    @apply_defaults
    def __init__(
        self, query_string: str, final_table: str, taskid: str = None, *args, **kwargs
    ) -> None:
        """
        Inicializa o operador.

        Args:
            query_string (str): Consulta SQL a ser analisada.
            final_table (str): Nome da tabela final da linhagem.
            taskid (str): ID da tarefa no Airflow (opcional).

        """
        super(ExtractLineage, self).__init__(*args, **kwargs)
        self.query_string: str = query_string.upper()
        self.final_table: str = final_table
        self.taskid = taskid

    def execute(self, context):
        """
        Executa o operador.

        Extrai informações de lineage (linhagem) a partir da consulta SQL fornecida.

        Args:
            context (dict): Dicionário de contexto do Airflow.

        Raises:
            Exception: Lança uma exceção em caso de erro na análise do SQL.

        """
        print(f"TASKIKD {self.taskid}")
        if self.taskid is not None:
            ti = context["ti"]
            objRulle = Rulles()
            objRulle.getRulleValues(taskid=self.taskid, ti=ti)
            self.query_string = objRulle.query
            self.final_table = objRulle.table

        query_cte = self.query_string.replace(
            remove_cte(self.query_string), ""
        ).lstrip()
        subquerys = []
        positions = get_list_of_start_ctes(query_cte)
        sizeVet = len(positions)

        for position in range(sizeVet):
            query_start = positions[position]
            query_end = positions[position + 1] if position + 1 <= sizeVet - 1 else -1
            new_query = query_cte[query_start + 4 : query_end]
            query_position_start = 0 if position == 0 else positions[position - 1]
            query_table = (
                query_cte[query_position_start : query_start - 1]
                .replace("\n", " ")
                .replace(",", " ")
            )
            new_query2 = new_query[: find_last_value(new_query, ",")].rstrip()
            table_name = (
                query_table[find_last_value(query_table, [" "]) :].rstrip().lstrip()
            )
            subquerys.append({"table": table_name, "query": new_query2[:-1]})

        cte_tables = {qq["table"]: extract_tables(qq["query"]) for qq in subquerys}

        try:
            tables = extract_tables(remove_cte(self.query_string))
            print(cte_tables)
            print("Lineage Final")
            lista_tabelas = [
                {
                    "table": table["full_table"],
                    "subtables": get_table_structure(table["full_table"], cte_tables),
                }
                for table in tables
            ]
            data = {self.final_table: lista_tabelas}
        except Exception as e:
            data = {self.final_table: ""}
            print("SQL INVALIDO")
            print(f"ERROR {e}")

        json_data = json.dumps(data, indent=4)
        self.xcom_push(context, key="lineage_result", value=json_data)
