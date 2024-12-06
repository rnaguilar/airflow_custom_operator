import os
from datetime import datetime, timedelta

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# função que valida se a data está entre o valor maximo e minimo
"""
Verifica se a data está dentro do range

:param datetime value: data para comparar 
:param datetime min_value: data minima 
:param datetime max_value: data maxima 

:return (boolean): validação da data
"""


class ListLogFilesOperator(BaseOperator):
    template_fields = ("base_log_folder",)

    @apply_defaults
    def __init__(self, base_log_folder, *args, **kwargs):
        super(ListLogFilesOperator, self).__init__(*args, **kwargs)
        self.base_log_folder = base_log_folder

    def execute(self, context):
        for root, dirs, files in os.walk(self.base_log_folder):
            for file in files:
                file_path = os.path.join(root, file)
                modification_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                self.log.info(f"File: {file_path} - Last modified: {modification_time}")
                print(f"File: {file_path} - Last modified: {modification_time}")


class CleanupLogFilesOperator(BaseOperator):
    template_fields = ("base_log_folder", "days")

    @apply_defaults
    def __init__(self, base_log_folder, days, *args, **kwargs):
        super(CleanupLogFilesOperator, self).__init__(*args, **kwargs)
        self.base_log_folder = base_log_folder
        self.days = days

    def execute(self, context):
        for root, dirs, files in os.walk(self.base_log_folder):
            for file in files:
                file_path = os.path.join(root, file)
                modification_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                age = datetime.now() - modification_time

                if age > timedelta(days=self.days):
                    self.log.info(
                        f"Deleting file: {file_path} - Last modified: {modification_time}"
                    )
                    print(
                        f"Deleting file: {file_path} - Last modified: {modification_time}"
                    )
                    os.remove(file_path)
