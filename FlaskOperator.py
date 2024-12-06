from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from flask import Flask

# função que valida se a data está entre o valor maximo e minimo
"""
Verifica se a data está dentro do range

:param datetime value: data para comparar 
:param datetime min_value: data minima 
:param datetime max_value: data maxima 

:return (boolean): validação da data
"""


class FlaskAPIServerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FlaskAPIServerOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        app = Flask(__name__)

        @app.route("/")
        def hello_world():
            return "Hello, Airflow API!"

        app.run(port=5000)
