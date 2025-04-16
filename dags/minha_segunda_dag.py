from time import sleep
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


#decorando a função pipeline com o decorador DAG, acrescentando comporamento a função pipeline
@dag(
        dag_id="minhha_segunda_dag",
        description="minha segunda etl",
        schedule="* * * * *",
        start_date=datetime(2025, 4, 16),
        catchup=False
)
def minha_segunda_dag():

    @task
    def primeira_atividade():
        return "Minha primeira atividade parametro para outra função"

    @task
    def segunda_atividade(response):
        print(response)
        print("Minha segunda atividade")
        sleep(2)

    @task
    def terceira_atividade():
        print("Minha terceira atividade")
        sleep(2)

    @task
    def quarta_atividade():
        print("Pipeline finalizada!")

    t1 = primeira_atividade()
    t2 = segunda_atividade(t1)
    t3 = terceira_atividade()
    t4 = quarta_atividade()


    # t1 >> t2 >> t3 >> t4
    chain(t1,t2,t3,t4)

minha_segunda_dag()