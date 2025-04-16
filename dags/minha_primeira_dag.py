from time import sleep
from datetime import datetime

from airflow.decorators import dag, task


#decorando a função pipeline com o decorador DAG, acrescentando comporamento a função pipeline
@dag(
        dag_id="minhha_primeira_dag",
        description="minha primeira etl",
        schedule="* * * * *",
        start_date=datetime(2025, 4, 16),
        catchup=False
)
def pipeline():

    @task
    def primeira_atividade():
        print("Minha primeira atividade")
        sleep(2)

    @task
    def segunda_atividade():
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
    t2 = segunda_atividade()
    t3 = terceira_atividade()
    t4 = quarta_atividade()


    t1 >> t2 >> t3 >> t4

pipeline()