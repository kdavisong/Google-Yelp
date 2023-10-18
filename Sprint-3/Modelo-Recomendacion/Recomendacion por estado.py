#!/usr/bin/env python
# coding: utf-8

# In[46]:


import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def recomendacion_para_estado(estado: str, **kwargs):
    execution_date = kwargs['execution_date']
    df1 = pd.read_csv('categoria_por_estado.csv')
    df1 = df1.sort_values(by=estado)
    categorias_top = df1['Categoria'].head().tolist()
    df2 = pd read_csv('oportunidad_por_postal.csv')
    df2 = df2[df2['state_id'] == estado]
    df2 = df2.sort_values(by='oportunidad')
    postal_top = df2['postal_code'].head().tolist()
    
    message = '\n-----\n'
    message += 'Categorías populares: ' + ', '.join(categorias_top) + '\n'
    message += 'Códigos postales con mejor oportunidad: ' + ', '.join(postal_top) + '\n'
    message += '-----'
    
    print(f"Execution date: {execution_date}")
    print(message)
    
    return message

default_args = {
    'owner': 'blue_consulting',
    'start_date': datetime(2023, 10, 18),  # You can set the desired start date
    'retries': 1,
}

dag = DAG(
    'recomendacion_por_estado',
    default_args=default_args,
    description='DAG to run the recomendacion_para_estado function',
    schedule_interval=None,  # Set the desired schedule interval or use external triggers
)

task_recomendacion = PythonOperator(
    task_id='run_recomendacion',
    python_callable=recomendacion_para_estado,
    op_args=['CL'],  # Replace with the desired state
    provide_context=True,  # Use provide_context to access context variables
    dag=dag,
)

# Set up task dependencies if needed
# task_recomendacion.set_upstream(...)
# task_recomendacion.set_downstream(...)

if __name__ == "__main__":
    dag.cli()


