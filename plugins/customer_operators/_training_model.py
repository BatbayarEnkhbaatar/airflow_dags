from random import randint # Import to generate random numbers


training_model_tasks = [
PythonOperator(
task_id=f"training_model_{model_id}",
python_callable=_training_model,
op_kwargs={
"model": model_id
}
) for model_id in ['A', 'B', 'C']
]