.PHONY: init run_task_1 run_task_2 test clean

init:
	./standalone.sh

run_task_1:
	AIRFLOW_HOME=$$(pwd) PYTHONPATH=$$(pwd):$${PYTHONPATH} airflow dags test task_1_total_new_bookings 2022-05-12

run_task_2:
	AIRFLOW_HOME=$$(pwd) PYTHONPATH=$$(pwd):$${PYTHONPATH} airflow dags test task_2_stream 2022-05-12

test:
	AIRFLOW_HOME=$$(pwd) PYTHONPATH=$$(pwd):$${PYTHONPATH} pytest

clean:
	rm -r .venv