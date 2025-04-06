# Makefile para manejar Airflow con Docker

# Variables
AIRFLOW_CMD=docker compose run --rm airflow

.PHONY: init up down create-user logs

init:
	@$(AIRFLOW_CMD) airflow db init

create-user:
	@$(AIRFLOW_CMD) airflow users create \
		--username admin \
		--password admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

up:
	docker compose up

down:
	docker compose down

logs:
	docker compose logs -f
