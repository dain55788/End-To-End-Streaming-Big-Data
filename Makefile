streaming_up:
	docker compose -f docker-compose.yaml up -d

streaming_down:
	docker compose -f docker-compose.yaml down

airflow_up:
	docker compose -f airflow-docker-compose.yaml up -d

airflow_down:
	docker compose -f airflow-docker-compose.yaml down
