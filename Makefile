streaming_up:
	docker compose -f docker-compose.yaml up -d

streaming_down:
	docker compose -f docker-compose.yaml down

# MinIO bucket
create_bucket:
	python scripts/create_bucket.py

airflow_up:
	docker compose -f airflow-docker-compose.yml up -d

airflow_down:
	docker compose -f airflow-docker-compose.yml down

storage_compose_up:
	docker compose  -f storage-docker-compose.yaml up

storage_compose_down:
	docker compose  -f storage-docker-compose.yaml down
