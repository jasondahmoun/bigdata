.PHONY: help start stop logs producer consumer spark clean

help:
	@echo "=== Commandes disponibles ==="
	@echo "  make start     - Démarrer l'infrastructure"
	@echo "  make stop      - Arrêter l'infrastructure"
	@echo "  make logs      - Voir les logs"
	@echo "  make producer  - Lancer le producer météo"
	@echo "  make consumer  - Voir les messages Kafka"
	@echo "  make spark     - Lancer l'agrégation Spark"
	@echo "  make clean     - Tout supprimer"

start:
	docker-compose up -d
	@echo "✅ Démarré! Jupyter: http://localhost:8888/lab"

stop:
	docker-compose down

logs:
	docker-compose logs -f

producer:
	docker exec -it jupyter python /home/jovyan/work/weather_producer.py

consumer:
	docker exec -it kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic weather_data \
		--from-beginning

spark:
	docker exec -it spark-submit spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/app/spark_weather_aggregation.py

clean:
	docker-compose down -v
