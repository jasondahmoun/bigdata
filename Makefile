.PHONY: help start stop logs producer consumer clean

help:
	@echo "=== Commandes disponibles ==="
	@echo "  make start     - Démarrer l'infrastructure"
	@echo "  make stop      - Arrêter l'infrastructure"
	@echo "  make logs      - Voir les logs"
	@echo "  make producer  - Lancer le producer météo"
	@echo "  make consumer  - Voir les messages Kafka"
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

clean:
	docker-compose down -v
