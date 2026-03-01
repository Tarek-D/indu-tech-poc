FROM apache/spark:3.5.0
USER root
# Installation de pyspark et des dépendances système si nécessaire
RUN pip install pyspark
WORKDIR /app
COPY ticket_processor.py .
# Le script sera lancé par le docker-compose