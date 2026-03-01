# Indu-tech-poc

## Présentation
Ce projet est un Proof of Concept (POC) simulant une ingestion de données IoT/Tickets en temps réel utilisant **Redpanda** pour le streaming et **PySpark** pour le traitement ETL.

Les tickets sont générés en temps réel et contiennent des informations sur les demandes des clients:

* L'ID du ticket
* L'ID du client 
* La date et l'heure de création
* La demande
* Le type de demande
* La priorité 

Mise en place un pipeline de données pour ingérer, traiter et analyser ces tickets en temps réel. 

## Outils

* Redpanda
* Pyspark
* Docker

## Architecture du Pipeline

```mermaid
graph LR
    subgraph Sources
        A[Générateur Python] -->|JSON| B(Redpanda Topic: client_tickets)
    end

    subgraph Traitement_Spark
        B -->|readStream| C{PySpark Processor}
        C -->|Transformation| D[Assignation Équipe]
        C -->|Agrégation| E[Insights par Type]
    end

    subgraph Stockage_Final
        D -->|Write| F[(Parquet Files)]
        E -->|Output| G[Console/Visualisation]
    end
```