# Scalable ETL Pipeline: Base SIRENE & Référentiel NAF

C'est un pipeline de données modulaires que j'ai fait en Python dans la logique d'une architecture de Data engineering. J'ai respecté la classification des datas : bronze, silver et gold, tout l'ETL est codé par mes mains et mes doigts, comme ce texte que tu lis.

L'idée de base c'est : Avoir toutes les datas des entreprises des CEO que j'ai en connexion sur LinkedIN pour situer leur busines, leur activité, leur chiffre d'affaires, la date de création de leur entreprise, leurs employés...

Le projet par d'une liste de connexions LinkedIn, de laquelle il classifie les rôles : `data analyst`, ceo, founder, dirigeant, PDG, Founder etc

Ensuite, le programme lance une recherche à partir du nom des entreprises de ces personnes là, directement à l'API du gouvernement Français

En sortie, on a 3 tables qui donnent les infos de l'entreprise : 
- les infos administratives, avec siège, activité, nombre d'employés, CA si renseigné
- les dirigeants de chacune des filiales et de chacun des sièges
- les établissements et les sièges sociaux, avec latitude et longitude, pour pouvoir les afficher dans des solutions de dataviz (pour ceux qui aimeraient avoir les entreprises dans le bâtiment dans la région de Lyon par exemple)



Ce que je voulais c'est avoir une pleine visibilité sur des potentiels prospects qui ont des besoins en data.

---

## Architecture du Projet (Data Lakehouse Pattern)

Le projet respecte une séparation stricte des responsabilités (découplage **Extract / Transform / Load**) et organise la donnée selon l'architecture Medallion :

* **Extract (Bronze Layer) :** Récupération brute des données d'entreprises via l'API Recherche Entreprises et téléchargement du référentiel des codes NAF (format CSV). Les fichiers sont historisés localement avec un timestamping strict pour permettre le *data replay*.
* **Transform (Silver Layer) :** Centralisation de la logique métier. Filtrage avancé des entreprises selon les tranches d'effectifs, nettoyage massif des chaînes de caractères (suppression des emojis/symboles parasites via Regex Unicode) et jointure optimisée en mémoire.
* **Load (Gold Layer) :** Exportation de la donnée enrichie et prête pour l'analyse ou l'ingestion en base de données.

```text
[API / Open Data] 
       │
       ▼
┌──────────────┐      1/ Sauvegarde Brute (Append-Only)
│  extract.py  │ ───► data/bronze/entreprises_2026-06-05.json
└──────────────┘
       │ (Chemin du fichier)
       ▼
┌──────────────┐      2/ Filtres effectifs + Nettoyage Regex (Polars)
│ transform.py │ ───► Sélection automatique du fichier le plus récent
└──────────────┘
       │ (DataFrame en mémoire)
       ▼
┌──────────────┐      3/ Écriture finale
│   load.py    │ ───► data/gold/entreprises_enrichies.csv
└──────────────┘