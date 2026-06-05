# Journal d'apprentissage Pandas - Syntaxe & Optimisation
> **Date :** 2026-06-01
## Problème 1 : Les lignes d'introduction dans les fichiers CSV de LinkedIn

* **Problématique :** L'export CSV de LinkedIn contient des phrases d'introduction en cellule A1 (ex: "Connections, compiled on..."). Pandas essaie de lire ces phrases comme les noms des colonnes, ce qui décale tout le tableau et crée des erreurs.
* **Idée à avoir :** Il faut dire à Pandas d'ignorer le début du fichier pour démarrer la lecture directement là où se trouvent les vrais titres de colonnes (*First Name, Last Name, Position*).
* **Solution :** Utiliser l'argument `skiprows` pour sauter les lignes inutiles.
    ```python
    import pandas as pd

    # Saute les 3 premières lignes d'introduction de LinkedIn
    df = pd.read_csv('Connections.csv', skiprows=3, encoding='utf-8')
    ```

---

## Problème 2 : Performance et lourdeur de la boucle `for` sur un DataFrame

* **Problématique :** Utiliser une boucle `for` pour analyser les lignes d'un DataFrame une par une (avec `if any()`) est très lent sur de grands volumes de données. De plus, faire un `.loc` à chaque itération force Pandas à scanner tout le tableau de manière répétitive pour rien.
* **Idée à avoir :** En Pandas, on évite les boucles. On doit penser en "vectorisation", c'est-à-dire appliquer une opération sur toute la colonne d'un seul coup grâce aux fonctions natives.
* **Solution :** Combiner le symbole `|` (qui signifie "OU" en Regex) et la fonction `.str.contains()`.
    ```python
    # On crée un "super mot-clé" avec des OU (|)
    pattern_analyst = "analyst|power bi|tableau|excel"

    # On applique le filtre sur toute la colonne d'un coup (case=False gère les majuscules)
    df.loc[df['position2'].str.contains(pattern_analyst, case=False, na=False), "Category"] = "data analyst"
    ```

---

## Problème 3 : Le script plante à cause de profils LinkedIn incomplets (Cellules vides)

* **Problématique :** Certains contacts n'ont pas renseigné leur poste actuel. Dans Pandas, cela crée des cases vides (`NaN`). Si on cherche un mot-clé textuel dans une case vide, Python plante avec une erreur (


## [2026-06-03] 
# Synthese Technique et Architecture - Pipeline LinkedIn ETL

## 1. Choix d'Architecture des Donnees (Vision Senior ELT)

Le projet adopte une approche ELT (Extract, Load, Transform) stricte. Aucun traitement n'est réalisé à la volée en mémoire vive sans persistance intermédiaire. Ce choix garantit l'indempotence du code et protège les quotas d'appels aux API sources.

### Structure du Data Lake (Dossier data_dir/)

* **1_bronze_json/ (Zone Brute) :**
    * Rôle : Stockage des réponses HTTP brutes au format JSON.
    * Règle : Un fichier par requête d'entreprise (ex: `laposte_raw.json`). Ce stockage local permet de relancer les scripts de nettoyage indéfiniment sans réinterroger l'API.

* **2_silver_parquet/ (Zone Propre) :**
    * Rôle : Centralisation globale de l'ensemble des données nettoyées et typées.
    * Format : Parquet (colonnaire), privilégié pour ses performances de compression et sa vitesse de lecture avec Pandas.
    * Découpage relationnel (Liaison via la clé primaire/étrangère `siren`) :
        * `entreprises.parquet` : Une ligne par entreprise (Siren, raison sociale, CA, secteur...).
        * `dirigeants.parquet` : Une ligne par dirigeant, toutes entreprises confondues (Siren, Nom, Prénom, Fonction...).
        * `etablissements.parquet` : Une ligne par établissement (Siret, Siren, Adresse...).

* **3_gold_csv/ (Zone Finale) :**
    * Rôle : Stockage du livrable final destiné à l'exploitation.
    * Fichier : `prospects_linkedin.csv`. Il contient le résultat des jointures et des filtres métier (exclusion des grandes structures comme La Poste/Total, exclusion des codes NAF non ciblés comme l'immobilier).

---

## 2. Configuration Centralisee (config.yaml)

L'arborescence logique est déclarée de manière centralisée pour s'affranchir des chemins écrits en dur dans le code Python.

```yaml
paths:
  data_dir: "data_dir"
  bronze_json: "data_dir/1_bronze_json"
  silver_parquet: "data_dir/2_silver_parquet"
  gold_csv: "data_dir/3_gold_csv"
```
## [2026-06-03]
### Pourquoi ne pas isoler la catégorisation LinkedIn dans un script indépendant ?

* **Sobriété du stockage (Pas de fichier intermédiaire inutile) :** Créer un script isolé t'obligerait à sauvegarder un fichier intermédiaire sur ton disque dur (ex: `connections_clean.csv`) juste pour passer la donnée au script suivant. Un Senior préfère manipuler cette liste temporaire directement en mémoire vive tant qu'elle n'a pas rencontré de source externe (l'API).
* **Unité de l'action (Le "Carburant" de l'API) :** La catégorisation des postes et l'isolation des "CEO" n'ont aucune autre utilité dans ton projet que de servir de déclencheur à l'étape d'extraction. Ces étapes forment un bloc logique unique : on nettoie *uniquement* pour savoir quoi requêter. Les séparer complexifierait la maintenance pour aucun bénéfice technique.
* **Fluidité de la chaîne d'exécution :** Garder cette logique au début du premier script permet de lancer ton extraction en une seule commande. De plus, si le fichier LinkedIn d'origine comporte une ligne corrompue, le script peut la rejeter ou la corriger en direct avant même de gaspiller une requête API ou de générer un fichier intermédiaire tronqué.

## [2026-06-03]
# Note de Conception Architecture : L'Illusion de DuckDB en Zone Bronze

Ce document analyse pourquoi l'intégration de DuckDB pour lire les fichiers JSON de la Zone Bronze constitue une **fausse bonne idée** technique, et définit son véritable périmètre d'efficacité dans ton pipeline.

---

## 1. La Fausse Bonne Idée (Le Mirage de la Performance)

### Le raisonnement (trompeur) :
"DuckDB est le moteur de base de données le plus rapide du moment pour traiter des volumes massifs de données en local. Comme j'ai potentiellement beaucoup d'entreprises à analyser, utiliser DuckDB à la place de Pandas pour lire mes fichiers JSON va réduire l'empreinte mémoire, accélérer mon script et m'éviter de manipuler des listes de DataFrames."

### Pourquoi ça a l'air vrai :
DuckDB intègre nativement une fonction nommée `read_json_auto()`. On peut littéralement écrire une requête SQL du type `SELECT * FROM read_json_auto('data_dir/1_bronze_json/*.json')`. Sur le papier, cela donne l'impression qu'on peut sauter l'étape de la boucle Python et charger toutes les entreprises d'un coup de manière ultra-optimisée.

---

## 2. La Réalité Technique (Pourquoi c'est nul ici)

Dans le monde réel de la production, un Data Engineer Senior écartera DuckDB à cette étape précise pour trois raisons majeures :

### A. Le cauchemar des données imbriquées (Nested JSON)
L'API Sirene/Insee ne renvoie pas un tableau plat. Elle renvoie un arbre hautement imbriqué : une entreprise contient une *liste* d'établissements, qui contient elle-même des sous-structures, et une *liste* de dirigeants officiels.

* **Avec Pandas :** Tu disposes de fonctions chirurgicales comme `pd.json_normalize()` ou de la manipulation d'objets Python (dictionnaires/listes) pour découper et extraire proprement ces données dans tes 3 tables distinctes (Entreprises, Dirigeants, Établissements).
* **Avec DuckDB :** Tu te retrouves obligé d'écrire des requêtes SQL d'une complexité rare, en utilisant des fonctions spécifiques comme `unnest()` pour aplatir les structures imbriquées. Ton code devient illisible, extrêmement difficile à maintenir et impossible à tester unitairement.

### B. Le problème de la granularité (Un fichier par entreprise)
DuckDB tire sa puissance de la lecture de gros volumes centralisés. Devoir scanner 1 000, 2 000 ou 5 000 micro-fichiers JSON indépendants éparpillés sur le disque force DuckDB à ouvrir et fermer des milliers de descripteurs de fichiers. 
À cette échelle, le coût d'ouverture des fichiers masque totalement le gain de performance du moteur SQL. Ta boucle Python `glob` couplée à une liste de DataFrames en mémoire vive sera tout aussi rapide, voire plus fluide.

### C. La gestion des erreurs et des fichiers corrompus
Si l'API a renvoyé un JSON vide ou malformé pour une entreprise spécifique :
* **Avec ta boucle `for` Pandas :** Tu captures l'exception proprement, tu affiches un `print` d'avertissement, et le script passe au fichier suivant. Ton pipeline est robuste.
* **Avec DuckDB (via une requête globale) :** Si DuckDB rencontre un seul fichier JSON corrompu ou dont la structure dévie légèrement au milieu de ton dossier, toute ta requête SQL échoue d'un coup sec, bloquant l'intégralité du traitement.

---

## 3. Le Véritable Rôle de DuckDB dans ton Pipeline



Pour autant, DuckDB n'est pas à jeter. Son utilisation devient pertinente **uniquement à l'Étape 3 (Le script de filtrage pour le Gold)**, une fois que Pandas a fait le travail difficile de nettoyage et a coulé la donnée dans des fichiers **Parquet** propres en zone Silver.

| Étape du Pipeline | Outil Recommandé | Rôle |
| :--- | :--- | :--- |
| **Étape 2 : Bronze ➔ Silver** | **Pandas + Pathlib** | Idéal pour ouvrir les JSON un par un, extraire les listes imbriquées complexes et structurer proprement les données. |
| **Étape 3 : Silver ➔ Gold** | **DuckDB (SQL)** | Idéal pour exécuter des requêtes de filtrage (ex: `WHERE effectifs > 10`) et des jointures instantanées directement sur les fichiers Parquet sans charger la mémoire de la machine. |

### Conclusion
Vouloir utiliser DuckDB sur du JSON brut imbriqué à l'étape 2, c'est utiliser un avion de chasse pour faire de la livraison de colis en centre-ville : c'est inadapté, complexe, et moins efficace qu'un utilitaire standard (Pandas). Reste sur ta liste de DataFrames, c'est l'état de l'art pour cette tâche.