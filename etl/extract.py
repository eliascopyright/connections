import tqdm, codecs, io
from datetime import date
import pandas as pd, yaml, json, tqdm
from pathlib import Path
import requests, time
from transform import Transform
from utilis import Utilis

LOGGER = Utilis.setup_logging("linkedin_pipeline", "linkedin_log")

class EnrichissementEntreprises:
 
 def transform_connections(cfg):
  """
  Transforme les données en catégorisant les postes, sauvegarde le fichier transformé en .csv
  Entrées: - cfg: dictionnaire de configuration contenant les chemins des données.
  Sorties : - fichier .csv transformé avec une nouvelle colonne "category" catégorisant les postes
  
  """
  linkedin_files = Path(cfg['paths']['linkedin_data'])
  df = pd.read_csv(linkedin_files/'Connections.csv', sep=",", skiprows=3)
  df["position2"] = df['Position'].str.lower()
  categories={
   "data analyst" :["bi", "visualisation", "vizualisation", "data viz", "analyst", "analytics", "power bi", "tableau", "reporting","excel"],
   "data engineer" : ["data engineer", "data engineering", "data eng", "gcp", "aws", "azure", "cloud", "bigquery", "snowflake", "databricks"\
          "spark", "hadoop", "airflow", "dbt", "pipeline", "ingénieur de données"],
   "data scientist" : ["data scientist", "data science", "machine learning", "deep learning"],
   "recruiter" : ['recrutement', "acquisition de talents", 'talent acquisition', "ressources humaines", 'recruiter', "recruitment", "recruteuse", "rh", 'hr', 'human resources', 'chasseur de tête', 'chasseuse de tête' \
   'hiring'],
   "developer" : ['developer', 'dev', 'software', 'full stack', 'backend', 'frontend', 'programmer', 'coder', 'ingénieur logiciel'],
   "ceo" : ['ceo', "chef d'entreprise", "président", "directrice", "fondateur", "fondatrice", "directeur", "pdg", 'chief executive officer', 'founder', 'co-founder', 'owner', 'president', 'director'],
   "cybersecurity": ["cybersecurity", "sécurité", "systèmes de sécurité", "cyber security", "cyber"]
   }

  for role, category in categories.items():
   pattern = '|'.join(category)
   df.loc[df['position2'].str.contains(pattern, case = False, na = False), 'category'] = role

   df['Nom prenom'] = df['First Name'] + " " + df['Last Name']

  # Save the file
  Path(cfg['paths']['input']).mkdir(parents = True, exist_ok = True)
  df.to_csv(Path(cfg['paths']['input'])/'connections_transformed.csv', index = False, sep = ";")
  
  return df
 
 def fetchRechercheEntreprises(q: str) -> dict:
   """
   Renvoie la réponse d'une requête à l'API Recherche Entreprises de l'état.
   Entrées:
    - q: str, la recherche qui nous intéresse (exemple: q='la poste')
  Sorties:
   - dict, dictionnaire (fichier JSON obtenu après avoir requêté l'API)
   """
   
   params = {
     "q": q
   }
   url_api = "https://recherche-entreprises.api.gouv.fr/search"
   response = requests.get(url_api, params=params)
   data = response.json()
   return data, q

 def jsonToDf():
   """
   Prend le fichier entreprises.json dans le json Path et renvoie un DataFrame avec les champs du dictionnaire.
   
   
   """
   
   cfg = Transform.load_cfg()
   json_files = Path(cfg['paths']['json'])
   with open(json_files/'entreprises.json', 'r') as f:
    data = json.load(f)
   
   
   new_lines = []
   etab = data.get("etablissement", {})
   uniteLegale = etab.get("uniteLegale", {})
   adresseEtab = etab.get("adresseEtablissement", {})
   dic = EnrichissementEntreprises.dic
   new_line = {}
   for key, function in dic.items():
     new_line[key] = function(etab, uniteLegale, adresseEtab)
   
   new_lines.append(new_line)
   df = EnrichissementEntreprises.jsonToDf()
   df["adresse"] = df['numeroVoieEtablissement'].str.cat(
      [
        df["typeVoieEtablissement"],
        df["libelleVoieEtablissement"]
        ], sep = " ", na_rep = ""
    )
   df = df.drop(["numeroVoieEtablissement", "typeVoieEtablissement", "libelleVoieEtablissement"], axis = 1)
   df = df[['denominationUniteLegale', 'siren', 'siret', 'dateCreationEtablissement','adresse',
    'trancheEffectifsEtablissement', 'dateDernierTraitementEtablissement',
    'etablissementSiege', 'nombrePeriodesEtablissement',
    'activitePrincipaleNAF25Etablissement', 'etatAdministratifUniteLegale',
    'statutDiffusionUniteLegale', 'categorieJuridiqueUniteLegale',
    'activitePrincipaleUniteLegale', 'nicSiegeUniteLegale',
    'complementAdresseEtablissement', 'codePostalEtablissement',
    'libelleCommuneEtablissement', 'code_commune',
    'coordonneeLambertAbscisseEtablissement',
    'coordonneeLambertOrdonneeEtablissement']]

   return pd.DataFrame(new_lines)
     
 def saveDataFrame(df: pd.DataFrame, filename: str):
    """
    Enregistre le dataframe df en entrée dans le chemin 'paths/csv' de la cfg
    Entrées:
      df : pandas.DataFrame à sauvegarder
    """
    filedate = f"{date.today().year}-0{date.today().month}-0{date.today().day}"
    filename = f'{filename}-{filedate}.csv'
   
    cfg = Transform.load_cfg()
    path_save = Path(cfg['paths']['csv'])
    df.to_csv(path_save/filename, sep = ",", index=False)
 
 def saveJSON(filename: str, data: dict):
   """
   Enregistre la réponse de la requête API sous le nom: `filename-date.json`.
   Entrées:
    - filename : nom de l'entreprise
    - data : fichier json réponse de la requête API
   Sorties: aucune, le fichier est enregistré
   """
   try:
    filename = filename.lower().replace(" ", "_")
    filename = filename.replace('"','')
    filename = f"{filename}-{date.today().year}-0{date.today().month}-0{date.today().day}.json"
    cfg = Utilis.load_cfg()
    json_path = Path(cfg["paths"]['bronze_json'])
    json_path.mkdir(exist_ok= True, parents = True)
    
  
    with open(json_path/filename, "w") as f:
       json.dump(data, f, indent = 4, ensure_ascii=True)
   except OSError as e:
     LOGGER.warning(f"Erreur detctée: {e}, Entrées : filename={filename}\n")
     pass
   except AttributeError as e:
     LOGGER.warning(f"Erreur detctée: {e}, Entrées : filename={filename}\n")
     pass

 def extractNAFTable():
   LOGGER.info("Enregistrement de la table NAF....")
   url_api = "https://www.data.gouv.fr/api/1/datasets/r/86b12b06-3eab-4b15-abdf-965d62a54166"
   response = requests.get(url_api,)
   cfg = Utilis.load_cfg()
   BASE_DIR = Path(__file__).parent.parent
   input_path = Path(cfg['paths']['input'])
   input_path.mkdir(exist_ok=True, parents=True)
   input_path_final = BASE_DIR/input_path
   df = pd.read_csv(io.BytesIO(response.content), sep = ';')
   df.to_csv(input_path_final/"naf_table.csv", sep = ";", index = False)
   LOGGER.info(f"Dictionnaire table NAF enregistré avec succès dans {input_path}...")
   return df
 

     

 def evaluateCompanySize():
   params = {
     "q": "total energies"
   }
   url_api = "https://recherche-entreprises.api.gouv.fr/search"
   response = requests.get(url_api, params=params)
   data = response.json()
   results_data = data.get("results", [])
   print(f"Il y a {len(results_data)} résultats dans results")
   
   for n, field in enumerate(results_data):
     dirigeants = len(field.get("dirigeants"))
     activite = field.get("activite_principale_naf25")
     matching_etablissement = len(field.get("matching_etablissements"))
     
     print(f"{n+1}e element : Il y a {len(field)} champs, {dirigeants} dirigeants, {matching_etablissement} établissements, activité: {activite}")
 
 def extractCompaniesInfos(companies_to_extract: pd.DataFrame) -> list:
   """
   Télécharges toutes les datas depuis la liste des entreprises linnkedin et les enregistre au format JSON
   Renvoie la liste de chaque query faite à l'API.
   """
   
   queries = []
   for company in tqdm.tqdm(companies_to_extract):
     data, query = EnrichissementEntreprises.fetchRechercheEntreprises(q=company)
     queries.append(query)
     EnrichissementEntreprises.saveJSON(filename = company, data = data)
   return queries
 
 def saveQueries(queries: list) -> None:
   """
   Enregistre la liste fournie en entrée dans un fichier json simple, pour qu'on l'ouvre comme une liste.
   Le but est de garder les queries faites à l'API pour les réinjecter dans le dataframe final
   Entrées:
    - queries: list, qui sera enregistrée dans le bronze_json.
   Sorties: 
    - None, aucune sortie
   """
   bronze_json = Path(Utilis.load_cfg()['paths']['bronze_json'])
   BASE_DIR = Path(__file__).parent.parent
   LOGGER.info(f"Sauvegarde des queries dans le fichier queries.json")
   with open(BASE_DIR/bronze_json/"queries.json", "w") as file:
     json.dump(queries, file)
   LOGGER.info(f"Sauvegarde des queries faite avec succès")
   
     
if __name__ == "__main__":
  cfg = Utilis.load_cfg()
  df = EnrichissementEntreprises.transform_connections(cfg)
  companies_to_extract = df['Company'][df["category"] == "ceo"].values
  queries = EnrichissementEntreprises.extractCompaniesInfos(companies_to_extract)
  EnrichissementEntreprises.saveQueries(queries)
  EnrichissementEntreprises.extractNAFTable()  
