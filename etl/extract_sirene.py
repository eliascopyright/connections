import requests, json
from transform import Transform
from pathlib import Path
class ApiSirene:
  
 def fetchSirene():
   """"
   Enregistre un fichier JSON réponse d'une requête sur l'API Sirene avec comme paramètre le "siren" donné en entrée
   Entrées : 
    - cfg : configuration, fichier YAML contenant les paths des files à enregistrer
    - siren : siren de l'entreprise
   """
   cfg = Transform.load_cfg()
   df = Transform.transform_connections(cfg)
  
   json_files = Path(cfg['paths']['json'])
   json_files.mkdir(parents = True, exist_ok = True)
  
   df['Siret'] = "Non trouvé"
   df["Tranche_effectif"] = "Non trouvé"
  
   url_api = "https://api.insee.fr/api-sirene/3.11/siret/99319029700016"
  
   HEADERS = {
   "X-INSEE-Api-Key-Integration": "3c6fdbc2-52e4-40e6-afdb-c252e430e617",
   "Content-Type": "application/json",
   "Accept": "application/json"
  }
   params = {
   "siren": "993190297"
  }
   response = requests.get(url_api, headers = HEADERS)
   data = response.json()
   with open(json_files/'entreprises.json', "w") as f:
    json.dump(data, f, indent = 4, ensure_ascii = False)
 
 