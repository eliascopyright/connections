import pandas as pd, re
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pathlib import Path
import json, tqdm
from utilis import Utilis
from extract import EnrichissementEntreprises

LOGGER = Utilis.setup_logging("linkedin_pipeline", "linkedin_log")
CFG = Utilis.load_cfg()

class Transform:
 dic = {
    "denominationUniteLegale":lambda p,c1, c2: c1.get("denominationUniteLegale"),
    "siren": lambda p,c1, c2: p.get("siren"),
    "siret":lambda p,c1, c2: p.get("siren"),
    "dateCreationEtablissement":lambda p,c1, c2: p.get("dateCreationEtablissement"),
    "trancheEffectifsEtablissement":lambda p,c1, c2: p.get("trancheEffectifsEtablissement"),
    "dateDernierTraitementEtablissement":lambda p,c1, c2: p.get("dateDernierTraitementEtablissement"),
    "etablissementSiege":lambda p,c1, c2: p.get("etablissementSiege"),
    "nombrePeriodesEtablissement":lambda p,c1, c2: p.get("nombrePeriodesEtablissement"),
    "activitePrincipaleNAF25Etablissement":lambda p,c1, c2: p.get("activitePrincipaleNAF25Etablissement"),
    "etatAdministratifUniteLegale": lambda p,c1, c2: c1.get("etatAdministratifUniteLegale"),
    "statutDiffusionUniteLegale":lambda p,c1, c2: c1.get("statutDiffusionUniteLegale"),
    "categorieJuridiqueUniteLegale":lambda p,c1, c2: c1.get("categorieJuridiqueUniteLegale"),
    "activitePrincipaleUniteLegale":lambda p,c1, c2: c1.get("activitePrincipaleUniteLegale"),
    "nicSiegeUniteLegale":lambda p,c1, c2: c1.get("nicSiegeUniteLegale"),
    "complementAdresseEtablissement": lambda p,c1,c2: c2.get("complementAdresseEtablissement"),
    "numeroVoieEtablissement":lambda p,c1,c2: c2.get("numeroVoieEtablissement"),
    "typeVoieEtablissement":lambda p,c1,c2: c2.get("typeVoieEtablissement"),
    "libelleVoieEtablissement":lambda p,c1,c2: c2.get("libelleVoieEtablissement"),
    "codePostalEtablissement":lambda p,c1,c2: c2.get("codePostalEtablissement"),
    "libelleCommuneEtablissement":lambda p,c1,c2: c2.get("libelleCommuneEtablissement"),
    "code_commune":lambda p,c1,c2: c2.get("codeCommuneEtablissement"),
    "coordonneeLambertAbscisseEtablissement":lambda p,c1,c2: c2.get("coordonneeLambertAbscisseEtablissement"),
    "coordonneeLambertOrdonneeEtablissement":lambda p,c1,c2: c2.get("coordonneeLambertOrdonneeEtablissement")
   }
 
 DIC_RECHERCHE_ENTREPRISES = {
    "Nom entreprise": lambda p,s: p.get("nom_complet"),
    "siren": lambda p,s: p.get("siren"),
    "siret": lambda p,s: s.get("siret"),
    "nb_etablissements": lambda p,s: p.get("nombre_etablissements"),
    "nb_etab_ouverts": lambda p,s: p.get("nombre_etablissements_ouverts"),
    "activite_principale": lambda p,s: s.get("activite_principale"),
    "activite_principale_naf25": lambda p,s: s.get("activite_principale_naf25"),
    "annee_tranche_effectif_salarie": lambda p,s: s.get("annee_tranche_effectif_salarie"),
    "siege_adresse": lambda p,s: s.get("adresse"),
    "code_postal": lambda p,s: s.get("code_postal"),
    "commune": lambda p,s: s.get("commune"),
    "caractere_employeur": lambda p,s: s.get("caractere_employeur"),
    "coordonnees": lambda p,s: s.get("coordonnees"),
    "date_creation": lambda p,s: s.get("date_creation"),
    "date_debut_activite": lambda p,s: s.get("date_debut_activite"),
    "latitude": lambda p,s: s.get("latitude"),
    "longitude": lambda p,s: s.get("longitude"),
    "etat_administratif": lambda p,s: p.get("etat_administratif"),
    "nature_juridique": lambda p,s: p.get("nature_juridique"),
    "tranche_effectif_salarie": lambda p,s: p.get("tranche_effectif_salarie"),
    "section_activite_principale": lambda p,s: p.get("section_activite_principale"),
    "tranche_effectif_salarie": lambda p,s: s.get("tranche_effectif_salarie"),
    "adresse": lambda p,s: f"{s.get("numero_voie")} {s.get("type_voie")} {s.get("libelle_voie")}".strip(),
    "annee_tranche_effectif_salarie": lambda p,s: s.get("annee_tranche_effectif_salarie"),
    "finances_ca": lambda p,s: p.get("finances", {}).get("2024", {}).get("ca") if p.get("finances",{}) !=None else 0,
    "resultat_net": lambda p,s: p.get("finances", {}).get("2024", {}).get("resultat_net") if p.get("finances",{}) !=None else 0
  }
 
 DIC_DIRIGEANTS = {
  "nom_complet": lambda p,s,d: p.get("nom_complet"),
  "siret": lambda p,s,d: s.get("siret") if p.get("siege", {}) !=None else "Pas de siege renseigne",
"Nom prénom": lambda p,s,d: f"{d.get("nom")} {d.get("prenoms")}" if d.get("prenoms") != None else "Non renseigné",
  "Date_de_naissance": lambda p,s,d: f"{d.get("date_de_naissance")}"if d.get("date_de_naissance") != None else "Non renseigné",
  "Qualite": lambda p,s,d: d.get("qualite")if d.get("qualite") != None else "Non renseigné",
  "nationalite": lambda p,s,d: d.get("nationalite")if d.get("nationalite") != None else "Non renseigné",
  "type_dirigeant": lambda p,s,d: d.get("type_dirigeant") if d.get("type_dirigeant") != None else "Non renseigné",
  "siren": lambda p,s,d: d.get("siren") if d.get("siren")!=None else "Non renseigné",
  "denomination": lambda p,s,d: d.get("denomination") if d.get("denomination") is not None else "Non renseigné",
  
  }
 
 DIC_ETAB = {
   "nom_complet": lambda p,e: p.get("nom_complet"),
   "siren": lambda p,e: p.get("siren"),
   "siret": lambda p,e: e.get("siret"),
   "tranche_effectif_salarie": lambda p,e: p.get("tranche_effectif_salarie"),
   "activite_principale": lambda p,e: e.get("activite_principale"),
   "activite_principale_naf25": lambda p,e: e.get("activite_principale_naf25"),
   "ancien_siege": lambda p,e: e.get("ancien_siege"),
   "annee_tranche_effectif_salarie": lambda p,e: p.get("siren"),
   "adresse": lambda p,e: e.get("adresse"),
   "code_postal": lambda p,e: e.get("code_postal"),
   "commune": lambda p,e: e.get("commune"),
   "date_creation": lambda p,e: p.get("date_creation"),
   "date_debut_activite":lambda p,e: e.get("date_debut_activite"),
   "date_fermeture": lambda p,e: e.get("date_fermeture"),
   "etat_administratif": lambda p,e: e.get("etat_administratif"),
   "latitude": lambda p,e: e.get("latitude"),
   "longitude": lambda p,e: e.get("longitude")
 }
 
 def openJson(filename : str):
   with open(filename, "r") as f:
     data = json.load(f)
   return data
 
 def getCompanyInfo(results: list) -> pd.DataFrame:
   """
   Prend en entrée un dictionnaire extrait de l'API du gouvernement en json, et renvoie un dataframe
   Entrées:
    - data: dict avec un champ results[]....
  Sorties:
   - pd.dataFrame
   """
   dic = Transform.DIC_RECHERCHE_ENTREPRISES
   new_lines = []
   for line in results:
     new_row = {}  
     siege = line.get("siege",{})
     for key, func in dic.items():
      new_row[key] = func(line, siege)
      if (key == "finances_ca") &\
      (type(func(line,siege))!=str) &\
        (func(line, siege) != None)&\
          (type(func(line, siege)) != int):
        LOGGER.info(f"key = {key}, finances_ca = {func(line, siege)}")
     new_lines.append(new_row)
   return pd.DataFrame(new_lines)
 
 def getDirigeants(results: list) -> pd.DataFrame:
  """"
  On prend la réponse à l'API recherche entreprise, et on extrait le siret
  pour en tirer les dirigeants.
  """
  dic = Transform.DIC_DIRIGEANTS
  new_lines = []
  for line in results:
    dirigeants = line.get("dirigeants", [])
    siege = line.get("siege", {})
    if dirigeants == []: continue
    for d in dirigeants:
      new_row = {}
      
      for key, func in dic.items():
        new_row[key] = func(line, siege, d)
    
      new_lines.append(new_row)
  return pd.DataFrame(new_lines)

 def getEtablissements(results: list) -> pd.DataFrame:
   dic = Transform.DIC_ETAB
   new_lines = []
   for line in results:
     etablissements = line.get("matching_etablissements",[])
     if etablissements == []: continue
     for etab in etablissements:
      new_row = {}
      for key, func in dic.items():
         new_row[key] = func(line, etab)
     new_lines.append(new_row)
      
   return pd.DataFrame(new_lines)
 
 def saveParquet(df: pd.DataFrame, filename: str):
   print(f'Chargement du fichier {filename}...')
   cfg = Utilis.load_cfg()
   path_parquet = Path(cfg["paths"]["silver_parquet"])
   path_parquet.mkdir(exist_ok=True, parents = True)
   if not filename.endswith('.parquet'):
     filename = filename + ".parquet"
   
   try:
    df.to_parquet(path_parquet/filename)
    LOGGER.info(f"Fichier parquet {filename} enregistré avec succès")
   except pa.lib.ArrowTypeError as e:
     LOGGER.warning(f"Erreur dans la sauvegarde du fichier parquet {e}")
     LOGGER.warning(f"Suppression des colonnes `finances_ca` et `resultat_net` {e}")
     df_tosave = df.drop(["finances_ca", "resultat_net"], axis = 1)
     df_tosave.to_parquet(path_parquet/filename)
     df_problemes = df[['finances_ca', 'resultat_net']]
     df_problemes.to_csv(path_parquet/"df_problemes.csv", sep=';', index=False)
  #  df_pa = pa.Table.from_pandas(df)
  #  pq.write_table(df_pa, path_parquet/filename)

 def compiledata():
   """La fonction renvoie 4 listes:
   Sorties:
   - companies: liste des dataframes avec les infos des entreprises
   - dirigeants: liste des dataframes avec les infos des dirigeants des entreprises
   - etablissements: liste des dataframes avec les infos des etabli....
   - no_results: liste des fichiers dont l'api n'a rien trouvé"""
   LOGGER.info(f"Début d'extraction des fichiers bronze")
   cfg = Utilis.load_cfg()
   BASE_DIR = Path(__file__).parent.parent
   bronze_path = Path(cfg['paths']['bronze_json'])
   bronze_path = BASE_DIR/bronze_path
   companies, etablissements, dirigeants = [], [], []
   no_results = []  
   queries = EnrichissementEntreprises.extractCompaniesInfos()
   for file in tqdm.tqdm(bronze_path.glob("*.json")):
     data = Transform.openJson(file)
     results = data.get("results", [])
     if results == []:
       LOGGER.debug(f"Le fichier bronze {file.stem} est vide....")
       no_results.append(file)
       continue
     LOGGER.debug(f"Le fichier bronze {file.stem} est non vide, extraction des datas...")
     df_company = Transform.getCompanyInfo(results)
     companies.append(df_company)
     df_dirigeants = Transform.getDirigeants(results)
     dirigeants.append(df_dirigeants)
     df_etablissement = Transform.getEtablissements(results)
     etablissements.append(df_etablissement)
   
   df_companies = pd.concat(companies, ignore_index=True)
   df_dirigeants = pd.concat(dirigeants, ignore_index=True)
   df_etablissements = pd.concat(etablissements, ignore_index=True)
   LOGGER.info(f"Tous les fichiers bronze ont été extraits avec succès....")
   return df_companies, df_dirigeants, df_etablissements, no_results
 
 def deleteEmojis(filename):
   input = Path(CFG['paths']['input'])
   BASE_DIR = Path(__file__).parent.parent
   input_path = BASE_DIR/input
   LOGGER.info(f'Ouverture du fichier {filename} pour suppression des emojis...')
   df = pd.read_csv(input_path/filename, sep = ";", encoding='utf-8')
   df["nom_prenom"] = df['Nom prenom'].str.strip()
   df['nom_prenom'] = df["Nom prenom"].str.upper()
   df["nom_prenom"] = (
     df["nom_prenom"]
     .str.replace(r'[^\w\s,-]', "", regex=True) # Enlève tout ce qui n'est pas lettre/chiffre/espace
     .str.strip()
   )
   df['Company'] = df['Company'].str.upper()
   df['Company'] = df['Company'].str.strip()
   return df
 
if __name__ == "__main__":
  companies, dirigeants, etablissements, no_results = Transform.compiledata()
  Transform.deleteEmojis("connections_transformed.csv")
  Transform.saveParquet(companies, "companies")
  Transform.saveParquet(dirigeants, "dirigeants")
  Transform.saveParquet(etablissements, "etablissements")
  
  