import polars as pl
import pandas as pd, json, yaml
from pathlib import Path
from utilis import Utilis
from transform import Transform
LOGGER = Utilis.setup_logging("linkedin_pipeline", "linkedin_log")
BASE_DIR = Path(__file__).parent.parent
CFG = Utilis.load_cfg()


class Load():
 
 DIC_EFFECTIFS = {
	"NN" : "Unités non employeuses",
 "00" : "0 salarié",
 "01" : "1 ou 2 salariés",
 "02" : "3 à 5 salariés",
 "03" : "6 à 9 salariés",
 "11" : "10 à 19 salariés",
 "12" : "20 à 49 salariés",
 "21" : "50 à 99 salariés",
 "22" : "100 à 199 salariés",
 "31" : "200 à 249 salariés",
 "32" : "250 à 499 salariés",
 "41" : "500 à 999 salariés",
 "42" : "1 000 à 1 999 salariés",
 "51" : "2 000 à 4 999 salariés",
 "52" : "5 000 à 9 999 salariés",
 "53" : "10 000 salariés et plus"
  
	}
 
 def load_parquet(filename: str) -> pl.DataFrame:
  """
  Charge un fichier parquet avec polars et renvoie un dataFrame
  """
  silver_parquet = Path(CFG['paths']['silver_parquet'])
  silver_parquet = BASE_DIR/silver_parquet
  LOGGER.info(f"Ouverture de {filename} pour gold...")
  
  parquet_path = Path(CFG['paths']['silver_parquet'])
  return pl.read_parquet(parquet_path/filename)
  
 def saveGold(df: pl.DataFrame, filename :str):
  """"
  Sauvegarde le dataframe
  """
  LOGGER.info("Fichier parquet ouvert, dataframe chargé")
  gold_path = Path(CFG['paths']["gold_csv"])
  gold_path.mkdir(exist_ok= True, parents=True)
  if filename.endswith(".csv") == False:
   filename = filename + '.csv'
  df.write_csv(gold_path/filename)
  LOGGER.info(f"Fichier gold {filename} enregistré dans {gold_path}, {len(df)} entreprises")
 
 def filterPME(df: pl.DataFrame):
   effectifs_acceptes = (["NN", "01", "02", "03", "11", "12"])
   df_gold = df.filter(
	 		pl.col("tranche_effectif_salarie").is_in(effectifs_acceptes)
	 	)
   return df_gold
 
 def filterSiret(df_companies: pl.DataFrame, etablissements: pl.DataFrame) -> pl.DataFrame:
  """
  Filtre le Dataframe etablissements sur la colonne `siret` des entreprises fournies en entrée.
  Renvoie un DataFrame polars filtré sur les sirets de `df_companies`.
  Entrées:
   - df_companies: pl.DataFrame des datas des entreprises
   - a_filtrer: pl.DataFrame des datas à filtrer sur le siret (établissements ou dirigeants)
  """
  siret = df_companies.select("siret")
  etablissements_filtres = etablissements.filter(
    pl.col("siret").is_in(companies["siret"])
  )
  return etablissements_filtres
  
 
if __name__ == "__main__":
 
 companies = Load.load_parquet("companies.parquet")
 companies = Load.filterPME(companies)
 Load.saveGold(companies, "companies.csv")

 dirigeants = Load.load_parquet("dirigeants.parquet")
 dirirgeants_filtres = (Load.filterSiret(companies, dirigeants))
 Load.saveGold(dirirgeants_filtres, "dirigeants_filtres.csv")
 connections_transformed = pl.from_pandas(data = Transform.deleteEmojis("connections_transformed.csv"))
 Load.saveGold(connections_transformed, "connections_transformed.csv")
 

 