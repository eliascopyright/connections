import yaml
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


class Utilis:
 
 BASE_DIR = Path(__file__).parent.parent
 
 CONFIG_FILE = BASE_DIR / "config.yaml"

 def load_cfg():
  with open(Utilis.CONFIG_FILE, 'r') as f:
   return yaml.safe_load(f)
  
 def setup_logging( pipeline_name :str, filename: str) -> logging.Logger:
   """
      
   """
   cfg = Utilis.load_cfg()
   logs_path = Path(cfg['paths']['logs'])
   logs_path.mkdir(exist_ok=True, parents =True)
   logger = logging.getLogger(pipeline_name)
   logger.setLevel(logging.DEBUG)
   formatter_file = logging.Formatter("%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s")
   console_handler = logging.StreamHandler()
   console_handler.setLevel(logging.INFO)
   formatter_console = logging.Formatter("%(levelname)s | %(filename)s | %(message)s")
   console_handler.setFormatter(formatter_console)
   if filename.endswith(".log") == False:
     filename = filename + '.log'
   file_handler = RotatingFileHandler(logs_path/filename, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
   file_handler.setLevel(logging.DEBUG)
   file_handler.setFormatter(formatter_file)
   logger.addHandler(console_handler)
   logger.addHandler(file_handler)
   return logger
  
logger = Utilis.setup_logging("linkedin_pipeline", "linkedin_log")
  