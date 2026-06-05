
import json

L = [
	"entreprise 1",
 "Entreprise 2",
 "entreprise 3",
 "entreprise 4"
]

with open("test.json", "w") as f:
 json.dump(L, f)
 
with open("test.json", "r") as f:
 data = json.load(f)
 

