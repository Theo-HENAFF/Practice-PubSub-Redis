import redis
import csv

r = redis.Redis(port=6379)

# =============================================================================
# Insertion des villes dans la DB
# =============================================================================
with open('ville.csv') as csv_file:
    file = csv.reader(csv_file, delimiter=';')
    for row in file:
        r.geoadd("Ville", float(row[1]), float(row[2]), row[0])

t = r.geodist("Ville", "Amiens", "Abbeville", "km")
print("Distance Amiens-Abbeville : {}km \n".format(t))

# =============================================================================
# Insertion des jobs
# =============================================================================
with open('job.csv') as csv_file:
    file = csv.reader(csv_file, delimiter=';')
    for row in file:
        r.lpush(row[1], row[0].replace(" ", ""))

liste = r.lrange("Amiens", 0, -1)
print("Liste des offres d'emplois sur Amiens\n", liste, "\n")

# =============================================================================
# Les villes à moins de 35km d'Amiens
# =============================================================================
cities35 = r.georadiusbymember("Ville", "Amiens", 35, "km")
print("Villes à moins de 35km d'Amiens \n", cities35, "\n")

# =============================================================================
# Les offres selon un rayon donne
# =============================================================================

rayon = 35
ville_dep = "Amiens"

cities_under_radius = r.georadiusbymember("Ville", ville_dep, rayon, "km")
offres_emplois = []
for city in cities_under_radius:
    liste_emplois = r.lrange(city, 0, -1)
    for emp in liste_emplois:
        offres_emplois.append("Emplois de {} à {}".format(emp.decode("utf-8"), city.decode("utf-8")))

for emp in offres_emplois:
    print(emp)



r.flushdb()
