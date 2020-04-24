import redis as r
import csv


redis = r.Redis(port=6379)
# =============================================================================
# Insertion des villes dans la DB
# =============================================================================
with open('ville.csv') as csv_file:
    file = csv.reader(csv_file, delimiter=';')
    for row in file:
        redis.geoadd("Ville", float(row[1]), float(row[2]), row[0])

# =============================================================================
# Insertion des jobs
# =============================================================================
with open('job.csv') as csv_file:
    file = csv.reader(csv_file, delimiter=';')
    for row in file:
        redis.lpush(row[1], row[0].replace(" ", ""))

# =============================================================================
# Les offres selon un rayon + publication
# =============================================================================
rayon = 35
ville_dep = "Amiens"
channel = 'Emplois_{}km_{}'.format(rayon, ville_dep)  # Emplois_35km_Amiens

employeur = r.Redis(host='localhost', port=6379, db=0)

# On recherche les emplois à 35km d'Amiens
cities_under_radius = redis.georadiusbymember("Ville", ville_dep, rayon, "km")
for city in cities_under_radius:
    liste_emplois = redis.lrange(city, 0, -1)
    for emp in liste_emplois:
        emp = "Emplois de {} a {}".format(emp.decode("utf-8"), city.decode("utf-8"))

        # On publie ces emplois
        employeur.publish(channel, emp)


redis.flushdb()
