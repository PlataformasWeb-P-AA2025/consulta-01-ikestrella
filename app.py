import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["canadianC"]

archivos_a_procesar = [
    {"ruta": 'data/2022.xlsx', "coleccion": "datos_2022"},
    {"ruta": 'data/2023.xlsx', "coleccion": "datos_2023"}
]

for item_archivo in archivos_a_procesar:
    dataframe_excel = pd.read_excel(item_archivo["ruta"])
    lista_registros = dataframe_excel.to_dict(orient='records')
    
    coleccion_mongo = db[item_archivo["coleccion"]]
    coleccion_mongo.delete_many({})
    coleccion_mongo.insert_many(lista_registros)


partidos_filtrados = db["datos_2023"].find({
    "Tournament": "Canadian Open",
    "Court": "Outdoor",
    "Surface": "Hard",
    "Comment": "Completed",
    "LRank": {"$lte": 20}
})

# Lista de todos los jugadores que ganaron al menos un partido en el Canadian Open 2023.
ganadores = db["datos_2023"].distinct("Winner", {
    "Tournament": "Canadian Open",
    "Comment": "Completed"
})

print("Jugadores que ganaron al menos un partido en Canadian Open 2023:")
for jugador in ganadores:
    print("-", jugador)





# Número total de partidos jugados por ronda en el torneo Canadian Open 2022.
rondas = db["datos_2022"].aggregate([
    {"$match": {"Tournament": "Canadian Open"}},
    {"$group": {"_id": "$Round", "total_partidos": {"$sum": 1}}},
    {"$sort": {"_id": 1}} 
])

print("\nCantidad de partidos por ronda en Canadian Open 2022:")
for ronda in rondas:
    print(f"{ronda['_id']}: {ronda['total_partidos']} partidos")