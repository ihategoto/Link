'''
Il programma genera casualmente dei document per la collection "nodes" del db.
Lo schema generato sara' identico allo schema definito in "nodo.js".

NOTA: i sensori potrebbero avere dei duplicati

#Definisco schema del documento
document = {
    "date": date,
    "slave": slave_counter,
    "sensors": [{
        "name": name,
        "address": sensor_counter,
        "value": value
    }]
}

'''
#Importo librerie
import datetime
import json
import time
import random

# ***Funzioni***
#Funzione per generare un sensore casuale
#-primo parametro: j (ovvero l'indice del nodo generato)
#--secondo parametro: sensor_names (lista dei possibili nomi per i sensori)
#---terzo parametro: sensors_range (lista dei possibili range per i valori dei sensori)
#----quarto parametro: lista dei dizionari dei sensori
def sensorGenerator(j, sensor_names, sensors_range, a):
    #Dichiaro dizionario per i nodi
    sensor = dict()

    #Genero un indice casuale per cercare: nome e valore
    rand = random.randrange(0, len(sensor_names))

    #Controllo che non esista gia il sensore
    while any(d["name"] == sensor_names[rand] for d in a):
        rand = random.randrange(0, len(sensor_names))

    rand_value = random.randrange(sensors_range[rand][0], sensors_range[rand][1])
    #Aggiungo gli item di seguito 
    sensor["name"] = sensor_names[rand]
    sensor["address"] = j
    sensor["value"] = rand_value
    return sensor

#Funzione di stampa per i dizionari
def prettierDocumentPrinter(d):
    print("Date:",d["date"])
    print("Slave:",d["slave"])
    print("Sensori:")
    for i in range(len(d["sensors"])):
        print("----------------------------")
        print("----> Name:",sensors[i]["name"])
        print("----> Address:",sensors[i]["address"])
        print("----> Value:",sensors[i]["value"])
    print()
        
    
#Definisco possibili variabili del sensore
#Lista dei nomi
sensor_names = [
    "val_CO",
    "val_eCO2",
    "val_TVCO",
    "val_PM1_0",
    "val_PM2_5",
    "val_PM4",
    "val_PM10",
    "val_PD",
    "val_T",
    "val_Umi",
    "val_O3",
    "val_H2O2"
]

#Lista dei range dei valori
sensors_range = [
    [0, 400],
    [450, 2000],
    [125, 600],
    [0, 1000],
    [0, 1000],
    [0, 1000],
    [0, 1000],
    [1, 30],
    [15, 50],
    [0, 100],
    [0, 10],
    [0, 2000]
]

#Lista dei documenti
nodesCollecion = list()

#Stampa di prova
for i in range(len(sensor_names)):
    print("Sensore:",sensor_names[i],"- Range","(",sensors_range[i][0],",",sensors_range[i][1],")")

#Definisco una data che vado ad incrementare ad ogni richiamo di 5 sec
date = datetime.datetime(2020, 12, 1, 16, 00, 00)
print(date.strftime("%c")) # Tue Dec  1 16:00:00 2020
print(date)                # 2020-12-01 16:00:00

#Chiedo in input quanti nodi bisogna generare
print()
print()
tot_nodi = int(input("Inserisci il numero di nodi da generare: "))

#Definico i secondi necessari per un nuovo aggiornamento
WAIT = 5

#Ciclo all'infinito
while(True):
    #Ciclo per il numero prefissato di nodi presi in input
    for i in range(tot_nodi):
        #Con timedelta posso selezionare in modo dinamico quale variabile modificare
        date += datetime.timedelta(seconds=5) 
        #Indice di slave che parte da 1
        slave_counter = i+1

        #Decido quanti sensori generare da 1 a 12 (0 non aveva senso generarlo)
        n_sensors = random.randrange(1,13) #da 1 a 12

        #Inizializzo una lista di sensori
        sensors = list()

        #Ciclo per i vari sensori
        for j in range(n_sensors):
            #Genero e aggiungo il sensore casuale alla lista di sensori
            sensors.append(sensorGenerator(j, sensor_names, sensors_range, sensors))
        
        #Costruisco il documento
        document = dict()
        document["date"] = date
        document["slave"] = slave_counter
        document["sensors"] = sensors

        #(non necessario) Salvo i documenti in una lista
        nodesCollecion.append(document)

        # Stampo il risultato
        #print(document)
        prettierDocumentPrinter(document)

    #Aspetto WAIT secondi per generare nuovi dati
    time.sleep(WAIT)

#Converto da dizionario(python) a stringa(json)
'''doc_json = json.dump(document)
print(doc_json)'''
