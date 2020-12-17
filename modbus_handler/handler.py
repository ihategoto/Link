"""
Versione per la demo che gestisce un singolo dispositivo.
"""
import os, stat, threading, minimalmodbus, serial, json, sched, time, pymongo, datetime

DEBUG = True
CONF_FILE = './config_file.json'

#MODBUS consts
SERIAL_PORT = '/dev/ttyS0'
BAUDRATE = 9600
BYTESIZE = 8
PARITY = serial.PARITY_NONE
STOP_BITS = 1
MODE = minimalmodbus.MODE_RTU
CLOSE_PORT_AFTER_EACH_CALL = False

#MongoDB consts
REFRESH_RATE = 5 #seconds
DB_HOST  = ''
DB_PORT = ''
DB_NAME = 'link'
COLLECTION_NAME = 'sensors'

#Pipe consts
PIPE_NAME = 'write_pipe'
EXIT_ON_ERROR_PIPE = True

#MODBUS data types
BIT = 0
COIL = 1
INPUT_REGISTER = 3
HOLDING_REGISTER = 4

def get_slaves():
    with open(CONF_FILE) as f:
        d = json.load(f)
    return d

SLAVES = get_slaves()

scheduler = sched.scheduler(time.time, time.sleep)


class Handler:
    """
    Il costruttore ha diversi compiti:
    
    - Prova ad aprire una connessione con tutti gli slave rilevati nel file di configurazione.
    - Se è riuscito ad aprire un numero di connessioni diverse da 0, inizia il processo di refresh.
    - Configura una pipe con cui cominicare con Express.
    """
    def __init__(self, slaves, mode, baudrate, bytesize, stop_bits, close_port_after_each_call, debug, refresh_rate):
        #Se nel file di configurazione non vi è elencato nessuno slave chiudo il programma.
        if len(slaves) == 0:
            print('La lista degli slave è vuota.\nControllare il contenuto del file:{}'.format(CONFIG_FILE))
            exit()
        self.refresh_rate = refresh_rate
        self.slave_instances = []
        for slave in slaves:
            try:
                self.slave_instances.append({'instance' : minimalmodbus.Instrument(SERIAL_PORT, slave['address'], mode = mode, close_port_after_each_call = close_port_after_each_call, debug = debug), 'info' : slave})
                index = len(self.slave_instances) - 1
                self.slave_instances[index]['instance'].serial.baudrate = BAUDRATE
                self.slave_instances[index]['instance'].serial.parity = PARITY
                self.slave_instances[index]['instance'].serial.bytesize = BYTESIZE
                self.slave_instances[index]['instance'].serial.stopbits = STOP_BITS
            except Exception as e:
                print('Qualcosa è andato storto mentre cercavo di aprire una connessione con lo slave {}:{}'.format(slave['address'], str(e)))
        if len(self.slave_instances) == 0:
            print("L'apertura della connessione è fallita con tutti gli slave presenti nel file di configurazione.\nTermino il processo.")
            exit()
        
        self.get_mongo()
        self.get_pipe()
        """
        Se si è arrivati fin qui abbiamo almeno uno slave con cui comunicare, dunque comincio il processo di refresh.
        """
        self.refresh_values()
        
    """
    Fa il refresh dei valori presenti in ogni slave classificato come 'to_update' : 1.
    """
    def refresh_values(self):
        post = {}
        for slave in self.slave_instances:
            for sensor in slave['info']['map']:
                #Se il sensore non deve essere aggiornato salto.
                print('Sto scrivendo:{}'.format(sensor['name']))
                if sensor['to_update'] == 0:
                    continue
                address, functioncode, callback = self.get_call_info(slave, sensor)
                try:
                    if sensor['type'] == HOLDING_REGISTER or sensor['type'] == INPUT_REGISTER:
                        value = callback(address, functioncode = functioncode, number_of_decimals = sensor['decimals'] if 'decimals' in sensor else 0)
                    else:
                        value = callback(address, functioncode = functioncode)
                    if 'sensors' not in post:
                        post = {
                            'date' : datetime.datetime.utcnow(),
                            'slave' : slave['info']['address'],
                            'sensors' : [
                                {
                                'name' : sensor['name'],
                                'address' : sensor['address'],
                                'type' : sensor['type'],
                                'um' : sensor['um'],
                                'value' : value
                                }
                                ],
                        }
                    else:
                        post['sensors'].append({
                            'name' : sensor['name'],
                            'address' : sensor['address'],
                            'type' : sensor['type'],
                            'um' : sensor['um'],
                            'value' : value
                        })
                except Exception as e:
                    print("Qualcosa è andato storto durante la lettura di {} da {}:{}".format(sensor['address'], slave['info']['address'], str(e)))
        self.collection.insert_one(post)
        scheduler.enter(5, 1, self.refresh_values)
        scheduler.run()
        
    """
    Ritorna l'indirizzo relativo, il functioncode adatto al sensore e la funzione corretta di minimalmodbus.
    """
    def get_call_info(self, slave, sensor):
        if sensor['type'] == BIT:
            address = sensor['address'] - 10000
            functioncode = 2
            callback = slave['instance'].read_bit
        elif sensor['type'] == INPUT_REGISTER:
            address = sensor['address'] - 30000
            functioncode = 4
            callback = slave['instance'].read_register
        elif sensor['type'] == HOLDING_REGISTER:
            address = sensor['address'] - 40000
            functioncode = 3
            callback = slave['instance'].read_register
        else:
            #L'indirizzo relativo delle coil coincide con quello fisico.
            address = sensor['address']
            functioncode = 1
            callback = slave['instance'].read_bit
            
        return address, functioncode, callback
        
    """
    Cerca di aprire una connesione con MongoDB.
    """
    def get_mongo(self):
        try:
            if DB_HOST == '' and DB_PORT == '':
                client = pymongo.MongoClient()
            else:
                client = pymongo.MongoClient(DB_HOST, DB_PORT)
        except pymongo.ConnectionFailure as e:
            print('Qualcosa è andato storto nella connessione con il database:{}'.format(str(e)))
            exit()
        self.db = client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]

    """
    Apre una named-pipe con il server node.
    """
    def get_pipe(self):
        print('Genero il thread che gestisce la pipe...')
        t = threading.Thread(target = self.handle_pipe, args=())
        t.daemon = True
        t.start()

    def handle_pipe(self):
        if not stat.S_ISFIFO(os.stat(PIPE_NAME).st_mode):
            # Creo la pipe
            try:
                os.mkfifo(PIPE_NAME)
            except Exception as e:
                print("Qualcosa è andato storto nell'apertura della pipe:{}".format(e))

        while True:
            try:
                fifo = open(PIPE_NAME, 'r')
                while True:
                    data = fifo.read()
                    if len(data) == 0:
                        break
                    """
                    Data: slave_address,coil_address,value
                    """
                    decoded_data = data.split(',')
                    try:
                        slave = minimalmodbus.Instrument(SERIAL_PORT, int(decoded_data[0]))
                        slave.write_bit(int(decoded_data[1]), int(decoded_data[2]), functioncode=5)
                        print("Scrittura riuscita!")
                    except Exception as e:
                        print("Qualcosa è andato storto durante la scrittura su uno slave modbus:{}".format(str(e)))

            except Exception:
                print("Qualcosa è andato storto durante l'apertura della pipe!")

Handler(SLAVES, MODE, BAUDRATE, BYTESIZE, STOP_BITS, CLOSE_PORT_AFTER_EACH_CALL, DEBUG, REFRESH_RATE)