"""
Versione per la demo che gestisce un singolo dispositivo.
"""
import os, stat, threading, minimalmodbus, serial, json, sched, time, datetime, atexit, random
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

DEBUG = True
CONF_FILE = './config_file.json'

#MODBUS consts
SERIAL_PORT = '/dev/ttyS0'
BAUDRATE = 9600
BYTESIZE = 8
PARITY = serial.PARITY_NONE
STOP_BITS = 1
MODE = minimalmodbus.MODE_RTU
TIME_OUT_READ = 3
TIME_OUT_WRITE = 5
CLOSE_PORT_AFTER_EACH_CALL = False

REFRESH_RATE = 5 #seconds



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

MANDATORY_FIELDS_SENSOR = ['name', 'address', 'type', 'um']
MANDATORY_FIELDS_SLAVE = ['address', ]



class Handler:
    """
    Il costruttore ha diversi compiti:
    
    - Prova ad aprire una connessione con tutti gli slave rilevati nel file di configurazione.
    - Se è riuscito ad aprire un numero di connessioni diverse da 0, inizia il processo di refresh.
    - Configura una pipe con cui cominicare con Express.
    """
    def __init__(self, slaves):
        #Se nel file di configurazione non vi è elencato nessuno slave chiudo il programma.
        if len(slaves) == 0:
            print('La lista degli slave è vuota.\nControllare il contenuto del file:{}'.format(CONFIG_FILE))
            exit()
        self.slave_instances = []
        for slave in slaves:
            if not self.check_fields(slave, False):
                print("Errore di configurazione in uno degli slave! Controllare {}!".format(CONFIG_FILE))
                continue
            try:
                self.slave_instances.append({'instance' : minimalmodbus.Instrument(SERIAL_PORT, slave['address'], mode = MODE, close_port_after_each_call = CLOSE_PORT_AFTER_EACH_CALL, debug = DEBUG), 'info' : slave})
                index = len(self.slave_instances) - 1
                self.slave_instances[index]['instance'].serial.baudrate = BAUDRATE
                self.slave_instances[index]['instance'].serial.parity = PARITY
                self.slave_instances[index]['instance'].serial.bytesize = BYTESIZE
                self.slave_instances[index]['instance'].serial.stopbits = STOP_BITS
                self.slave_instances[index]['instance'].serial.timeout = TIME_OUT_READ
                self.slave_instances[index]['instance'].serial.write_timeout = TIME_OUT_WRITE
            except Exception as e:
                print('Qualcosa è andato storto mentre cercavo di aprire una connessione con lo slave {}:{}'.format(slave['address'], str(e)))
        if len(self.slave_instances) == 0:
            print("L'apertura della connessione è fallita con tutti gli slave presenti nel file di configurazione.\nTermino il processo.")
            exit()
        
        self.get_influx()
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
                if sensor['to_update'] == 0:
                    continue
                """
                if not self.check_fields(sensor, True):
                    print('Errore di configurazione di un sensore appartenente allo slave {}! Salto la scrittura sul database.'.format(slave['info']['address']))
                """
                address, functioncode, callback = self.get_call_info(slave, sensor)
                try:
                    """
                    if sensor['type'] == HOLDING_REGISTER or sensor['type'] == INPUT_REGISTER:
                        value = callback(address, functioncode = functioncode, number_of_decimals = sensor['decimals'] if 'decimals' in sensor else 0)
                    else:
                        value = callback(address, functioncode = functioncode)
                    """
                    value = random.randrange(0, 1000)
                    data = Point("sensori").tag("slave", str(slave['info']['address'])).tag("sensor", str(sensor['address'])).field("value", value)
                    self.write_api.write(org = "Link", bucket = "sensors", record = data, write_precision = 's')
                    self.write_api.close()
                except Exception as e:
                    print("Qualcosa è andato storto durante la lettura di {} da {}:{}".format(sensor['address'], slave['info']['address'], str(e)))
        scheduler.enter(REFRESH_RATE, 1, self.refresh_values)
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
    Stabilisce una connessione con il Influx.
    Per il momento non controllo che la connessione vada a buon fine.
    """
    def get_influx(self):
        client = InfluxDBClient(url = "http://localhost:8086", token = "KA9HI3YXW5HOS3jOsjkHOqprBLYBQnY0cJJMnFKeXOOvflqUPBVdax4NOHuiIBTFk2dvxxcChrfvosjJ1XEMVw==", org = "Link", debug = True)
        self.write_api = client.write_api(write_options = SYNCHRONOUS)

    """
    Controllo se il file di configurazione ha tutti i campi necessari per la scrittura sul database.
    Parametri:

    - obj: l'oggetto su cui controllare i campi.
    - slave_or_sensor: flag che indica se i stanno controllando i campi di uno slave o di un sensore.
    """
    def check_fields(self, obj, slave_or_sensor):
        if slave_or_sensor:
            for field in MANDATORY_FIELDS_SENSOR:
                if field not in obj:
                    return False
        else:
            for field in MANDATORY_FIELDS_SLAVE:
                if field not in obj:
                    return False
        return True

Handler(SLAVES)