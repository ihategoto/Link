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

#InfluxDB
URL_INFLUX = "http://localhost:8086"
TOKEN_INFLUX = "KA9HI3YXW5HOS3jOsjkHOqprBLYBQnY0cJJMnFKeXOOvflqUPBVdax4NOHuiIBTFk2dvxxcChrfvosjJ1XEMVw=="
ORGANIZATION_INFLUX = "Link"
BUCKET_INFLUX = "sensors"

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

class InvalidRegister(minimalmodbus.ModbusException):
    pass

class Handler:
    def __init__(self, slaves):
        #Se nel file di configurazione non vi è elencato nessuno slave chiudo il processo.
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
    Fa il refresh dei valori presenti in ogni slave classificato come 'to_update' : 1.
    """
    def refresh_values(self):
        for slave in self.slave_instances:
            for sensor in slave['info']['map']:
                #Se il sensore non deve essere aggiornato salto.
                if sensor['to_update'] == 0:
                    continue
                if not self.check_fields(sensor, True):
                    print('Errore di configurazione di un sensore appartenente allo slave {}! Salto la scrittura sul database.'.format(slave['info']['address']))
                    continue                
                address, functioncode, callback = self.get_call_info(slave, sensor)
                try:
                    """
                    if sensor['type'] == HOLDING_REGISTER or sensor['type'] == INPUT_REGISTER:
                        value = callback(address, functioncode = functioncode, number_of_decimals = sensor['decimals'] if 'decimals' in sensor else 0)
                    else:
                        value = callback(address, functioncode = functioncode)
                    """
                    value = random.randrange(0, 1000)
                    data = Point("sensori").tag("slave", str(slave['info']['address'])).tag("sensor", str(sensor['address'])).field("value", value).field("um", sensor['um'])
                    self.write_api.write(org = ORGANIZATION_INFLUX, bucket = BUCKET_INFLUX, record = data, write_precision = 's')
                    self.write_api.close()
                except Exception as e:
                    print("Qualcosa è andato storto durante la lettura di {} da {}:{}".format(sensor['address'], slave['info']['address'], str(e)))
        
    """
    Ritorna l'indirizzo relativo, il functioncode adatto al sensore e la funzione corretta di minimalmodbus.
    
    - slave: dict contenente tutte le informazioni riguardanti il sensore di cui si vuole scrivere sul database.
    - sensor: dict contenente sia l'oggetto minimalmodbus.Instrument che i metadati inerenti allo slave.
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
    Stabilisce una connessione con il server Influx.
    """
    def get_influx(self):
        try:
            client = InfluxDBClient(url = URL_INFLUX, token = TOKEN_INFLUX, org = ORGANIZATION_INFLUX, debug = DEBUG)
        except Exception as e:
            print("Qualcosa è andato storto durante l'apertura della connessione con InfluxDB: {}".format(str(e)))
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

    """
    Metodo statico utilizzato per scrivere su un sensore attraverso modbus.
    Non avendo particolari informazioni riguardanti il sensore stesso, si dovranno effettuare tutti
    i controlli del caso per verificare la buona riuscita dell'operazione.
    """
    @staticmethod
    def write(slave, sensor, value, decimals = 0):
        try:
            s = minimalmodbus.Instrument(SERIAL_PORT, slave, mode = MODE, close_port_after_each_call = CLOSE_PORT_AFTER_EACH_CALL, debug = DEBUG)
        except Exception:
            raise
        if sensor >= 0 and sensor <= 9998:
            #Coil
            if value != 0 and value != 1 and value is not True and value is not False:
                raise ValueError("Dati non validi per una coil!")
            try:
                s.write_bit(sensor, value)
            except Exception :
                raise
        elif sensor >= 40000 and sensor <= 49998:
            #Holding register
            try:
                s.write_register(sensor, value, number_of_decimals = decimals)
            except Exception:
                raise
        else:
            raise InvalidRegister

class RefreshThread(object):

    def __init__(self, instance):
        self.instance = instance
        thread = threading.Thread(target = self.run, args = ())
        thread.daemon = False
        thread.start()

    def run(self):
        while True:
            now = int(time.time())
            self.instance.refresh_values()
            time.sleep(5-(int(time.time())-now) if 5-(int(time.time())-now) >= 0 else 0)


h = Handler(SLAVES)
RefreshThread(h)