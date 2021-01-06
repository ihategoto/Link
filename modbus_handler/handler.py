import os, threading, minimalmodbus, serial, json, time, datetime, atexit, logging
from pystalk import BeanstalkClient, BeanstalkError

DEBUG = False
CONF_FILE = 'config_file.json'
DAEMON_LOG_FILE = "daemon.log"

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

#BEANSTALKD consts
BEANSTALKD_HOST = '127.0.0.1'
BEANSTALKD_PORT = 11300
OUTPUT_TUBE = 'data'
INPUT_TUBE = 'commands'

#MODBUS data types
BIT = 0
COIL = 1
INPUT_REGISTER = 3
HOLDING_REGISTER = 4

def get_slaves():
    with open(CONF_FILE) as f:
        d = json.load(f)
    return d

MANDATORY_FIELDS_SENSOR = ['name', 'address', 'type']
MANDATORY_FIELDS_SLAVE = ['address', ]

class InvalidValue(ValueError):
    pass

class InvalidRegister(minimalmodbus.ModbusException):
    pass

class Handler:
    def __init__(self):
        slaves = get_slaves()
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
            except ValueError as e:
                print('Uno o più parametri indicati per la linea seriale non è valido:{}'.format(e))
            except serial.SerialException as e:
                print('Impossibile contattare lo slave {}: {}'.format(slave['address'], e))
        
        if len(self.slave_instances) == 0:
            print("L'apertura della connessione è fallita con tutti gli slave presenti nel file di configurazione.\nTermino il processo.")
            exit()
        
        self.get_beanstalk()
        
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
                    if sensor['type'] == HOLDING_REGISTER or sensor['type'] == INPUT_REGISTER:
                        value = callback(address, functioncode = functioncode, number_of_decimals = sensor['decimals'] if 'decimals' in sensor else 0)
                    else:
                        value = callback(address, functioncode = functioncode)
                    data = {'slave' : slave['info']['address'], 'sensor' : sensor['address'], 'timestamp' : time.time(), "value" : value}
                    self.client.put_job(json.dumps(data))
                except (ValueError, TypeError) as e:
                    print("Qualcosa è andato storto durante la lettura di {} da {}:{}".format(sensor['address'], slave['info']['address'], e))
                except minimalmodbus.ModbusException as e:
                    print("Errore MODBUS durante la lettura di {} da {}: {}".format(sensor['address'], slave['info']['address'], e))
                except BeanstalkError as e:
                    print("Impossibile scrivere sul server BeansTalk il contenuto del sensore {} dello slave {}: {}".format(sensor['address'], slave['info']['address'], e))

    """
    Ritorna l'indirizzo relativo, il functioncode adatto al sensore e la funzione corretta di minimalmodbus.
    
    - slave: dict contenente tutte le informazioni riguardanti il sensore di cui si vuole scrivere sul database.
    - sensor: dict contenente sia l'oggetto minimalmodbus.Instrument che i metadati inerenti allo slave.
    
    Ritorna: indirizzo relativo del sensore, il functioncode per l'operazione richiesta e la funzione adeguata per eseguirla.
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
    Controllo se il file di configurazione ha tutti i campi necessari per la scrittura sul database.
    Parametri:

    - obj: l'oggetto su cui controllare i campi.
    - slave_or_sensor: flag che indica se si stanno controllando i campi di uno slave o di un sensore.
    
    Ritorna True se l'oggetto contiene tutti campi necessari, False altrimenti.
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
    Stabilisco una connessione con il server beanstalkd, ed inserisco il producer nella tube indicata.
    In caso di insuccesso termino il processo.
    """
    def get_beanstalk(self):
        self.client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT)
        try:
            self.client.use(OUTPUT_TUBE)
        except BeanstalkError as e:
            print("Impossibile utilizzare la tube: {}!".format(e.message))
            exit()

    """
    Metodo statico utilizzato per scrivere su un sensore attraverso modbus.
    Non avendo particolari informazioni riguardanti il sensore stesso, si dovranno effettuare tutti
    i controlli del caso per verificare la buona riuscita dell'operazione.

    Parametri:
    - slave: indirizzo dello slave.
    - sensor: indirizzo del sensore.
    - value: valore da scrivere sul sensore.
    - decimals: numero di decimali con cui scrivere il valore.

    Raises: ValueError, TypeError, serial.SerialException, minimalmodbus.ModbusException, InvalidRegister
    """
    @staticmethod
    def write(slave, sensor, value, decimals = 0):
        slave = int(slave)
        sensor = int(sensor)
        value = int(value)
        try:
            s = minimalmodbus.Instrument(SERIAL_PORT, slave, mode = MODE, close_port_after_each_call = CLOSE_PORT_AFTER_EACH_CALL, debug = DEBUG)
            s.serial.baudrate = BAUDRATE
            s.serial.parity = PARITY
            s.serial.bytesize = BYTESIZE
            s.serial.stopbits = STOP_BITS
            s.serial.timeout = TIME_OUT_READ
            s.serial.write_timeout = TIME_OUT_WRITE
        except (ValueError, serial.SerialException):
            raise
        if sensor >= 0 and sensor <= 9998:
            #Coil
            try:
                s.write_bit(sensor, value)
            except (TypeError, ValueError, minimalmodbus.ModbusException, serial.SerialException):
                raise
        elif sensor >= 40000 and sensor <= 49998:
            #Holding register
            try:
                s.write_register(sensor, value, number_of_decimals = decimals)
            except (TypeError, ValueError, minimalmodbus.ModbusException, serial.SerialException):
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
            time.sleep(REFRESH_RATE-(int(time.time())-now) if REFRESH_RATE-(int(time.time())-now) >= 0 else 0)

class WriteDaemon(object):

    def __init__(self):
        thread = threading.Thread(target = self.run, args = ())
        thread.daemon = True
        thread.start()

    def run(self):
        client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT, auto_decode = True)
        try:
            client.watch(INPUT_TUBE)
        except BeanstalkError as e:
            print("Impossibile inserire nella watchlist la tube '{}': {}".format(INPUT_TUBE, e))

        while True:
            for job in client.reserve_iter():
                data = job.job_data
                client.delete_job(job.job_id)
                try:
                    decoded_data = json.loads(data)
                    Handler.write(decoded_data['slave'], decoded_data['sensor'], decoded_data['value'])
                except json.JSONDecodeError as e:
                    print("Impossibile decodificare il comando {}: {} ".format(data, e))
                except KeyError as e:
                    print("Il formato del comando non è valido: {}".format(e))
                except ValueError as e:
                    print("Valore non valido: {}".format(e))
                except TypeError as e:
                    print("Tipo non valido: {}".format(e))
                except serial.SerialException as e:
                    print("Errore della linea seriale: {}".format(e))
                except minimalmodbus.ModbusException as e:
                    print("Errore nel protocollo Modbus: {}".format(e))
                except InvalidRegister as e:
                    print("Indirizzo del registro non valido.")

if __name__ == "__main__":
    h = Handler()
    RefreshThread(h)
    WriteDaemon()