import os, threading, minimalmodbus, serial, json, time, datetime, atexit, uuid
from pystalk import BeanstalkClient, BeanstalkError

DEBUG = False
CONF_FILE = 'config_file.json'
DAEMON_LOG_FILE = "daemon.log"

#MODBUS consts
SERIAL_PORT = '/dev/ttyUSB0'
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
SERVER_TUBE = 'driver'
OUTPUT_TUBE = 'data'
INPUT_TUBE = 'commands'

#MODBUS data types
BIT = 0
COIL = 1
INPUT_REGISTER = 3
HOLDING_REGISTER = 4

#MODBUS SCANNER consts
UPPER_BOUND_ADDRESS = 32

def get_slaves():
    with open(CONF_FILE) as f:
        d = json.load(f)
    return d

MANDATORY_FIELDS_SENSOR = ['address', 'type']
MANDATORY_FIELDS_SLAVE = ['address', ]

mutex = threading.Lock()

"""
Utilities
"""
# Controlla se gli elementi di una lista sono tutti numeri interi.
def are_integers(l):
    try:
        for i in l:
            s = str(i)
            try:
                int(s)
            except ValueError:
                return False
        return True
    except TypeError:
        return False

# Controlla se il variabile contiene una valore intero.        
def is_integer(v):
    s = str(v)
    try:
        int(s)
        return True
    except TypeError:
        return False

# Funzione per la stampa di messaggi di log.
def print_log(class_name, msg):
    print("{} {}:{}".format(class_name, time.strftime("%a %d/%m/%Y %H:%M:%S"), msg))

"""
Eccezioni personalizzate per la gestione del driver.
"""
class InvalidRegister(minimalmodbus.ModbusException):
    pass

class InvalidCommand(ValueError):
    pass

"""
La seguente funzione 
"""
def clean_up_processes(scanner_tube = None, handler_tube = None):
    pass

"""
La classe 'Driver' soprintende e coordina l'azione delle due classi 'Handler' e 'Scanner'.
"""
class Driver(object):
    self.scanning = False
    self.scanning_mutex = threading.Lock()
    self.retrieving = False
    self.retrieving_mutex = threading.Lock()

    """
    Il costruttore deve creare un client beanstalk per poter comunicare con lo script di 
    sincronizzazione locale.
    """
    def __init__(self):
        print("\t***Driver attivato***")
        self.client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT, auto_decode = True)
        try:
            print_log("Driver", "'watch': {} .".format(SERVER_TUBE))
            self.client.watch(SERVER_TUBE)
        except BeanstalkError as e:
            print_log("Driver", "'watch': {} - Fallito: {}".format(SERVER_TUBE, e))
            self.driver_exit()
        self.serve()

    """
    Si mette in ascolto sulla tube 'driver', ed esegue eventuali comandi.
    """
    def serve(self):
        for job in self.client.reserve_iter():
            data = job.data
            client.delete_job(job.job_id)
            print_log("Driver", "parsing del comando: {}".format(data))
            try:
                words =self._parse_command(data)
            except InvalidCommand as e:
                print_log("Driver", "comando '{}' non valido:{}".format(data,e))
                continue
            if words[0] == "scan":
                scan(words)
            else:
                start(words)

    def scan(self, parameters):
        self.scanning_mutex.acquire()
        if self.scanning:
            print_log("Driver", "comando 'scan': processo di scanning ancora in corso.")
            self.scanning_mutex.release()
            return
        self.scanning = True
        self.scanning_mutex.release()
        self.retrieving_mutex.acquire()
        if self.retrieving:
            #kill the thread
            pass
        else:
            self.retrieving_mutex.release()
        file_name = uuid.uuid4().hex
        #avvia la fase di scanning

    def start(self, parameters):
        self.scanning_mutex.acquire()
        if self.scanning:
            print_log("Driver", "comando 'start': processo di scanning ancora in corso.")
            self.scanning_mutex.release()
            return
        self.scanning_mutex.release()
        self.retrieving_mutex.acquire()
        if self.retrieving:
            print_log("Driver", "comando 'start': processo di retrieving ancora in corso.")
            self.retrieving_mutex.release()
            return
        self.retrieving = True
        self.retrieving_mutex.release()
        #avvia la fase di retrieving
            

    """
    Il seguente metodo fa il parsing del comando arrivato sulla tube 'driver'.
    I comandi ammessi sono riportati di seguito:

    - scan [[start_address end_address], [address_list]]
    - start data_tube command_tube config_file

    Constraints:
    - start_address, end_address: devono essere numeri interi maggiori di -1 e minori di UPPER_BOUND_ADDRESS.
    - address_list: deve essere una stringa che inizia con '[' e che finisce con ']'. Gli elementi sono divisi
                    da ',' e possono essere solo dei numeri interi maggiori di -1 e minori di UPPER_BOUND_ADDRESS.
                    Non sono ammessi spazi.
    - config_file: path assoluto di un file esistente.

    Returns:
    Il metodo ritorna una lista con gli oggetti nativi dei parametri.
    """
    def _parse_command(self, data):
        if len(data) == 0:
            raise InvalidCommand("la stringa ricevuta non contiene alcun carattere.")
        
        words = data.split()
        if words[0] == "scan":
            if len(words) == 2:
                """
                In questo caso il parametro deve essere una lista.
                """
                if words[1][0] != '[' or words[1][-1:] != ']':
                    raise InvalidCommand("la stringa {} non è valida come lista.".format(words[1]))     
                addresses = words[1].strip("[]").split(',')
                try:
                    addresses_int = [int(x) for x in addresses]
                except ValueError:
                    raise InvalidCommand("uno o più elementi della lista {} non sono indirizzi validi.".format(addresses))
                if  min(addresses_int) < 0 or max(addresses_int) > UPPER_BOUND_ADDRESS:
                    raise InvalidCommand("la lista {} contiene degli indirizzi al di fuori del range consentito.".format(addresses))
                
                return ["scan", [addresses_int, ]]
            elif len(words) == 3:
                """
                In questo caso si devono trovare in words[1] e words[2] due numeri interi.
                """
                try:
                    p1 = int(words[1])
                    p2 = int(words[2])
                except ValueError:
                    raise InvalidCommand("il range di indirizzi specificato non è valido.")
                
                if p1 <= p2:
                    raise InvalidCommand("l'indirizzo massimo è più piccolo o uguale all'indirizzo minimo.")

                if p1 < 0 or p2 > UPPER_BOUND_ADDRESS:
                    raise InvalidCommand("il range di indirizzi identificato non è valido.")

                return ["scan", [p1, p2]]
            else:
                raise InvalidCommand("numero di parametri non valido per il comando 'start'.")
        elif words[0] == "start":
            if len(words) != 4:
                raise InvalidCommand("sono necessari tre parametri per il comando 'start'.")
            if not os.path.isfile(words[3]):
                raise InvalidCommand("il file di configurazione non valido per il comando 'start'.")
            return words
        else:
            raise InvalidCommand("{} non corrisponde a nessun comando valido.")

    """
    Termina il processo del driver e killa tutti i thread avviati.
    """
    def driver_exit(self):
        print_log("Driver", "termino il processo.")
        exit()


"""
La classe 'Handler' gestisce lo scambio di dati in tempo reale con gli slave MODBUS presenti nella mesh.
"""
class Handler:
    """
    Il costruttore importa il layout della rete MODBUS dal file di configurazione. 
    Se non riesce ad aprire una connessione con nessuno degli slave, oppure il file di configurazione è mal formattato
    termina il processo.
    """
    """
    def __init__(self):
        try:
            slaves = get_slaves()
        except json.JSONDecodeError as e:
            print("Impossibile leggere il contenuto del file di configurazione {}: {}".format(CONF_FILE, e))
            exit()
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
    def __init__(self, tube):

        
    """
    Fa il refresh dei valori presenti in ogni slave classificato come 'to_update' : 1.
    Nel caso in cui un sensore non abbia sufficienti informazioni associate, la sua lettura viene saltata.
    Le eventuali eccezioni vengono gestite all'interno del metodo.
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
        slave['instance'].serial.close()   #per dare la possibilità al daemon dei comandi di accedere agli slave.
    
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
    Controllo se il file di configurazione ha tutti i campi necessari.
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
                s.write_register(sensor-40000, value, number_of_decimals = decimals, functioncode=6)
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
            mutex.acquire()
            now = int(time.time())
            self.instance.refresh_values()
            mutex.release()
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
                mutex.acquire()
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
                finally:
                    mutex.release()

"""
La classe 'Scanner' identifica tutti gli slave modbus presenti nella mesh.
"""
class Scanner(object):
    """
    Fa lo scanner dall'indirizzo 'start_address' all'indirizzo 'end_address' (escluso). 
    Quindi non si può avere 'start_address'=='end_address'. Alternativamente si può 
    fornire una lista di indirizzi 'address_list' su cui effettuare lo scan. 
    Non si possono usare entrambi i metodi.Nel caso in cui entrambi siano validi,
    viene data la precedenza alla lista di indirizzi.
    """
    def __init__(self, tube):
        pass
    
    def scan(self):
        pass

if __name__ == "__main__":
    atexit.register(clean_up_processes)
    d = Driver()