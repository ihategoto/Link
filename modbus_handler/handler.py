import os, threading, minimalmodbus, serial, json, time, datetime, atexit, uuid
from pystalk import BeanstalkClient, BeanstalkError

DEBUG = False

#MODBUS consts
SERIAL_PORT = '/dev/ttyS0'
BAUDRATE = 9600
BYTESIZE = 8
PARITY = serial.PARITY_NONE
STOP_BITS = 1
MODE = minimalmodbus.MODE_RTU
TIME_OUT_READ = 3
TIME_OUT_WRITE = 5
CLOSE_PORT_AFTER_EACH_CALL = True

REFRESH_RATE = 5 #seconds

#BEANSTALKD consts
BEANSTALKD_HOST = '127.0.0.1'
BEANSTALKD_PORT = 11300
SERVER_TUBE = 'driver'

#MODBUS data types
BIT = 0
COIL = 1
INPUT_REGISTER = 3
HOLDING_REGISTER = 4

#MODBUS SCANNER consts
UPPER_BOUND_ADDRESS = 32

def get_slaves(config_file):
    with open(config_file) as f:
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

class EmptyMesh(Exception):
    pass

class InvalidNode(Exception):
    pass

"""
La seguente funzione chiude interrompe tutti gli eventuali thread ancora in esecuzione (è necessaria?)
"""
def clean_up_processes():
    pass

"""
La seguente classe rappresenta il thread che viene lanciato per effettuare il retrieve dei dati.
"""
class RetrieveThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.instance = Handler()
        self.stop_flag = threading.Event()

    def set_tube(self, tube):
        self.instance.set_tube(tube)

    def set_config(self, config_file):
        self.instance.set_config(config_file)

    def run(self):
        self.stop_flag.clear()
        while True:
            mutex.acquire()
            now = int(time.time())
            try:
                self.instance.refresh_values()
            except AttributeError:
                print_log("RetrieveThread", "parametri non sufficienti per avviare il processo di retrieving.")
                print_log("RetrieveThread", "esco!")
                mutex.release()
                return
            mutex.release()
            if self.stopped():
                print_log("RetrieveThread", "esco!")
                return
            time.sleep(REFRESH_RATE-(int(time.time())-now) if REFRESH_RATE-(int(time.time())-now) >= 0 else 0)

    def stopped(self):
        return self.stop_flag.is_set()

    def stop(self):
        self.stop_flag.set()

"""
La seguente classe rappresenta il thread che viene lanciato per eseguire eventuali scritture sugli slave.
"""
class WriteThread(threading.Thread):

    def __init__(self, args = {}):
        threading.Thread.__init__(self)
        self.stop_flag = threading.Event()

    def set_tube(self, tube):
        self.command_tube = tube

    def run(self):
        if not hasattr(self, 'command_tube'):
            print_log("WriteThread", "parametri non sufficienti per avviare il thread.")
            return
        self.stop_flag.clear()
        client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT, auto_decode = True)
        try:
            client.watch(self.command_tube)
        except BeanstalkError as e:
            print_log("write_thread","impossibile inserire nella watchlist la tube '{}': {}".format(INPUT_TUBE, e))

        while True:
            """
            if mutex.locked():
                mutex.release()
            """
            if self.stopped():
                print_log("WriteThread", "esco!")
                return
            for job in client.reserve_iter():
                data = job.job_data
                client.delete_job(job.job_id)
                mutex.acquire()
                try:
                    decoded_data = json.loads(data)
                    print_log("WriteThread", "eseguo il comando {}".format(decoded_data))
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
            
    def stopped(self):
        return self.stop_flag.is_set()

    def stop(self):
        self.stop_flag.set()
"""
La classe 'Driver' soprintende e coordina l'azione delle due classi 'Handler' e 'Scanner'.
"""
class Driver(object):

    """
    Il costruttore deve creare un client beanstalk per poter comunicare con lo script di 
    sincronizzazione locale.
    """
    def __init__(self):
        print("\t***Driver attivato***")
        self.scanning_thread = None
        self.retrieving_thread = None
        self.write_thread = None
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
        while True:
            for job in self.client.reserve_iter():
                data = job.job_data
                self.client.delete_job(job.job_id)
                print_log("Driver", "parsing del comando: {}".format(data))
                try:
                    words = self._parse_command(data)
                except InvalidCommand as e:
                    print_log("Driver", "comando '{}' non valido:{}".format(data,e))
                    continue
                if words[0] == "scan":
                    self.scan(words)
                else:
                    self.start(words)

    """
    Il seguente metodo avvia il thread che si occuperà dello scanning della mesh MODBUS
    nel caso in cui sia possibile farlo. Definisco di seguito il comportamento nei vari 
    contesti possibili:

    - Se è in corso il processo di scanning: 
    Il metodo ritorna senza far nulla.
    
    - Se è in corso il processo di retrieving:
    Il metodo interrompe il thread che sta eseguendo il retrieving e poi fa cominciare
    il processo di scanning.

    - Se non è in corso né il processo di scanning né quello di retrieving:
    Il metodo avvia il processo di scanning.
    """
    def scan(self, parameters):
        if self.retrieving_thread is not None and self.retrieving_thread.is_alive() and self.write_thread is not None and self.write_thread.is_alive():
            self.retrieving_thread.stop()
            self.write_thread.stop()

    """
    Il seguente metodo avvia il thread che si occuperà del retrieving dei dati.
    Definisco di seguito il comportamento nei vari contesti possibili:

    - Se è in corso il processo di scanning:
    Il metodo ritorna senza far nulla.

    - Se è in corso il processo di retrieving:
    Il metodo ritorna senza far nulla.

    - Se non è in corso né il processo di retrieving né il processo di scanning:
    Il metodo avvia il processo di retrieving.
    """
    def start(self, parameters):
        if isinstance(self.scanning_thread, threading.Thread):
            if self.scanning_thread.is_alive():
                print_log("Driver", "comando 'start': processo di scanning in corso.")
                return
        """
        Si assume che i due thread RetrieveThread e WriteThread siano coordinati.
        Ovvero che se RetrieveThread è attivo lo è anche WriteThread.
        """
        if isinstance(self.retrieving_thread, threading.Thread):
            if self.retrieving_thread.is_alive():
                print_log("Driver", "comando 'start': processo di retrieving in corso.")
                return
            self.retrieving_thread = RetrieveThread()
            self.write_thread = WriteThread()
        else:
            self.retrieving_thread = RetrieveThread()
            self.write_thread = WriteThread()

        try:
            self.retrieving_thread.set_tube(parameters[1])
        except BeanstalkError as e:
            print_log("Driver", "comando 'start': impossibile settare la tube dei dati: {}".format(e))
            return 
        try:
            self.retrieving_thread.set_config(parameters[3])
        except json.JSONDecodeError as e:
            print_log("Driver", "comando 'start': file di configurazione non valido: {}".format(e))
            return
        except EmptyMesh:
            print_log("Driver", "comando 'start': mesh vuota.")
            return
        except minimalmodbus.ModbusException as e:
            print_log("Driver", "comando 'start': errore durante l'apertura di nodo della mesh: {}".format(e))
        except serial.SerialException as e:
            print_log("Driver", "comando 'start': errore nell'utilizzo della linea seriale: {}".format(e))

        self.write_thread.set_tube(parameters[2])
        self.retrieving_thread.start()
        self.write_thread.start()
    
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
                
                return ["scan", addresses_int]
            elif len(words) == 3:
                """
                In questo caso si devono trovare in words[1] e words[2] due numeri interi.
                """
                try:
                    p1 = int(words[1])
                    p2 = int(words[2])
                except ValueError:
                    raise InvalidCommand("il range di indirizzi specificato non è valido.")
                
                if p1 >= p2:
                    raise InvalidCommand("l'indirizzo massimo è più piccolo o uguale all'indirizzo minimo.")

                if p1 < 0 or p2 > UPPER_BOUND_ADDRESS:
                    raise InvalidCommand("il range di indirizzi identificato non è valido.")

                return ["scan", p1, p2]
            else:
                raise InvalidCommand("numero di parametri non valido per il comando 'scan'.")
        elif words[0] == "start":
            if len(words) != 4:
                raise InvalidCommand("sono necessari tre parametri per il comando 'start'.")
            if not os.path.isfile(words[3]):
                raise InvalidCommand("il file di configurazione non valido per il comando 'start'.")
            return words
        else:
            raise InvalidCommand("non corrisponde a nessun comando valido.")

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

    def set_tube(self,tube):
        self.get_beanstalk(tube)

    def set_config(self, config_file):
        slaves = get_slaves(config_file)
        if len(slaves) == 0:
            raise EmptyMesh
        self.slave_maps = []
        for slave in slaves:
            if not self.check_fields(slave, False):
                continue
            if not hasattr(self, 'serial_instance'):
                try:
                    self.serial_instance = minimalmodbus.Instrument(SERIAL_PORT, slave['address'], mode = MODE, close_port_after_each_call = CLOSE_PORT_AFTER_EACH_CALL, debug = DEBUG)
                    self.serial_instance.serial.baudrate = BAUDRATE
                    self.serial_instance.serial.parity = PARITY
                    self.serial_instance.serial.bytesize = BYTESIZE
                    self.serial_instance.serial.stopbits = STOP_BITS
                    self.serial_instance.serial.timeout = TIME_OUT_READ
                    self.serial_instance.serial.write_timeout = TIME_OUT_WRITE
                except Exception:
                    raise

            self.slave_maps.append(slave)
        
        if len(self.slave_maps) == 0:
            raise EmptyMesh

    """
    Fa il refresh dei valori presenti in ogni slave classificato come 'to_update' : 1.
    Nel caso in cui un sensore non abbia sufficienti informazioni associate, la sua lettura viene saltata.
    Le eventuali eccezioni vengono gestite all'interno del metodo.
    """
    def refresh_values(self):
        if not hasattr(self, 'slave_maps') or not hasattr(self, 'client') or not hasattr(self,'serial_instance'):
            raise AttributeError
        for slave in self.slave_maps:
            self.serial_instance.address = slave['address']
            for sensor in slave['map']:
                #Se il sensore non deve essere aggiornato salto.
                if sensor['to_update'] == 0:
                    continue
                if not self.check_fields(sensor, True):
                    print('Errore di configurazione di un sensore appartenente allo slave {}! Salto la scrittura sul database.'.format(slave['info']['address']))
                    continue                
                address, functioncode, callback = self.get_call_info(self.serial_instance, sensor)
                try:
                    value = callback(address, functioncode = functioncode)
                    data = {'slave' : slave['address'], 'sensor' : sensor['address'], 'timestamp' : time.time(), "value" : value}
                    self.client.put_job(json.dumps(data))
                except (ValueError, TypeError) as e:
                    print("Qualcosa è andato storto durante la lettura di {} da {}:{}".format(sensor['address'], slave['address'], e))
                except minimalmodbus.ModbusException as e:
                    print("Errore MODBUS durante la lettura di {} da {}: {}".format(sensor['address'], slave['address'], e))
                except BeanstalkError as e:
                    print("Impossibile scrivere sul server BeansTalk il contenuto del sensore {} dello slave {}: {}".format(sensor['address'], slave['address'], e))
            time.sleep(1)
    """
    Ritorna l'indirizzo relativo, il functioncode adatto al sensore e la funzione corretta di minimalmodbus.
    
    - slave: dict contenente tutte le informazioni riguardanti il sensore di cui si vuole scrivere sul database.
    - sensor: dict contenente sia l'oggetto minimalmodbus.Instrument che i metadati inerenti allo slave.
    
    Ritorna: indirizzo relativo del sensore, il functioncode per l'operazione richiesta e la funzione adeguata per eseguirla.
    """
    def get_call_info(self, serial_instance, sensor):
        if sensor['type'] == BIT:
            address = sensor['address'] - 10000
            functioncode = 2
            callback = serial_instance.read_bit
        elif sensor['type'] == INPUT_REGISTER:
            address = sensor['address'] - 30000
            functioncode = 4
            callback = serial_instance.read_register
        elif sensor['type'] == HOLDING_REGISTER:
            address = sensor['address'] - 40000
            functioncode = 3
            callback = serial_instance.read_register
        else:
            #L'indirizzo relativo delle coil coincide con quello fisico.
            address = sensor['address']
            functioncode = 1
            callback = serial_instance.read_bit
            
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
    def get_beanstalk(self, tube):
        self.client = BeanstalkClient(BEANSTALKD_HOST, BEANSTALKD_PORT)
        try:
            self.client.use(tube)
        except BeanstalkError as e:
            print("Impossibile utilizzare la tube: {}!".format(e))
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

"""
La classe 'Scanner' identifica tutti gli slave modbus presenti nella mesh.
"""
class Scanner(object):
    """
    Fa lo scanner dall'indirizzo 'start_address' all'indirizzo 'end_address' (escluso). 
    Quindi non si può avere 'start_address'=='end_address'. Alternativamente si può 
    fornire una lista di indirizzi 'address_list' su cui effettuare lo scan. 
    Non si possono usare entrambi i metodi. ,
    Nel caso in cui entrambi siano validi,
    viene data la precedenza alla lista di indirizzi.
    """
    def __init__(self, tube):
        pass
    
    def scan(self):
        pass

if __name__ == "__main__":
    #atexit.register(clean_up_processes)
    d = Driver()