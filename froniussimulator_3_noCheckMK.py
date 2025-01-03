#!/usr/bin/env python
"""
Simulates a Fronius Smart Meter for providing necessary
information to inverters (e.g. Gen24).
Necessary information is provided via MQTT and translated to MODBUS TCP

Based on
https://www.photovoltaikforum.com/thread/185108-fronius-smart-meter-tcp-protokoll

"""
###############################################################
# Import Libs
###############################################################
import threading
import struct
import time
import json
import getopt
import sys
import socket
import signal
import os

from pymodbus.version import version
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.datastore import ModbusSparseDataBlock
from pymodbus.datastore import ModbusSlaveContext
from pymodbus.datastore import ModbusServerContext
from pymodbus.server import StartTcpServer
from pymodbus.transaction import ModbusAsciiFramer
from pymodbus.transaction import ModbusBinaryFramer
from pymodbus.transaction import ModbusSocketFramer
from pymodbus.transaction import ModbusTlsFramer

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe

###############################################################
# Timer Class
###############################################################
class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = threading.Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


###############################################################
# Configuration
###############################################################
mqtt_conf = {
            'username':"",
            'password':"",
            'address': "",
            'port': 1883
}
MQTT_TOPIC_CONSUMPTION  = "FSM/Leistung" # Import Watts
MQTT_TOPIC_TOTAL_IMPORT = "FSM/Netzbezug_total" # Import Wh
MQTT_TOPIC_TOTAL_EXPORT = "FSM/Netzeinspeisung_total" # Export Wh

correction_factor = int(1000)
modbus_port = 502

###############################################################
# MQTT service
###############################################################

lock = threading.Lock()

net_power = "0"
export_power = "0"
import_power = "0"
rtime = 0

ti_int1 = "0"
ti_int2 = "0"
exp_int1 = "0"
exp_int2 = "0"
ep_int1 = "0"
ep_int2 = "0"

mqtt_client = mqtt.Client("SmartMeter", clean_session=False)
#mqtt_client.username_pw_set(mqtt_conf['username'], mqtt_conf['password'])
mqtt_client.connect(mqtt_conf['address'], mqtt_conf['port'], 60)

mqtt_client.subscribe(MQTT_TOPIC_CONSUMPTION)
mqtt_client.subscribe(MQTT_TOPIC_TOTAL_IMPORT)
mqtt_client.subscribe(MQTT_TOPIC_TOTAL_EXPORT)
mqtt_connected = 0

def on_connect(client, userdata, flags, rc):
   global mqtt_connected
   mqtt_connected = 1
   print("MQTT connected.")

def on_disconnect(client, userdata, rc):
   global mqtt_connected
   mqtt_connected = 0
   print("MQTT disconnected unexpectedly.")

mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_connect = on_connect
mqtt_client.clean_session=False

def on_message(client, userdata, message):
    global net_power
    global export_power
    global import_power

    print("Received message '" + str(message.payload) + "' on topic '"
        + message.topic + "' with QoS " + str(message.qos))

    if not isfloat(message.payload):
        return

    lock.acquire()

    if message.topic == MQTT_TOPIC_CONSUMPTION:
       net_power = message.payload
    elif message.topic == MQTT_TOPIC_TOTAL_IMPORT:
        import_power = message.payload
    elif message.topic == MQTT_TOPIC_TOTAL_EXPORT:
        export_power = message.payload

    lock.release()

mqtt_client.on_message = on_message

mqtt_client.loop_start()

###############################################################
# Update Modbus Registers
###############################################################
def updating_writer(a_context):
    global net_power
    global export_power
    global import_power

    global ep_int1
    global ep_int2
    global exp_int1
    global exp_int2
    global ti_int1
    global ti_int2

    global mqtt_connected

    lock.acquire()
    # Considering correction factor
    print("adjusted values:")

    adjusted_import = float(import_power) * correction_factor
    print(adjusted_import)

    adjusted_export = float(export_power) * correction_factor
    print(adjusted_export)

    # Converting current power consumption out of MQTT payload to Modbus register

    electrical_power_float = float(net_power) # extract value out of payload
    print(electrical_power_float)
    if electrical_power_float == 0:
        ep_int1 = 0
        ep_int2 = 0
    else:
        electrical_power_hex = hex(struct.unpack('<I', struct.pack('<f', electrical_power_float))[0])
        electrical_power_hex_part1 = str(electrical_power_hex)[2:6] # extract first register part (hex)
        electrical_power_hex_part2 = str(electrical_power_hex)[6:10] # extract seconds register part (hex)
        ep_int1 = int(electrical_power_hex_part1, 16) # convert hex to integer because pymodbus converts back to hex itself
        ep_int2 = int(electrical_power_hex_part2, 16) # convert hex to integer because pymodbus converts back to hex itself

    # Converting total import value of smart meter out of MQTT payload into Modbus register

    total_import_float = int(adjusted_import)
    total_import_hex = hex(struct.unpack('<I', struct.pack('<f', total_import_float))[0])
    total_import_hex_part1 = str(total_import_hex)[2:6]
    total_import_hex_part2 = str(total_import_hex)[6:10]
    ti_int1  = int(total_import_hex_part1, 16)
    ti_int2  = int(total_import_hex_part2, 16)

    # Converting total export value of smart meter out of MQTT payload into Modbus register

    total_export_float = int(adjusted_export)
    total_export_hex = hex(struct.unpack('<I', struct.pack('<f', total_export_float))[0])
    total_export_hex_part1 = str(total_export_hex)[2:6]
    total_export_hex_part2 = str(total_export_hex)[6:10]
    exp_int1 = int(total_export_hex_part1, 16)
    exp_int2 = int(total_export_hex_part2, 16)

    print("updating internal memory")
    context = a_context[0]
    register = 3
    address = 0x9C87
    values = [0, 0,               # Ampere - AC Total Current Value [A]
              0, 0,               # Ampere - AC Current Value L1 [A]
              0, 0,               # Ampere - AC Current Value L2 [A]
              0, 0,               # Ampere - AC Current Value L3 [A]
              0, 0,               # Voltage - Average Phase to Neutral [V]
              0, 0,               # Voltage - Phase L1 to Neutral [V]
              0, 0,               # Voltage - Phase L2 to Neutral [V]
              0, 0,               # Voltage - Phase L3 to Neutral [V]
              0, 0,               # Voltage - Average Phase to Phase [V]
              0, 0,               # Voltage - Phase L1 to L2 [V]
              0, 0,               # Voltage - Phase L2 to L3 [V]
              0, 0,               # Voltage - Phase L1 to L3 [V]
              0, 0,               # AC Frequency [Hz]
              ep_int1, 0,         # AC Power value (Total) [W] ==> Second hex word not needed
              0, 0,               # AC Power Value L1 [W]
              0, 0,               # AC Power Value L2 [W]
              0, 0,               # AC Power Value L3 [W]
              0, 0,               # AC Apparent Power [VA]
              0, 0,               # AC Apparent Power L1 [VA]
              0, 0,               # AC Apparent Power L2 [VA]
              0, 0,               # AC Apparent Power L3 [VA]
              0, 0,               # AC Reactive Power [VAr]
              0, 0,               # AC Reactive Power L1 [VAr]
              0, 0,               # AC Reactive Power L2 [VAr]
              0, 0,               # AC Reactive Power L3 [VAr]
              0, 0,	              # AC power factor total [cos phi]
              0, 0,               # AC power factor L1 [cos phi]
              0, 0,               # AC power factor L2 [cos phi]
              0, 0,               # AC power factor L3 [cos phi]
              exp_int1, exp_int2, # Total Watt Hours Exported [Wh]
              0, 0,               # Watt Hours Exported L1 [Wh]
              0, 0,               # Watt Hours Exported L2 [Wh]
              0, 0,               # Watt Hours Exported L3 [Wh]
              ti_int1, ti_int2,   # Total Watt Hours Imported [Wh]
              0, 0,               # Watt Hours Imported L1 [Wh]
              0, 0,               # Watt Hours Imported L2 [Wh]
              0, 0,               # Watt Hours Imported L3 [Wh]
              0, 0,               # Total VA hours Exported [VA]
              0, 0,               # VA hours Exported L1 [VA]
              0, 0,               # VA hours Exported L2 [VA]
              0, 0,               # VA hours Exported L3 [VA]
              0, 0,               # Total VAr hours imported [VAr]
              0, 0,               # VA hours imported L1 [VAr]
              0, 0,               # VA hours imported L2 [VAr]
              0, 0                # VA hours imported L3 [VAr]
]

    context.setValues(register, address, values)
    lock.release()


###############################################################
# Config and start Modbus TCP Server
###############################################################
def run_updating_server():
    global modbus_port
    lock.acquire()
    data_block = ModbusSparseDataBlock({

        40001:  [21365, 28243],
        40003:  [1],
        40004:  [65],
        40005:  [70,114,111,110,105,117,115,0,0,0,0,0,0,0,0,0,         # Manufacturer "Fronius
                83,109,97,114,116,32,77,101,116,101,114,32,54,51,65,0, # Device Model "Smart Meter
                0,0,0,0,0,0,0,0,                                       # Options N/A
                0,0,0,0,0,0,0,0,                                       # Software Version  N/A
                48,48,48,48,48,48,48,49,0,0,0,0,0,0,0,0,               # Serial Number: 00000
                240],                                                  # Modbus TCP Address:
        40070: [213],
        40071: [124],
        40072: [0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,
                0,0,0,0],

        40196: [65535, 0],
    })

    slave_context = ModbusSlaveContext(
            di=data_block,
            co=data_block,
            hr=data_block,
            ir=data_block,
        )

    a_context = ModbusServerContext(slaves=slave_context, single=True)

    lock.release()

    ###############################################################
    # Run Update Register every 5 Seconds
    ###############################################################
    interval = 5  # 5 seconds delay
    timer = RepeatedTimer(interval, updating_writer, a_context)

    print("### start server, listening on " + str(modbus_port))
    address = ("", modbus_port)
    StartTcpServer(
            context=a_context,
            address=address,
            framer=ModbusSocketFramer,
            allow_reuse_address=True,
        )


values_ready = False

while not values_ready:
      print("Waiting for data from MQTT broker")
      time.sleep(1)
      lock.acquire()
      if import_power != '0' and export_power != '0':
         print("Received required data. Starting Modbus Server")
         values_ready = True
      lock.release()
run_updating_server()
