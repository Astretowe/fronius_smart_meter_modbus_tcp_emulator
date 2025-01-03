#!/usr/bin/env python
"""
Emulator which takes generic "load", "import" and "export" information (via MQTT) and provides a Modbus TCP interface which behaves similar enough to a Fronius Smart Meter to satisfy a Fronius inverter.
Please take note that this currently doesn't properly map the metrics outside the three mentioned above. This means that the inverter will display "0" in most other fields (like voltage).

Most code is taken / derived from previous implementations:
https://github.com/americanium/fronius_sm_simulator_rtu
https://github.com/americanium/fronius_sm_simulator_tcp
https://github.com/tichachm/fronius_smart_meter_modbus_tcp_emulator

Version 1.0.0
"""

import concurrent.futures
import datetime
import os
import struct
import threading
import time

from pymodbus.datastore import ModbusSparseDataBlock
from pymodbus.datastore import ModbusSlaveContext
from pymodbus.datastore import ModbusServerContext
from pymodbus.server import ServerStop
from pymodbus.server import StartTcpServer
from pymodbus.transaction import ModbusSocketFramer

import paho.mqtt.client as mqtt


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()

        if self.args is None:
            self.function()
        else:
            self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = threading.Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def mqtt_on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = True
    print("MQTT connected.")


def mqtt_on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print("MQTT disconnected unexpectedly.")


def mqtt_on_message(client, userdata, message):
    global current_power
    global total_export
    global total_import
    global last_message_received

    if debug:
        print("Received message '" + str(message.payload) + "' on topic '" + message.topic + "' with QoS " + str(message.qos))

    if isinstance(message.payload, float):
        converted_value = message.payload
    else:
        converted_value = float(message.payload)

    lock.acquire()

    if message.topic == mqtt_topic_current_power:
        current_power = converted_value
        last_message_received = datetime.datetime.now()
    elif message.topic == mqtt_topic_total_import:
        total_import = converted_value
        last_message_received = datetime.datetime.now()
    elif message.topic == mqtt_topic_total_export:
        total_export = converted_value
        last_message_received = datetime.datetime.now()

    if debug:
        print("Updated value: " + "{0:0.2f}".format(converted_value))

    lock.release()


"""Convert the given number to two bytes fit for the modbus registers."""
def to_two_bytes(input_value):
    if input_value == 0:
        return 0, 0
    else:
        hex_string = str(hex(struct.unpack('<I', struct.pack('<f', float(input_value)))[0]))
        return int(hex_string[2:6], 16), int(hex_string[6:10], 16)


"""Set up a mqtt client instance."""
def setup_mqtt():
    mqtt_client = mqtt.Client("SmartMeter", clean_session=True)

    mqtt_client.on_disconnect = mqtt_on_disconnect
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message

    if mqtt_username and mqtt_password:
        mqtt_client.username_pw_set(mqtt_username, mqtt_password)

    mqtt_client.connect(mqtt_host, mqtt_port, 60)

    mqtt_client.subscribe(mqtt_topic_current_power)
    mqtt_client.subscribe(mqtt_topic_total_import)
    mqtt_client.subscribe(mqtt_topic_total_export)

    return mqtt_client


"""Check connection state and start / stop the modbus server.

This will allow us to better communicate the "smart meter" state to whoever is requesting data.
"""
def check_server():
    global server_started

    # we need to check if the mqtt connection is up AND if the last update is "recent".
    received_updates = (datetime.datetime.now() - last_message_received).total_seconds() < 15

    if debug:
        print("----------------------")
        print("check_server conditions:")
        print("server_started: " + "{0!s}".format(server_started))
        print("mqtt_connected: " + "{0!s}".format(mqtt_connected))
        print("received_updates: " + "{0!s}".format(received_updates))
        print("----------------------")

    if mqtt_connected and received_updates:
        if not server_started:
            print("(Re-)starting server. Listening on port " + str(modbus_port))
            server_started = True
            StartTcpServer( # this will actually block the thread until the server is stopped, so we need to switch the bool before doing this.
                context=server_context,
                address=bind_address,
                framer=ModbusSocketFramer,
                allow_reuse_address=True,
            )

    elif server_started:
        # conditions not met and server is running -> stop server.
        print("Stopping server to prevent outdated data from being transmitted.")
        server_started = False
        ServerStop()

"""Update the server context object with the current meter data."""
def update_server_context(input_context):

    lock.acquire()

    # Applying correction factor
    adjusted_power = float(current_power) * correction_factor_current_power
    adjusted_import = float(total_import) * correction_factor_total_import
    adjusted_export = float(total_export) * correction_factor_total_export

    if debug:
        print("----------------------")
        print("Adjusted values for register update:")
        print("Power: " + "{0:0.2f}".format(adjusted_power))
        print("Import: " + "{0:0.2f}".format(adjusted_import))
        print("Export: " + "{0:0.2f}".format(adjusted_export))
        print("----------------------")

    # Converting smart meter values into Modbus registers
    power_int1, power_int2 = to_two_bytes(adjusted_power)
    import_int1, import_int2 = to_two_bytes(adjusted_import)
    export_int1, export_int2 = to_two_bytes(adjusted_export)

    values = [0, 0,  # Ampere - AC Total Current Value [A]
              0, 0,  # Ampere - AC Current Value L1 [A]
              0, 0,  # Ampere - AC Current Value L2 [A]
              0, 0,  # Ampere - AC Current Value L3 [A]
              0, 0,  # Voltage - Average Phase to Neutral [V]
              0, 0,  # Voltage - Phase L1 to Neutral [V]
              0, 0,  # Voltage - Phase L2 to Neutral [V]
              0, 0,  # Voltage - Phase L3 to Neutral [V]
              0, 0,  # Voltage - Average Phase to Phase [V]
              0, 0,  # Voltage - Phase L1 to L2 [V]
              0, 0,  # Voltage - Phase L2 to L3 [V]
              0, 0,  # Voltage - Phase L1 to L3 [V]
              0, 0,  # AC Frequency [Hz]
              power_int1, 0,  # AC Power value (Total) [W] ==> Second hex word not needed
              0, 0,  # AC Power Value L1 [W]
              0, 0,  # AC Power Value L2 [W]
              0, 0,  # AC Power Value L3 [W]
              0, 0,  # AC Apparent Power [VA]
              0, 0,  # AC Apparent Power L1 [VA]
              0, 0,  # AC Apparent Power L2 [VA]
              0, 0,  # AC Apparent Power L3 [VA]
              0, 0,  # AC Reactive Power [VAr]
              0, 0,  # AC Reactive Power L1 [VAr]
              0, 0,  # AC Reactive Power L2 [VAr]
              0, 0,  # AC Reactive Power L3 [VAr]
              0, 0,  # AC power factor total [cos phi]
              0, 0,  # AC power factor L1 [cos phi]
              0, 0,  # AC power factor L2 [cos phi]
              0, 0,  # AC power factor L3 [cos phi]
              export_int1, export_int2,  # Total Watt Hours Exported [Wh]
              0, 0,  # Watt Hours Exported L1 [Wh]
              0, 0,  # Watt Hours Exported L2 [Wh]
              0, 0,  # Watt Hours Exported L3 [Wh]
              import_int1, import_int2,  # Total Watt Hours Imported [Wh]
              0, 0,  # Watt Hours Imported L1 [Wh]
              0, 0,  # Watt Hours Imported L2 [Wh]
              0, 0,  # Watt Hours Imported L3 [Wh]
              0, 0,  # Total VA hours Exported [VA]
              0, 0,  # VA hours Exported L1 [VA]
              0, 0,  # VA hours Exported L2 [VA]
              0, 0,  # VA hours Exported L3 [VA]
              0, 0,  # Total VAr hours imported [VAr]
              0, 0,  # VA hours imported L1 [VAr]
              0, 0,  # VA hours imported L2 [VAr]
              0, 0  # VA hours imported L3 [VAr]
              ]

    register_type = 3
    start_address = 0x9C87
    input_context[0].setValues(register_type, start_address, values)
    lock.release()


print("Application startup.")

# seems to be a workaround for some issue I didn't bother to actually investigate.
thread_pool_ref = concurrent.futures.ThreadPoolExecutor

# get configuration from docker environment.
debug = os.environ['DEBUG'].lower() == "true"

mqtt_username = os.environ['MQTT_USERNAME']
mqtt_password = os.environ['MQTT_PASSWORD']
mqtt_host = os.environ['MQTT_HOST']
mqtt_port = int(os.environ['MQTT_PORT'])

mqtt_topic_current_power = os.environ['MQTT_TOPIC_CURRENT_POWER']  # Current Watt
mqtt_topic_total_import = os.environ['MQTT_TOPIC_TOTAL_IMPORT']  # Import Wh
mqtt_topic_total_export = os.environ['MQTT_TOPIC_TOTAL_EXPORT']  # Export Wh

correction_factor_current_power = float(os.environ['CORRECTION_FACTOR_CURRENT_POWER'])  # adjustment factor if input data is not correctly scaled.
correction_factor_total_import = float(os.environ['CORRECTION_FACTOR_TOTAL_IMPORT'])  # adjustment factor if input data is not correctly scaled.
correction_factor_total_export = float(os.environ['CORRECTION_FACTOR_TOTAL_EXPORT'])  # adjustment factor if input data is not correctly scaled.

modbus_port = 502  # hardcoded since we can re-map it via docker.

print("Using config from env:")
print("----------------------")
print("DEBUG: " + "{0!s}".format(debug))
print("----------------------")
print("MQTT_USERNAME: " + mqtt_username)
print("MQTT_PASSWORD: " + "****")
print("MQTT_HOST: " + mqtt_host)
print("MQTT_PORT: " + "{0:d}".format(mqtt_port))
print("----------------------")
print("MQTT_TOPIC_CURRENT_POWER: " + mqtt_topic_current_power)
print("MQTT_TOPIC_TOTAL_IMPORT: " + mqtt_topic_total_import)
print("MQTT_TOPIC_TOTAL_EXPORT: " + mqtt_topic_total_export)
print("----------------------")
print("CORRECTION_FACTOR_CURRENT_POWER: " + "{0:0.2f}".format(correction_factor_current_power))
print("CORRECTION_FACTOR_TOTAL_IMPORT: " + "{0:0.2f}".format(correction_factor_total_import))
print("CORRECTION_FACTOR_TOTAL_EXPORT: " + "{0:0.2f}".format(correction_factor_total_export))
print("----------------------")

lock = threading.Lock()

server_started = False
last_message_received = datetime.datetime.now()

current_power = "0"
total_export = "0"
total_import = "0"

mqtt_connected = False
mqtt_client = setup_mqtt()
mqtt_client.loop_start()

values_ready = False
print("Waiting for data from MQTT broker. Application will continue once import and export are available.")

while not values_ready:
    time.sleep(1)
    lock.acquire()
    if total_import != '0' and total_export != '0':
        print("Received required data. Continuing with start procedure.")
        values_ready = True
    lock.release()


# initialize modbus server
lock.acquire()
data_block = ModbusSparseDataBlock({

    40001: [21365, 28243],
    40003: [1],
    40004: [65],
    40005: [ord("F"), ord("r"), ord("o"), ord("n"), ord("i"), ord("u"), ord("s"), 0, 0, 0, 0, 0, 0, 0, 0, 0,  # Manufacturer
            ord("S"), ord("m"), ord("a"), ord("r"), ord("t"), ord(" "), ord("M"), ord("e"), ord("t"), ord("e"), ord("r"), ord(" "), ord("6"), ord("3"), ord("A"), 0,  # Device Model
            0, 0, 0, 0, 0, 0, 0, 0,  # Options N/A
            0, 0, 0, 0, 0, 0, 0, 0,  # Software Version  N/A
            48, 48, 48, 48, 48, 48, 48, 49, 0, 0, 0, 0, 0, 0, 0, 0,  # Serial Number: 00000
            240],  # Modbus TCP Address
    40070: [213],
    40071: [124],
    40072: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0],

    40196: [65535, 0],
})

slave_context = ModbusSlaveContext(
    di=data_block,
    co=data_block,
    hr=data_block,
    ir=data_block,
)

server_context = ModbusServerContext(slaves=slave_context, single=True)

lock.release()


bind_address = ("", modbus_port)
run_interval = 2

# continuously update the registers.
update_context_timer = RepeatedTimer(run_interval, update_server_context, server_context)

# continuously update server state.
check_server_timer = RepeatedTimer(run_interval, check_server)

while True:
      time.sleep(5000)