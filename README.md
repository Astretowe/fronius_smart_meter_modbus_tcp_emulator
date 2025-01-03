# fronius_smart_meter_modbus_tcp_emulator
Emulate a Fronius Modbus TCP Smart Meter in a docker container.

The emulator can provide a Modbus TCP server interface which satisfies the requirements of a Fronius inverter.
The input data is provided via MQTT.
For configuration, please see the docker-compose file.

Usage:
- Pull the repo to a local folder.
- Adjust compose file.
- docker compose up -d --build fronius-sim
