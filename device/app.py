#!/usr/bin/env python

from flask import Flask, request
from flask_socketio import SocketIO
import os

from device.Device import Device


app = Flask(__name__)
sockethost = SocketIO(app, async_mode="threading")

# Global device instance
device = None

def create_app(config_file, salt):
    """ Application factory for initializing the app and device. """
    global device
    device = Device(config_file, sockethost, salt)

    # Define all routes and socket events
    app.route("/")(device.index)
    app.route("/favicon.ico")(device.favicon)
    app.route("/get_config", methods=["GET"])(device.get_config)
    app.route("/get_files", methods=["GET"])(device.get_files)
    app.route("/save_config", methods=["POST"])(device.save_config)
    app.route("/restartConnections", methods=["GET"])(device.on_restart_connections)
    app.route("/emitFiles", methods=["GET"])(device.emitFiles)
    app.route("/scan", methods=["GET"])(device.on_scan)

    sockethost.on("connect")(device.on_local_dashboard_connect)
    sockethost.on("disconnect")(device.on_local_dashboard_disconnect)

    # Start the background task for the device
    sockethost.start_background_task(device.run)

    return app

# Initialize the app only once
if __name__ != "__main__":
    # This branch runs when the script is imported by Gunicorn
    # Prefer new env var; fall back to legacy for compatibility
    config_file = os.getenv("STORAGE_TOOL_DEVICE_CONFIG_FILE")
    if not config_file:
        raise ValueError("STORAGE_TOOL_DEVICE_CONFIG_FILE environment variable must be set (legacy: CONFIG_FILE)")
    salt = os.getenv("SALT")
    create_app(config_file, salt)

if __name__ == "__main__":
    # For local debugging and development
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", type=str, required=False, default="config/config.yaml", help="Config file for this instance")
    parser.add_argument("-s", "--salt", type=str, required=False)
    args = parser.parse_args()

    # Run the application using the provided configuration
    create_app(args.config, args.salt)
    port = os.environ.get("STORAGE_TOOL_DEVICE_CONFIG_PORT", "8811")
    port = int(port) if port else 8811
    sockethost.run(app=app, host="0.0.0.0", port=port)
