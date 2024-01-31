# Geometric Density Alert Module

## Description
The Geometric Density Alert Module is a Python script that subscribes to MQTT topics related to GAP (Green Anode Plant) density monitoring. It processes incoming data, performs density calculations, and generates alerts based on predefined conditions.

## Key Features
- Monitors multiple MQTT topics for Geo Density, Feed Rate, and Anode Number.
- Calculates density based on specified conditions.
- Generates alerts and tasks when "Geometric Density" is going low.
- Sends email notifications with create task at Pulse.
- Posts data at Kairos for historical tracking.

## Script Structure
1. Import necessary libraries and modules.
2. Configure script parameters using the `app_config` module.
3. Define MQTT topics for different data streams.
4. Initialize MQTT client and set up connection callbacks.
5. Define functions for data processing, alert generation, and API interactions.
6. Connect to the MQTT broker and start the event loop.

## Script Components
- `on_message`: Processes incoming data from MQTT topics.
- `process_responses`: Generates a dataframe for calculation and then calls the calculation method.
- `calculation`: Returns density results based on predefined parameters.
- `create_task`: Posts data to Pulse for creating a task.
- `sendAlmEmail`: Posts data to an external API for sending an email.
- `task_attachment`: Posts data to an external API for getting a plot URL for Pulse task.
- `uploadRefernceData`: After getting a plot URL, it needs to be in a special folder so that Pulse detects this file to attach at UI, so sending the URL and getting a response of that special folder link.
- `createActivityLink`: Returning Pulse task link so that it would be attached with an email.
- `postDataApi`: Posting Alert timestamp to Kairos for historical tracking.

## Usage
1. Ensure the required libraries are installed (`requests`, `pandas`, `numpy`, `paho.mqtt.client`).
2. Configure MQTT broker details and API endpoints in the `app_config` module.
3. Run the script to start monitoring and processing MQTT data.

## Console Output
- **Logs**: Display connection status and script updates.
- **Data Processing**: Show processed dataframes and calculations.
- **Alert Generation**: Indicate when alerts are generated and tasks created.
- **Email Notifications**: Sent to specified recipients with task details.

**Note:** This script is designed for monitoring GAP density and generating alerts.
