import requests
import json
import pandas as pd 
import numpy as np
import paho.mqtt.client as paho
import string
import random
import time
import app_config.app_config as cfg
import os
import datetime
import pytz
import math

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


config = cfg.getconfig()
PUBLIC_DATACENTER_URL = config["api"].get("public_datacenter_url", "NA")

topic_id = "60ae9143e284d016d3559dfb"
topic_line1 = "u/{}/GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric/m".format(topic_id)
topic_line2 = "u/{}/GAP_GAP03.PLC03.SCHENCK2_FEED_RATE/m".format(topic_id)
topic_line3 = "u/{}/GAP_GAP04.PLC04.MLD1_DATA_Anode_Number/m".format(topic_id)
topic_line4 = "u/{}/GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric/m".format(topic_id)
topic_line5 = "u/{}/GAP_GAP04.PLC04.MLD2_DATA_Anode_Number/m".format(topic_id)
topic_line6 ="u/{}/alert_time".format(topic_id)
topic_line7 ="u/{}/alert_time_OFF".format(topic_id)


topic_line_id = {
    # "GAP_GAP01.PLC01.RHODAX_GAP_SP": "Rhodax gap working setpoint",
    "GAP_GAP02.PLC02.H070_TIC_01_SP": "Preheating screw temperature setpoint",
    "GAP_GAP02.PLC02.H090_TIC_01_SP": "Heat exchanger temperature setpoint",
    "GAP_GAP02.PLC02.H100_TIC_01_SP": "Unloading section temperature setpoint",
    "GAP_GAP02.PLC02.H110_TIC_01_SP": "Jacketed pipes temperature setpoint",
    "GAP_GAP02.PLC02.H120_TIC_01_SP": "Pitch tank 2 temperature setpoint",
    "GAP_GAP02.PLC02.H130_TIC_01_SP": "Pitch tank 1 temperature setpoint",
    "GAP_GAP02.PLC02.H140_TIC_01_SP": "Pitch melter temperature setpoint",
    "GAP_GAP01.PLC01.M080_TIC_01_SP": "Mixer air temperature setpoint",
    "GAP_GAP01.PLC01.M120_TIC_01_SP": "Bagfilter air temperature setpoint",
    "GAP_GAP03.PLC03.NEW_FORMULA_KGS": "Green on Total Paste",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKGS": "Green on Total Paste",
    "GAP_GAP03.PLC03.NEW_FORMULA_KFR": "Fines on solid",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKFR": "Fines on solid",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKFP": "Fresh Product",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKCB": "Coke and baked",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKDM": "Solid Material",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKG": "Grains on solid material",
    "GAP_GAP03.PLC03.NEW_FORMULA_KLP": "Liquid Pitch percentage",
    "GAP_GAP03.PLC03.NEW_FORMULA_KBS": "Baked on coke and baked",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKTP": "Total Paste",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKLP": "Liquid Pitch percentage",
    "GAP_GAP03.PLC03.NEW_FORMULA_FKBS": "Baked on coke and baked",
    "GAP_GAP02.PLC02.H070_TIC_01_SP": "Preheating screw temperature setpoint",
    "GAP_GAP01.PLC01.M080_TIC_01_SP": "Mixer air temperature setpoint",
    "GAP_GAP01.PLC01.M120_TIC_01_SP": "Bagfilter air temperature setpoint",
    "GAP_GAP04.PLC04.K363_K040A_MVF_01_VTK": "Vibrating motor setpoint for vibration time mould 1",
    "GAP_GAP04.PLC04.K363_K050A_MVF_01_VTK": "Vibrating motor setpoint for vibration time mould 2"
}

tags = [f"{parameter_id}" for parameter_id in topic_line_id.keys()]

port = 1883

client = paho.Client()

def on_log(client, userdata, obj, buff):
    print ("log:" + str(buff))


def on_connect(client, userdata, flags, rc):
    client.subscribe(topic_line1)
    client.subscribe(topic_line2)
    client.subscribe(topic_line3)
    client.subscribe(topic_line4)
    client.subscribe(topic_line5)
    print ("Connected!")
    print("updated code Version: 01/01/24")


count1 = 0  
count2 = 0 
count3 = 0
count4 = 0
count5 = 0
unique_responses1 = []  
unique_responses2 = []  
unique_responses3 = []
unique_responses4 = []
unique_responses5 = []


unique_timestamps1 = set() 
unique_timestamps2 = set()  
unique_timestamps3 = set() 
unique_timestamps4 = set()
unique_timestamps5 = set()  

alertList = []
attachment_path = ''

result1 = 0
result2 = 0

flag1 = False
flag2 = False


isAlertActive = False
isAlertActiveM1 = False
isAlertActiveM2 = False

atAlert = 0

deAlert = 0

list_of_dicts1 = []
list_of_dicts2 = []
result_task_table = {}

last_task_id = ''

n = 1 # no of alert in table



def getValues(tagList):
    url = "https://data.exactspace.co/kairosapi/api/v1/datapoints/query"
    d = {
        "metrics": [
            {
                "tags": {},
                "name": "",
                "aggregators": [
                    {
                        "name": "avg",
                        "sampling": {
                            "value": "1",
                            "unit": "minutes"
                        }
                    }
                ]
            }
        ],
        "plugins": [],
        "cache_time": 0,
        "cache_time": 0,
        "start_relative": {
            "value": "20",
            "unit": "minutes"
        }
    }
    finalDF = pd.DataFrame()
    for tag in tagList:
        d['metrics'][0]['name'] = tag
        res = requests.post(url=url, json=d)
        values = json.loads(res.content)
        df = pd.DataFrame(values["queries"][0]["results"][0]['values'], columns=['time', values["queries"][0]["results"][0]['name']])
        finalDF = pd.concat([finalDF, df], axis=1)

    finalDF = finalDF.loc[:, ~finalDF.columns.duplicated()]
    finalDF.dropna(subset=['time'], inplace=True)
    # finalDF['time'] = pd.to_datetime(finalDF['time'], unit='ms').dt.strftime('%d-%m-%y %H:%M:%S')
    return finalDF

def get_task_content(id):
    # URL for the GET request
    url = f'https://data.exactspace.co/exactapi/activities/{id}'

    # Make the GET request
    response = requests.get(url)

    # Check if the GET request was successful (status code 2xx)
    if response.status_code // 100 == 2:
        task_data = response.json()
        return task_data.get("content", [])
    else:
        print(f"Failed to retrieve content for task with ID {id}. Status code: {response.status_code}")
        return []

def append_to_content(id):

    # Get existing content
    global result_task_table
    existing_content = get_task_content(id)

    # Append new content item to the existing content list
    new_content_item = [
        {
            "type": "text",
            "value": "Setpoints Changes."
        },
        {
            "type": "table",
            "value": [
                ["Tag", "Initial Values", "Final Values"]
            ]
        }
    ]
    existing_content.append(new_content_item[0])
    existing_content.append(new_content_item[1])

    # Prepare data for PATCH request
    data = {
        "completedBy": "Pulse",
        "status": "done",
        "content": existing_content
    }

    # print(data)

    # URL for the PATCH request
    url = f'https://data.exactspace.co/exactapi/activities/{id}'

    # Iterate over recommendations and append values to 'content'
    for tag, values in result_task_table.items():
        actual_value = str(values[0])  # Convert to string if needed
        recommended_value = str(values[1])

        # Append the information to 'content'
        data["content"][2]["value"].append([tag, actual_value, recommended_value])
    
    # Convert the data to a JSON string
    json_data = json.dumps(data)

    # Set the headers to indicate that you are sending JSON data
    headers = {"Content-Type": "application/json"}

    # Make the PATCH request
    response = requests.patch(url, data=json_data, headers=headers)

    # Print the response (optional, for debugging purposes)
    print(response.text)

    # Check if the PATCH request was successful (status code 2xx)
    if response.status_code // 100 == 2:
        print(f"Content appended successfully to task with ID {id}.")
    else:
        print(f"Failed to append content to task with ID {id}. Status code: {response.status_code}")

def postDataApi(outputTag,store_vals_to_post):
    url="https://data.exactspace.co/kairosapi/api/v1/datapoints"
   
    body = [{
        "name": str(outputTag),
        "datapoints": store_vals_to_post,
        "tags":{"type":"historic"}}]
    
    res = requests.post(url = url,json = body,stream=True)
    print(res.content)
    print(res)
    return res.status_code

def createActivityLink(id):
    try:
        return  "https://data.exactspace.co/pulse-master/my-tasks/" + id
    except Exception as e:
        print("error in create link for task.")
        pass  # Do nothing for now, but you can add error handling logic

    return "#"  # Example: Return a default link

def uploadRefernceData(fileName):
    global attachment_path
    print(fileName)
    print(type(fileName))
    str_fileName = str(fileName)
    
    path = ""
    files = {'upload_file': open(str(path+str_fileName),'rb')}
   
    url=config['api']['meta']+"/attachments/tasks/upload"

    response = requests.post(url, files=files)
    print ("uploading")
    print (url)
    print ("+"*20)

    if(response.status_code==200):
        status ="success"
        data = response.content
        # Parse the JSON data
        parsed_data = json.loads(data)
        # Access the "name" from the parsed JSON data
        name = parsed_data['result']['files']['upload_file'][0]['name']
        attachment_path ="https://data.exactspace.co/exactapi/attachments/tasks/download/"+name

        
    else:
        status= (str(response.status_code) + str(response.content))
    print (response.status_code, response.content)

    return status

def task_attachment(alert_time):
    global attachment_path
    # Define the URL
    url = "http://10.0.0.7:1789/alertplot"

    # Define the data you want to send in the POST request (if needed)
    data = {
        "alertTime_start": alert_time,
    }

    # Set the headers to indicate that you are sending JSON data
    headers = {"Content-Type": "application/json"}

    # Make the POST request
    response = requests.post(url, json=data, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.content.decode('utf-8')  # Decode bytes to string assuming utf-8 encoding
        print("getting response: ", data)
        fileName = data
        uploadRefernceData(fileName)
        
    else:
        print("Not getting any responese. response code: ", response.status_code)

    
    return


def calculation(input_df):
    global isAlertActive, isAlertActiveM1, isAlertActiveM2, list_of_dicts1, list_of_dicts2, result_task_table, last_task_id
    try:
        # Copy the input DataFrame to avoid modifying the original data
        df = input_df.copy()
        df = df.dropna()

        # Filter the DataFrame based on conditions
        df = df[(df['SCHENCK2_FEED_RATE'] >= 4000) & (df['SCHENCK2_FEED_RATE'] < 6700) 
                & (df['Geo_density'] >= 1.56) & (df['Geo_density'] <= 1.69)
                ]

        df = df[df['anode_number'] % 1 == 0]
        df['anode_number'] = df['anode_number'].astype(int)
        df = df.sort_values(by='anode_number')
        
        #  Find consecutive duplicate values in the column and mark them for removal
        df['to_remove'] = df['anode_number'] == df['anode_number'].shift(1)

        df = df[df['to_remove'] != True]

        df['Geo_density'] = df['Geo_density'].round(3)

        # Reset the index to use the default integer index
        df.reset_index(drop=True, inplace=True)
        
        benchmark = 1.645
        
        
        
        

        # print("benchmark:", benchmark)

        df['z_scores'] = (df['Geo_density'] - benchmark) 

        
        negative_z_scores = df[df['z_scores'] < 0]

        print("input_df......................:")
        print(input_df)

        print("df..................: ")
        print(df)

        
    
        print("isAlert: ",isAlertActive)

        if isAlertActive and not df.empty:
            alert_time = df.iloc[0]['timestamp']
            alert_time = alert_time/1
            outputTag = 'GAP_DENSITY_ALERT'
            post_alert_time = [[alert_time, 1]]
            postDataApi(outputTag, post_alert_time)

        elif not isAlertActive and not df.empty:
            alert_time = df.iloc[0]['timestamp']
            alert_time = alert_time/1
            outputTag = 'GAP_DENSITY_ALERT'
            post_alert_time = [[alert_time, 0]]
            postDataApi(outputTag, post_alert_time)
       
        if not negative_z_scores.empty:
            result_size = len(negative_z_scores)
            if result_size >= 3 and len(df) >=4:
                alert_time = negative_z_scores.iloc[0]['timestamp']
                alert_time = df.iloc[0]['timestamp']
                tableDf = getValues(tags)
                row = tableDf.tail(1)
                row = row.round(4)
                row = row.drop(columns=['time'])
                list_of_dicts1 = row.to_dict('records')
                alert_time  = alert_time/1
                outputTag = 'GAP_DENSITY_ALERT'
                post_alert_time = [[alert_time, 1]]
                postDataApi(outputTag, post_alert_time)
                print('Inside Alert Generate Module')
                return alert_time
    

        
        if negative_z_scores.empty and not df.empty and len(df) >= 4 and isAlertActive:
            print("alert deactiveted")
            
            isAlertActive = False
            isAlertActiveM2 = False
            isAlertActiveM1 = False
            alert_time = df.iloc[0]['timestamp']

            ms_post_alert_time = np.int64(alert_time)
            ms_post_alert_time_as_int = int(ms_post_alert_time)

            ms_post_alert_time = alert_time
            data = {"time": ms_post_alert_time_as_int}
            json_data2 = json.dumps(data)
            client.publish(topic_line7, json_data2)
            print("data published........")


            tableDf = getValues(tags)
            row = tableDf.tail(1)
            row = row.round(4)
            row = row.drop(columns=['time'])
            list_of_dicts2 = row.to_dict('records')
            if list_of_dicts2 and list_of_dicts1:
                result_task_table = {
                                        key: (list_of_dicts1[0][key], list_of_dicts2[0][key])
                                        for key in list_of_dicts1[0]
                                        if list_of_dicts1[0][key] != list_of_dicts2[0][key] and not (math.isnan(list_of_dicts1[0][key]) or math.isnan(list_of_dicts2[0][key]))
                                    }
    
            alert_time = alert_time/1
            outputTag = 'GAP_DENSITY_ALERT'
            post_alert_time = [[alert_time, 0]]
            postDataApi(outputTag, post_alert_time)
            append_to_content(last_task_id)
            print("inside deactivate module........")
            return 0

        return 0
    except Exception as e:
        print("Error in calculation:", e)
        return 0

def process_responses():
    global count1, count2, count3, count4, count5, unique_responses1, unique_responses2, unique_timestamps1, unique_timestamps2, unique_timestamps3, unique_responses3, n, unique_responses5, unique_responses4, unique_timestamps4, unique_timestamps5
    global result1, result2, alertList, isAlertActive, flag1, flag2
    sample_size = 60
    print("...Geometric1 Density: {} Geometric2 Density: {} Schenk2: {} Anode number1: {} Anode number2: {}".format(count1,count4, count2, count3, count5))


    try:
        # Check if we have received enough packets for both topics
        if count1 >= sample_size and count2 >= sample_size and count3 >= sample_size and not flag1:
            # Create DataFrames when sample size is met
            df1 = pd.DataFrame(unique_responses1)
            df2 = pd.DataFrame(unique_responses2)
            df3 = pd.DataFrame(unique_responses3)


            # Merge the DataFrames based on the 'timestamp' column
            merged_df = df1.merge(df2, on="timestamp", how="inner")
            merged_df = merged_df.merge(df3, on='timestamp', how="inner")
        
            # Calculate results based on merged data
            result1 = calculation(merged_df)
        

            count1 = 0
            count3 = 0

            unique_responses1[:] = []
            
            unique_responses3[:] = []
    
            unique_timestamps1 = set()
           
            unique_timestamps3 = set()

            flag1 = True
            
    
        if count2 >= sample_size and count4 >= sample_size and count5 >= sample_size and not flag2:
            # Create DataFrames when sample size is met
            df6 = pd.DataFrame(unique_responses2)
            df4 = pd.DataFrame(unique_responses4)
            df5 = pd.DataFrame(unique_responses5)

            merged_df = df6.merge(df4, on="timestamp", how="inner")
            merged_df = merged_df.merge(df5, on='timestamp', how="inner")

            # # Calculate results based on merged data
            result2 = calculation(merged_df)
            

            count4 = 0 
            count5 = 0
            unique_responses4[:] = []
            unique_responses5[:] = []
            unique_timestamps4 = set()
            unique_timestamps5 = set()

            flag2 = True

        if(result2 != 0 and result1 != 0 and not isAlertActive):
               # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result2/1000  # Convert milliseconds to seconds
                mld = 'MLD1 & MLD2'
                task_attachment(epoch_timestamp)

                create_task(epoch_timestamp, mld)

                isAlertActive = True

                print("alert Generated")
                flag1 = False
                flag2 = False
                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()
        elif(result2 == 0 and result1 != 0 and not isAlertActive):
                # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result1/1000  # Convert milliseconds to seconds
                mld = "MLD1"
                task_attachment(epoch_timestamp)

                create_task(epoch_timestamp, mld)

                isAlertActive = True
                print("alert Generated")

                flag1 = False
                flag2 = False

                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()
        elif(result2 != 0 and result1 == 0 and not isAlertActive):
                # Replace epoch_timestamp with your actual epoch timestamp
                epoch_timestamp = result2/1000  # Convert milliseconds to seconds

                mld = "MLD2"
                task_attachment(epoch_timestamp)

                create_task(epoch_timestamp, mld)

                isAlertActive = True

                print("alert Generated")
                flag1 = False
                flag2 = False
                result1 = 0
                result2 = 0
                count1 = 0
                count2 = 0
                count3 = 0
                count4 = 0 
                count5 = 0
                unique_responses1[:] = []
                unique_responses2[:] = []
                unique_responses3[:] = []
                unique_responses4[:] = []
                unique_responses5[:] = []

                unique_timestamps1 = set()
                unique_timestamps2 = set()
                unique_timestamps3 = set()
                unique_timestamps4 = set()
                unique_timestamps5 = set()
        
        if(flag1 and flag2):
            flag1 = False
            flag2 = False
            count2 = 0
            unique_responses2[:] = []
            unique_timestamps2 = set()
        
        if(isAlertActive and count1 >= sample_size and count2 >= sample_size and count3 >= sample_size and count4 >= sample_size and count5 >= sample_size):
            result1 = 0
            result2 = 0
            count1 = 0
            count2 = 0
            count3 = 0
            count4 = 0 
            count5 = 0
            unique_responses1[:] = []
            unique_responses2[:] = []
            unique_responses3[:] = []
            unique_responses4[:] = []
            unique_responses5[:] = []
            # post_alert_time = [[alert_time, 0]]
            unique_timestamps1 = set()
            unique_timestamps2 = set()
            unique_timestamps3 = set()
            unique_timestamps4 = set()
            unique_timestamps5 = set()
            flag1 = False
            flag2 = False

        if(result1 == 0 and result2 == 0 and count1 >= sample_size and count2 >= sample_size and count3 >= sample_size and count4 >= sample_size and count5 >= sample_size):
            result1 = 0
            result2 = 0
            count1 = 0
            count2 = 0
            count3 = 0
            count4 = 0 
            count5 = 0
            unique_responses1[:] = []
            unique_responses2[:] = []
            unique_responses3[:] = []
            unique_responses4[:] = []
            unique_responses5[:] = []

            unique_timestamps1 = set()
            unique_timestamps2 = set()
            unique_timestamps3 = set()
            unique_timestamps4 = set()
            unique_timestamps5 = set()
            flag1 = False
            flag2 = False

        # manage stack overflow
        if(count1 >= 150 or count2 >= 150 or count3 >= 150 or count4 >= 150 or count5 >= 150):
            result1 = 0
            result2 = 0
            count1 = 0
            count2 = 0
            count3 = 0
            count4 = 0 
            count5 = 0
            unique_responses1[:] = []
            unique_responses2[:] = []
            unique_responses3[:] = []
            unique_responses4[:] = []
            unique_responses5[:] = []
            # post_alert_time = [[alert_time, 0]]
            unique_timestamps1 = set()
            unique_timestamps2 = set()
            unique_timestamps3 = set()
            unique_timestamps4 = set()
            unique_timestamps5 = set()
            flag1 = False
            flag2 = False



    except Exception as e:
        print("An error occurred in process_responses:", e)
       
def on_message(client, userdata, message):
    global count1, count2,count3,count4, count5, unique_timestamps1, unique_responses1, unique_timestamps2, unique_responses2, unique_timestamps3, unique_responses3, unique_responses4, unique_responses5, unique_timestamps4, unique_timestamps5  # Declare global variables

    incoming_msg = json.loads(message.payload)
    topic = message.topic
    # print(topic) # debug

    if topic == topic_line1:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            geo_density = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "Geo_density": geo_density,
                "timestamp":timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps1:
                unique_timestamps1.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses1.append(result)  # Add the response to the list of unique responses
                count1 += 1  # Increment the count variable

            process_responses()
        
    if topic == topic_line2:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            SCHENCK2_FEED_RATE = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "SCHENCK2_FEED_RATE": SCHENCK2_FEED_RATE,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps2:
                unique_timestamps2.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses2.append(result)  # Add the response to the list of unique responses
                count2 += 1  # Increment the count variable
            process_responses()

    if topic == topic_line3:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            anode_number = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "anode_number": anode_number,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps3:
                unique_timestamps3.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses3.append(result)  # Add the response to the list of unique responses
                count3 += 1  # Increment the count variable
            process_responses()

    if topic == topic_line4:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            geo_density = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "Geo_density": geo_density,
                "timestamp":timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps4:
                unique_timestamps4.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses4.append(result)  # Add the response to the list of unique responses
                count4 += 1  # Increment the count variable

            process_responses()

    if topic == topic_line5:
    # Ensure that the message is a list with at least one element
        if isinstance(incoming_msg, list) and len(incoming_msg) > 0:
            data = incoming_msg[0]  # Assuming the first element contains the data

            # Extract 'r' and 't' values
            anode_number = data.get("r", None)
            timestamp = int(data.get("t", None))

            # Create a dictionary to store the values
            result = {
                "anode_number": anode_number,
                "timestamp": timestamp
            }

            # Check if this result's timestamp is unique
            if timestamp not in unique_timestamps5:
                unique_timestamps5.add(timestamp)  # Add the timestamp to the set of unique timestamps
                unique_responses5.append(result)  # Add the response to the list of unique responses
                count5 += 1  # Increment the count variable
            process_responses()
    

def sendAlmEmail(alertArg):
    global alertList, n
    unitName, SiteName, CustomerName = 'GAP', 'GAP Mahan', 'Green Anode Plant, Mahan'
    alertLevel = 'warning'
     
    n = 1

    if len(alertList) < 1:
        alertList.append(alertArg)
    else:
        alertList = []
        alertList.append(alertArg)

    emailTemplate = os.path.join(os.getcwd(), 'templates/almEmailTemplate.html')
    
    with open(emailTemplate, 'r') as f:
        s = f.read()

    if PUBLIC_DATACENTER_URL != 'NA':
        logoLink = 'img src="{}pulse-files/email-logos/logo.png"'.format(PUBLIC_DATACENTER_URL)
        s = s.replace('img src="#"', logoLink)
    else:
        logoLink = 'https://data.exactspace.co/pulse-files/email-logos/logo.png'
        s = s.replace('img src="#"', 'img src="{}"'.format(logoLink))

        
    if str(alertLevel) == "warning":
        s = s.replace("""<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:red; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active</b></td>""",
                      """<td colspan="3" align="left" style="border-bottom: solid 1px #CACACA; color:#fd7e14; padding-bottom: 5px; font-size: 15px;"><b>Alarms Active</b></td>"""
                     )
        
    # Replace the placeholder with the actual link
    s = s.replace("""<a style="color: #fff; text-decoration:none;" href="#" id = 'task_link'>More Details</a>""", 
                  """<a style="color: #fff; text-decoration:none;" href="{}" id = 'task_link'>More Details</a>""".format(alertArg.get("task_link")))


    s = s.replace('UnitName', unitName)
    s = s.replace('SiteName', SiteName)
    s = s.replace('CustomerName', CustomerName)
    
    devTable = ''

    if alertList:
        devTable = '<tbody id="devList">'
        for alert in alertList:
            try:
                devTable += '''
                    <tr>
                        <td align="center" width="40" style="border-bottom: solid 1px #CACACA;">{}</td>
                        <td align="left" style="font-size: 13px; border-bottom: solid 1px #CACACA;">{}</td>
                        <td align="left" width="100" style="font-size: 13px; border-bottom: solid 1px #CACACA;">{}</td>
                    </tr>
                '''.format(n, alert.get('desc', ''), alert.get('alert_time', ''))
                n += 1
            except Exception as e:
                print('Error in creating a table of alert', e)
                return

        s = s.replace('<tbody id="devList">', devTable)

    else:
        print('Alarm with no open criticalTags, mail not sent!')
        return

    with open(os.path.join(os.getcwd(), 'templates/almEmailTemp.html'), 'wb') as f:
        f.write(s.encode('utf-8'))

    with open(os.path.join(os.getcwd(), 'templates/almEmailTemp.html'), 'r') as f:
        msg_body = f.read()

    try:
        url = config['api']['meta'].replace('exactapi', 'mail/send-mail')
        payload = json.dumps({
            'from': 'vikram.k@exactspace.co',
            'to': ['vikram.k@exactspace.co',
                   'ashlin.f@exactspace.co',
        'anisha.jonnalagadda@adityabirla.com',
        'sayan.dey@adityabirla.com',
        'anurag.gaurav@adityabirla.com',
        'arun@exactspace.co'],
            'html': msg_body,
            'bcc': [],
            'subject': 'GAP Density Going Low!',
            'body': msg_body
        })
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=payload, headers=headers)

        if response.text == 'Success':
            return 'Success'
        else:
            print('Error in sending mail', response.status_code)
            return 'Fail'

    except Exception as e:
        print('Error in sending mail', e)
        return 'Fail'

def create_task(alert_time, mld):

    ms_post_alert_time = 1000*alert_time
    
    # data = {"time": ms_post_alert_time}
    # json_data1 = json.dumps(data)
    # client.publish(topic_line6, json_data1)

    if mld == 'MLD1 & MLD2':
        data = {"mould12": ms_post_alert_time}
        json_data1 = json.dumps(data)
        client.publish(topic_line6, json_data1)
    elif mld == 'MLD1':
        data = {"mould1": ms_post_alert_time}
        json_data1 = json.dumps(data)
        client.publish(topic_line6, json_data1)
    elif mld == 'MLD2':
        data = {"mould2": ms_post_alert_time}
        json_data1 = json.dumps(data)
        client.publish(topic_line6, json_data1)
    
   

    global attachment_path, last_task_id
    # Define the URL where you want to make the POST request
    url = "https://data.exactspace.co/exactapi/activities"

    # Replace epoch_timestamp with your actual epoch timestamp
    epoch_timestamp = alert_time

    # Specify the local timezone (e.g., 'US/Eastern', 'Europe/London', etc.)
    local_timezone = pytz.timezone('Asia/Kolkata')

    # Convert the epoch timestamp to a datetime object in the local timezone
    dt = datetime.datetime.fromtimestamp(epoch_timestamp, tz=local_timezone)

    # Add 2 hours to the datetime object
    dt_alert_due_time = dt + datetime.timedelta(minutes=30)

    # Format the datetime object as a string in the desired format
    formatted_date_alert_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_date_alert_due_time = dt_alert_due_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")

    data = {
    "type": "task",
    "voteAcceptCount": 0,
    "voteRejectCount": 0,
    "acceptedUserList": [],
    "rejectedUserList": [],
    "dueDate": formatted_date_alert_due_time,
    "assignee": "6149b9acf1902b2b7aecf9b1",
    "source": "Anode Forming",
    "team": "Operations",
    "createdBy": "5f491bb942ba5c3f7a474d15",
    "createdOn": formatted_date_alert_time,
    "siteId": "5cef6b03be741b86a8c893a0",
    "subTasks": [],
    "chats": [],
    "taskPriority":"high",
    "updateHistory": [{
        "action": "This task is created by Pulse.",
        "by": "",
        "on": formatted_date_alert_time
    }],
    "unitsId": "60ae9143e284d016d3559dfb",
    "collaborators": [
        "632d3bd36d161904360db797", #intern
        '61431baf1c46e3435ff50ac7', #sayan
        '5f491bb942ba5c3f7a474d15', #anisha
        '6149b95af1902b2b7aecf9ac',  # anurag
        '5c591d697dc9e324ee08a456'
    ],
    "status": "--",
    "content": [
        {
            "type": "title",
            "value": "GAP Density Going Low From 1.65 Due To {}".format(mld)
        }
    ],
    "taskGeneratedBy": "system",
    "incidentId": "",
    "category": "",
    "sourceURL": "",
    "notifyEmailIds": [
        "vikram.k@exactspace.co",
        'ashlin.f@exactspace.co',
        'anisha.jonnalagadda@adityabirla.com',
        'sayan.dey@adityabirla.com',
        'anurag.gaurav@adityabirla.com',
        'arun@exactspace.co'
    ],
    "chat": [],
    "taskDescription": "<p><img src=\"{}\"></p>".format(attachment_path),
    "triggerTimePeriod": "days",
    "viewedUsers": [],
    "completedBy": "",
    "equipmentIds": [],
    "mentions": [],
    "systems": []
}
    # Convert the data to a JSON string
    json_data = json.dumps(data)

    # Set the headers to indicate that you are sending JSON data
    headers = {"Content-Type": "application/json"}
    print(data)

    # Make the POST request
    response = requests.post(url, data=json_data, headers=headers)

    # Check the response
    if response.status_code == 200:
        print("Task Create request was successful")
        # print("Response:", response.text)
        response_data = response.json()

        # Extract the 'id' field from the response
        id_value = response_data.get('id')
        last_task_id = id_value
        task_link = createActivityLink(id_value)

        sendAlmEmail({"alert_time":formatted_date,"desc":"GAP Density Going Low From 1.65 Due To {}".format(mld),"task_link":task_link})
    else:
        print("Task Create request failed with status code:", response.status_code)
        return


try:
    client.username_pw_set(username=config["BROKER_USERNAME"], password=config["BROKER_PASSWORD"])
except:
    pass
client.connect('10.1.0.10', port)
client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
