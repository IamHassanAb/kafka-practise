# app_b.py
import streamlit as st
from ctypes import *
# CDLL(r"C:\Users\MustafaAli\anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll")

from confluent_kafka import Consumer, KafkaError
from kafka import KafkaConsumer
import json
import pandas as pd
import time

# Set up Kafka consumer configuration
# def read_ccloud_config(config_file):
#     conf = {}
#     with open(config_file) as fh:
#         for line in fh:
#             line = line.strip()
#             if len(line) != 0 and line[0] != "#":
#                 parameter, value = line.strip().split('=', 1)
#                 conf[parameter] = value.strip()
#     return conf
    

def main():
    st.title("📊 Live Poll Dashboard")
    st.divider()

    total_sale = 0
    total_vol = 0
    num_orders = 0
    records = []
    answer_ids=set()

    c = KafkaConsumer( 
                      bootstrap_servers="localhost:9092", 
                      session_timeout_ms = 45000,
                      enable_auto_commit=True,
                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    c.subscribe(['orders-topic'])
    
    toast=st.empty()
    placeholder=st.empty()
    st_all_orders=st.empty()
    st_all_records=st.empty()
        
    while True:
        st.toast('Fetching')
        for msg in c:
            # print('{}'.format(message.value))
        
            # msg = c.poll(1.0)
        
            

            if msg is None:
                st.toast('Nothing Recieved')
                continue
                
            # if msg.error():
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         continue
            #     else:
            #         print(msg.error())
            #         break
            
            if msg:
            
                time.sleep(0.5)
        
                st.toast('Poll Response Recieved')
                
                time.sleep(1)
                poll_data = msg.value
                row = {"answer_id":poll_data["answer_id"]}
                for qa in poll_data["answer_array"]:
                    row.update(qa)

                '''
                [
                    {
                        "answer_id":2000,
                        "How was the Conference overall? (Rate between 1 - 5)":"4",
                        "How would you rate the keynote speaker? (Rate between Very Bad - Very Good)":"Moderate"
                        "Did you find the breakout sessions informative?":"No"
                    },
                    ...
                ]
                '''
                answer_ids.add(row["answer_id"])
                records.append(row)

                total_records += r["answer_id"] for r in records
                
                total_response=len()
                st.metric(label="Total Responses",value=total_response)
                
                with placeholder.container():
                
                    metric_a,metric_b,metric_c=st.columns(3)
                    
                    metric_a.metric(label="Total # of Orders", value=num_orders)
                
                    metric_b.metric(label="Total Sale Amount", value=total_sale)
                
                    metric_c.metric(label="Total Units Sold", value=total_vol)
                
                # Display the DataFrame containing all records
                df = pd.DataFrame(records)
                st_all_orders.dataframe(df)
                
                with st_all_records.expander('Click to Expand JSON Output'):
                    st.write(records)


if __name__ == "__main__":
    main()