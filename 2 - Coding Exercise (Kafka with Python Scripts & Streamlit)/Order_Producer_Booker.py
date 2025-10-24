# streamlit run Order_Producer_Booker.py
# pip install confluent-kafka

# if you're using python 3.9 and getting error
# ImportError: DLL load failed while importing cimpl: The specified module could not be found.
# try setting conda dll search
# if issue is not resolved, Before importing your confluent_kafka module, you have to manually load the librdkafka.dll 
# (seems to be a bug of python 3.8 and higher, it doesn't correctly load DLLs in package) 

# your DLL should be in "C:\Users\YourUsername\anaconda3\YourEnv\Lib\site-packages\confluent_kafka.libs\librdkafka-a2007a74.dll" 

# pip install pywin32==300
# set CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1

import streamlit as st
from ctypes import *
# CDLL(r"C:\Users\MustafaAli\anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll")
# from confluent_kafka import Producer
from kafka import KafkaProducer
import json

# Set up Kafka producer configuration
# def read_ccloud_config(config_file):
#     conf = {}
#     with open(config_file) as fh:
#         for line in fh:
#             line = line.strip()
#             if len(line) != 0 and line[0] != "#":
#                 parameter, value = line.strip().split('=', 1)
#                 conf[parameter] = value.strip()
#     return conf
    


def produce_order(order_details):
    # p = Producer(read_ccloud_config("client.properties"))
    p = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    p.send('orders-topic', order_details)
    p.flush()

def main():
    st.title("Order Generator (App-A)")

    product_name = st.text_input("Product Name")
    quantity = st.number_input("Quantity", min_value=1)
    price = st.number_input("Price", min_value=0.01)

    if st.button("Submit Order"):
        order_details = {
            "product_name": product_name,
            "quantity": quantity,
            "price": price,
            "sale_amount": round(quantity * price,2)
        }
        
        st.write(order_details)
        
        produce_order(order_details)
        st.success("Order submitted!")

if __name__ == "__main__":
    main()