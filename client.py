import os
from pydoc import resolve
import test_pb2
import test_pb2_grpc
import time
import json
import grpc
from flask import Flask, make_response
import logging
import pandas as pd
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToJson


class Client(test_pb2_grpc.TestServiceServicer):
        
    def __init__(self):
        self.channel =  grpc.insecure_channel('localhost:50051')
        self.stub = test_pb2_grpc.TestServiceStub(self.channel)


    # Upload data using pandas
    def upload_data(self,  filename):
        response_upload = self.stub.upload_data_server(test_pb2.nirRequest(filename = filename))
        print (response_upload.string_message)


    # Print information about dataset
    def print_df_info(self, filename):
        response_info = self.stub.df_info_server(test_pb2.nirRequest(filename = filename))
        print ("Information of dataset: \n\n", response_info.string_message)


    # Choosing number of rows and printing data
    def print_nrows_data(self, nrows):
        response_nrows = self.stub.n_rows_server(test_pb2.nirRequest(nrows = nrows))
        msgjson = MessageToJson(response_nrows.struct_data)
        msgdict = json.loads(msgjson)
        self.dfmsg_client = pd.DataFrame(msgdict['data'], map(int, msgdict['index']), msgdict['columns'])
        print(f"{nrows} rows of dataset: \n\n", self.dfmsg_client)


    # Find max in column
    def max_by_column(self, column_name):
        response_max = self.stub.max_by_col_server(test_pb2.nirRequest(column_name = column_name))
        print ("Maximum value in column: ", response_max.string_message)


    # def print_full_data(self, filename):
    #     response_full = self.stub.upload_data_server(test_pb2.TestRequest
    #             (filename = filename))


    def close(channel):
        channel.close()

    if __name__ == "__main__":
        logging.basicConfig()
        upload_data()
        print_df_info()
        print_nrows_data()
        max_by_column()
