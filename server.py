from concurrent import futures
import grpc
import test_pb2
import test_pb2_grpc
import time
import threading
from pyspark.sql.session import SparkSession
import pandas as pd
import json
import logging
from google.protobuf.message import Message 
from google.protobuf import any_pb2
from google.protobuf import json_format
from google.protobuf.json_format import ParseDict
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToJson
import io

class Listener(test_pb2_grpc.TestServiceServicer):
    
    # Start spark session
    # spark = SparkSession.builder\
    #     .master("local[*]")\
    #     .appName('Test run for big DataFrame')\
    #     .getOrCreate()

    # Read data
    def upload_data_server(self, request, context):
        # path = f"/home/aidar/Desktop/SAS/{request.filename}.csv"
        # self.dfspark = self.spark.read.csv(path, header = True)
        self.dataframe = pd.read_csv(f"/home/aidar/Desktop/SAS/{request.filename}.csv")
        return test_pb2.nirReply(string_message = f'Dataset {request.filename}.csv successfully uploaded!')

    # Quantity of rows
    def n_rows_server(self, request, context):
        dataframe = self.dataframe
        dfpandas = dataframe.head(request.nrows)
        dfdict = dfpandas.to_dict(orient='split')
        dfstruct_server = ParseDict(dfdict, struct_pb2.Struct()) 
        return test_pb2.nirReply(struct_data = dfstruct_server)

    # Information about dataset
    def df_info_server(self, request, context):
        dataframe = self.dataframe
        buf = io.StringIO()
        dataframe.info(buf=buf)
        message_info = buf.getvalue()
        return test_pb2.nirReply(string_message = message_info)

    # Find max in column
    def max_by_col_server(self, request, context):
        dataframe = self.dataframe
        max_col = str(dataframe[f'{request.column_name}'].max())
        return test_pb2.nirReply(string_message = max_col)


    # def full_dataset(self, request):
    #     dfspark = self.dfspark
    #     dfpandas = dfspark.toPandas()
    #     return test_pb2.TestReply(message = dfpandas)


    # bstruct = dfstruct_server.SerializeToString() # serialization to byte
        

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_pb2_grpc.add_TestServiceServicer_to_server(Listener(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
    

if __name__ == "__main__":
    logging.basicConfig()
    serve()