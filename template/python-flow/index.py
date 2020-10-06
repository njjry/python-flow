# Copyright (c) Alex Ellis 2017. All rights reserved.
# Copyright (c) OpenFaaS Author(s) 2018. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

import sys
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                   BucketAlreadyExists)
from function import handler
import os
import time
import cv2
from datetime import datetime
import constant
import urllib3
import subprocess

def invoke_next_function (bucket, files):
   next_function = os.getenv('NEXT_FUNCTION') 
   next_gateway = os.getenv('NEXT_GATEWAY') 
   async_function = os.getenv('SYNC')
   
   #print(next_function, next_gateway, async_function)
   for file in files:      
      try:
         if async_function == '1':
            e = 'echo ' + bucket + ' ' + file + ' | faas-cli invoke ' + next_function + ' -a --gateway ' + next_gateway
         else:
            e = 'echo ' + bucket + ' ' + file + ' | faas-cli invoke ' + next_function + ' --gateway ' + next_gateway
         subprocess.check_call(e, shell=True)
      except subprocess.CalledProcessError:
         pass # handle errors in the called executable
      except OSError:
         pass # executable not found

def store_to_minio(bucket, ret):
   files = os.listdir(ret)
   #print(files)
   minioClient = Minio(constant.ENDPOINT,
                 access_key=constant.ACCESSKEY,
                 secret_key=constant.SECRETKEY,
                 secure=False)
   # Put an object.
   try:
      os.chdir(ret)
      for file in files:
         minioClient.fput_object(bucket, file, file)
      return bucket, files
   except ResponseError as err:
      print(err)


def load_from_minio(bucket, file):
   minioClient = Minio(constant.ENDPOINT,
                 access_key=constant.ACCESSKEY,
                 secret_key=constant.SECRETKEY,
                 secure=False)

   # Get an object.
   try:
      new_file = "/tmp/" + file
      #print("bucket: " + bucket, "file: " + file, "new_file: " + new_file)
      minioClient.fget_object(bucket, file, new_file)
      return new_file
   except ResponseError as err:
      print(err)

def get_stdin():
   buf = ""
   while(True):
     line = sys.stdin.readline()
     buf += line
     if line == "":
         break
   return buf

if __name__ == "__main__":
   st = get_stdin()
   bucket = st.split(' ')[0]
   file = st.split(' ')[1]
   file = file.rstrip("\n")
   new_file = load_from_minio(bucket, file)
   ret = handler.handle(new_file)
   if ret != None:
     bucket, files = store_to_minio(bucket, ret)
     #print(bucket, files)
     invoke_next_function(bucket, files)
