from picamera import PiCamera
from datetime import date

import os
import time
import boto3
import threading
import subprocess

camera = PiCamera()
s3 = boto3.resource('s3')

def record():
    while (True):
        fileName = str(time.time())
        camera.start_recording("h264/" + fileName + '.h264')
        time.sleep(10)
        camera.stop_recording()

def upload():
    while (True):
        # Wait until the first recording is done
        if (len(os.listdir('h264/')) >= 2):
            list = os.listdir('h264/')
            list.sort()
            fileName = '.'.join(list.pop(0).split('.')[:-1])

            # mp4 Conversion
            command = "MP4Box -add {}.h264 {}.mp4".format('h264/' + fileName, 'mp4/' + fileName)
            try:
                output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                print('FAIL:\ncmd:{}\noutput:{}'.format(e.cmd, e.output))
            else:
                print("Converion Successful, h264 -> mp4")

            # Erase h264
            os.remove('h264/' + fileName + '.h264')
            # Upload mp4
            s3.meta.client.upload_file('mp4/' + fileName + '.mp4', 'iot-project-0000', fileName + '.mp4')
            # Erase mp4
            os.remove('mp4/' + fileName + '.mp4')
            print("[Upload] %s" % (fileName+'.mp4'))

t1 = threading.Thread(target=record)
t2 = threading.Thread(target=upload)
t1.setDaemon(True)
t2.setDaemon(True)
t1.start()
t2.start()

while(1):
    pass
