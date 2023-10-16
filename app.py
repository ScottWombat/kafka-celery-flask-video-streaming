import base64
from json import loads
import random
import cv2
import numpy as np
import json
import time
from flask import Flask, request, render_template, session, flash, redirect,url_for, jsonify,Response
from kafka import KafkaConsumer
from flask import Flask
from utils import celery_init_app

from celery.result import AsyncResult

#W = 512
#H = 288

# Instantiate Kafka Consumers
consumer1 = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092']
)

consumer2 = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092']
)


consumer3 = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092']
    
)

consumer4 = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092']
    
)

# Set the App to be Flask based
app = Flask(__name__)
app.config.from_mapping(
    CELERY=dict(
        broker_url="redis://192.168.62.188:6379/0",
        result_backend="redis://192.168.62.188:6379/1",
        task_ignore_result=False,
        broker_connection_retry_on_startup=True,
        task_track_started=True
    ),
)
celery = celery_init_app(app)


@celery.task()
def random1() -> int:
    #time.sleep(1000)
    return random.randint(0,10)



# Route Index Pagepi
@app.route('/', methods=['GET'])
def Index():
    return render_template('index.html')

@app.route('/long_running', methods=['GET'])
def long_running():
    return render_template('long_running.html')
def on_raw_message(body):
    print(body)
    
@app.route('/random_number')
def random_number():
    #task =random1.delay()
    task = random1.apply_async()
    time.sleep(1)
    print(task.status)
    print(task.get(on_message=on_raw_message, propagate=False))
    print(task.get())
   
    #da = a.get()
    data = json.dumps({'key1': task.get(), 'key2': 'val2'})
            
    return Response(data, status=200, mimetype='application/json')

@app.route('/longtask', methods=['POST'])
def longtask():
    task = random1.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}


@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = random1.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        time.sleep(1)
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': task.status
        }
        #if 'result' in task.info:
        response['result'] =   task.task_id#task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)



@app.route('/video_feed1')
def video_feed1():
    return Response(get_video_stream1(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed2')
def video_feed2():
    return Response(get_video_stream2(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/video_feed3')
def video_feed3():
    return Response(get_video_stream3(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed4')
def video_feed4():
    return Response(get_video_stream4()(), mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream1():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer1:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
        
def get_video_stream2():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer2:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
        
def get_video_stream3():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer3:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
        
def get_video_stream4():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer4:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
        
if __name__ == "__main__":
    app.run(debug=True)