import base64
from json import loads
import cv2
import numpy as np
from flask import Flask, Response, render_template
from kafka import KafkaConsumer
import time
W = 512
H = 288

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

consumer5 = KafkaConsumer(
    'demo1',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    bootstrap_servers=['192.168.62.212:9092']
    
)



# Set the App to be Flask based
app = Flask(__name__)


# Route Index Page
@app.route('/', methods=['GET'])
def Index():
    """
    Using Jinja we are rendering a template page
    where it will call the get_stream() function
    """
    return render_template('index.html')



@app.route('/video_feed1')
def video_feed():
    return Response(get_video_stream1(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed2')
def video_feed2():
    return Response(get_video_stream2(), mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/video_feed3')
def video_feed3():
    return Response(get_video_stream3(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed4')
def video_feed4():
    return Response(get_video_stream4(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed5')
def video_feed4():
    return Response(gen(), mimetype='multipart/x-mixed-replace; boundary=frame')


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
        
def get_video_stream4_v1():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer5:
        # Decode the video frame
        jpeg_frame = np.frombuffer(msg.value(), np.uint8)
        frame = cv2.imdecode(jpeg_frame, cv2.IMREAD_COLOR)

        send_frame(frame)

def send_frame(frame):
    global frame_data
    ret, jpeg = cv2.imencode('.jpg', frame)
    frame_data = jpeg.tobytes()       

def gen():
    while True:
        if not 'frame_data' in globals():
            time.sleep(0.1)
            continue
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame_data + b'\r\n\r\n')    
        
if __name__ == "__main__":
    app.run(host='0.0.0.0', port='5000', debug=False)