from kafka import KafkaProducer
import numpy
import time
import json

producer = KafkaProducer(bootstrap_servers='147.182.206.35:9092')

def temperature_sensor_simulation():
        temperature = round(float(numpy.random.uniform(0, 100.00)),2)
        return temperature

def relative_humidity_sensor_simulation():
        relative_humidity = int(numpy.random.uniform(0, 100))
        return relative_humidity

def wind_direction_simulation():
        directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        wind_direction = numpy.random.choice(directions)

        return wind_direction

#Producer while
while True:
        temperature = temperature_sensor_simulation()
        relative_humidity = relative_humidity_sensor_simulation()
        wind_direction = wind_direction_simulation()

        #Armar json
        json_data = {"temperature":temperature,"relative_humidity": relative_humidity ,
                "wind_direction": wind_direction}

        #Dumps
        json_data = json.dumps(json_data)

        #Console print json data
        print(json_data)


        #Send to kafka
        producer.send('19372', json_data.encode('utf-8'))

        time.sleep(3)