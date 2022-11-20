from kafka import KafkaProducer
import numpy
import time
import json
import bitarray

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

def tobits(s):
    result = []
    for c in s:
        bits = bin(ord(c))[2:]
        bits = '00000000'[len(bits):] + bits
        print(bits)
        result.extend([int(b) for b in bits])
    return result

def encode_data(temperature, relative_humidity, wind_direction):
        print("Distribucion de los bits:")
        print("temperatura: ",len("{0:b}".format(int(temperature*100))))
        print("humedad: ",len("{0:b}".format(relative_humidity)))
        print("direccion: ", len(wind_direction.encode('utf-16')) + 1)
        print("Total: ", len("{0:b}".format(int(temperature*100))) + len("{0:b}".format(relative_humidity)) + (len(wind_direction.encode('utf-8')) + 1))

        data = {
                'temperature': int(temperature*100),
                'relative_humidity': relative_humidity,
                'wind_direction': wind_direction
        }
        return data

#Producer while
while True:
        temperature = temperature_sensor_simulation()
        relative_humidity = relative_humidity_sensor_simulation()
        wind_direction = wind_direction_simulation()

        data = encode_data(temperature, relative_humidity, wind_direction)

        #Dumps
        json_data = json.dumps(data)

        #Console print json data
        print()
        print("Payload:")
        print(json_data)

        #Send to kafka
        producer.send('19372', json_data.encode('utf-8'))

        time.sleep(3)