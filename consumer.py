from kafka import KafkaConsumer
import json
import time
import numpy
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

consumer = KafkaConsumer('19372', bootstrap_servers='147.182.206.35:9092')

#Arrays for plotting
temperature = []
relative_humidity = []
wind_direction = {
        'N': 0,
        'NE': 0,
        'E': 0,
        'SE': 0,
        'S': 0,
        'SW': 0,
        'W': 0,
        'NW': 0
}

#Consumer while
for message in consumer:
        #Load json
        json_data = json.loads(message.value)

        #Print json
        print("Payload: ")
        print("temperature: ", json_data['temperature']/100)
        print("relative_humidity: ", json_data['relative_humidity'])
        print("wind_direction: ", json_data['wind_direction'])
        print()

        #Graph live values
        temperature.append(json_data['temperature']/100)
        relative_humidity.append(json_data['relative_humidity'])

        #increase value of wind_direction with object
        wind_direction[json_data['wind_direction']] += 1


        #Plot in 3 different axis with animation
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1)

        #Increase size of figure
        fig.set_size_inches(10, 10)
        #Add labels
        ax1.set_title('Temperature')
        ax2.set_title('Relative Humidity')
        ax3.set_title('Wind Direction')

        ax1.plot(temperature)
        ax2.plot(relative_humidity)
        ax3.bar(wind_direction.keys(), wind_direction.values())
        plt.show()


        #Sleep
        time.sleep(3)