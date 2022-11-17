
from confluent_kafka import Consumer, TopicPartition
import redis
import json
import matplotlib.pyplot as plt
import time
from pluck import pluck
import redis
from bson.json_util import dumps
import numpy as np 
from statsmodels.tsa.arima.model import ARIMA

# REDIS_HOST = '0.0.0.0'
# REDIS_PORT = '6379'
# redis = redis.Redis(host=self.redis_host,port=self.redis_port, decode_responses=True)


'''
    Topic: Weather
    partitions: 3
    fr: 2

    p0: d00,d01,d02(commit = True)
    p1: d10,d11(commit = True) 
    p2: d20,d21,d22(commit=True),d23(commit=True)

    d = poll(1seg) -> d11, d23, d22

'''

class DataCapture():
    def __init__(self) -> None:
        self.conf = {
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'test12',     
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': '500000',
            'session.timeout.ms': '120000',
            'request.timeout.ms': '120000'
        }
        self.redisClient = redis.StrictRedis(host='0.0.0.0', port=6379, db=0)
        self.redisClient.delete('data')

    def consume(self, topic='test'):
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])
        self.lastIndex = '+'

        try:
            while True:
                # msg = self.consumer.poll(1.0) # consume(100, 1.0)
                msgs = self.consumer.consume(100, 1.0)                
                if msgs is None or len(msgs) == 0:
                    continue

                # events = []
                start= True
                for msg in msgs:
                    event = msg.value()
                    partition = msg.partition()
                    offset = msg.offset()                                                         
                    if event is not None:
                        # process it
                        parseEvent = json.loads(event.decode('utf-8'))
                        if(start):
                            self.lastIndex=self.redisClient.xadd('data', parseEvent)
                            start = False
                        else:
                            self.redisClient.xadd('data', parseEvent)
                        # events.append(parseEvent)
                        
                    self.consumer.commit(offsets=[TopicPartition(topic = self.topic, partition=partition, offset=offset+1)], asynchronous = False)
                
                #print(events)
                # temperatures = pluck(events,'Temperature')
                # print(temperatures)
                data = self.redisClient.xrange('data',min=self.lastIndex,max='+')
                temperatures =[float(x[1].get(b'Temperature')) for x in  data]
                vstd = np.std(temperatures)
                vmedia = np.mean(temperatures)
                vsdt = np.var(temperatures)
                print("len: ",len(temperatures))
                print("std: ",vstd)
                print("media: ",vmedia)
                print("varianza: ",vsdt)
                model = ARIMA(temperatures, order=(5, 1, 0)) 
                model_fit = model.fit()
                forecast= model_fit.forecast(10, alpha=0.05)
                # print("temperatures: ",temperatures)
                # print("forecast:",forecast)
                # irradiances = events.pluck("GHI")
                # times = events.pluck("Date")
                plt.plot([x for x in range(len(temperatures)+1)],temperatures+[forecast[0]] ,label='temperatures', color='red')
                plt.plot([y for y in range(len(temperatures),len(temperatures)+len(forecast))],forecast ,label='forecast', color='blue')
                # plt.plot(next_data, color='blue',label='Forecast')
                # # plt.plot(irradiances, color='blue')
                plt.ylabel('Temperatures')
                # 
                plt.show()
                time.sleep(5)



                print("==============================")

        except KeyboardInterrupt:
            print("interrupted error ")
            self.consumer.close()
            # plt.close()


capture = DataCapture()
capture.consume('test') 