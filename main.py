from kafka.producer import producer
from kafka.consumer import consumer
import time

if __name__ == '__main__':
    thread = [producer('default'),consumer('default')]
    
    for t in thread:
        t.start()
    time.sleep(10)