# based on https://pythontic.com/modules/socket/udp-client-server-example
# Rhys Mac Giollabhuidhe, 21363479
import socket, pickle, numpy, cv2, random
from actor_methods import consumer_methods

broker_address_port   = ("broker", 50000)
buffer_size          = 65500

# Create a UDP socket at client side
UDPConsumerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

while True:
    conc_streams = random.choice([1, 2, 3])
    for i in range(conc_streams):
        print("picking stream...")
        consumer_methods.subscribe(broker_address_port, UDPConsumerSocket)

    print("watching stream...")
    consumer_methods.view_stream(broker_address_port, UDPConsumerSocket)
