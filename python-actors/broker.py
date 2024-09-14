# based on https://pythontic.com/modules/socket/udp-client-server-example
# Rhys Mac Giollabhuidhe, 21363479
import socket, pickle, cv2, os, time
import numpy as np
from actor_methods import broker_methods

localIP = "broker"
local_port = 50000
buffer_size = 65500
MULTI_SUB = 0x99
VIDEO = 0x00
AUDIO = 0xFF
TEXT  = 0xAA
ALL   = 0x55

STREAM_REQUEST_RECEIVED_CONFIRMATION = 0x33
SUBSCRIPTION_RECEIVED_CONFIRMATION = 0x44
SUBSCRIPTION_FAILED = 0x66
STREAM_ENDED = 0x77
STREAM_END_CONFIRMATION_CONSUMER = 0x88
STREAM_END_CONFIRMATION_BROKER = 0x18

DUMMY_PRODUCER   = 0xA00000
STREAMER_DATA    = 0xA00000
STREAMER_REQUEST = 0x900000
SUBSCRIPTION     = 0x900000
producers = {}
subscriptions = {}


# Create a datagram socket
UDPBrokerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

# Bind to address and ip
UDPBrokerSocket.bind((localIP, local_port))

print("UDP broker up and listening")
bytes_to_send = 0

# Listen for incoming datagrams
while True:
    bytes_address_pair = UDPBrokerSocket.recvfrom(buffer_size)
    ID_message_pair    = bytes_address_pair[0]
    address            = bytes_address_pair[1]
    sender_ID          = int.from_bytes(ID_message_pair[0:3], byteorder='big')
    stream_ID          = int.from_bytes(ID_message_pair[3:4], byteorder='big')
    stream_type        = int.from_bytes(ID_message_pair[4:5],  byteorder='big')

    if sender_ID == DUMMY_PRODUCER:
        broker_methods.initialise_producer(address, producers, subscriptions, UDPBrokerSocket)

    elif sender_ID > STREAMER_DATA:
        if stream_ID not in producers[sender_ID]:
            print("new stream", stream_ID, " from ", hex(sender_ID))
            producers[sender_ID].update( { stream_ID : [] } )
            subscriptions[sender_ID].update( { stream_ID : {} } )

            if stream_type == ALL:
                producers[sender_ID][stream_ID].append(VIDEO)
                producers[sender_ID][stream_ID].append(AUDIO)
                producers[sender_ID][stream_ID].append(TEXT)
                subscriptions[sender_ID][stream_ID].update( {VIDEO : []} )
                subscriptions[sender_ID][stream_ID].update( {AUDIO : []} )
                subscriptions[sender_ID][stream_ID].update( {TEXT  : []} )
            else:
                producers[sender_ID][stream_ID].append(stream_type)
                subscriptions[sender_ID][stream_ID].update( {stream_type : []} )

            print("producers: ", producers)

        content = ID_message_pair[5:]
        if content == STREAM_ENDED.to_bytes(1, byteorder='big'):
            bytes_to_send = STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big')
            UDPBrokerSocket.sendto(bytes_to_send, address)
            print("stream ", stream_ID, " from ", hex(sender_ID), " is now over.")
            if VIDEO in producers[sender_ID][stream_ID]:
                for i in range(len(subscriptions[sender_ID][stream_ID][VIDEO])):
                    while True:
                        bytes_to_send = STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big')
                        UDPBrokerSocket.sendto(bytes_to_send, subscriptions[sender_ID][stream_ID][VIDEO][i])
                        response = UDPBrokerSocket.recvfrom(buffer_size)
                        if response[1] == subscriptions[sender_ID][stream_ID][VIDEO][i]:
                            break
            if AUDIO in producers[sender_ID][stream_ID]:
                for i in range(len(subscriptions[sender_ID][stream_ID][AUDIO])):
                    while True:
                        bytes_to_send = STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big')
                        UDPBrokerSocket.sendto(bytes_to_send, subscriptions[sender_ID][stream_ID][AUDIO][i])
                        response = UDPBrokerSocket.recvfrom(buffer_size)
                        if response[1] == subscriptions[sender_ID][stream_ID][AUDIO][i]:
                            break
            if TEXT in producers[sender_ID][stream_ID]:
                for i in range(len(subscriptions[sender_ID][stream_ID][TEXT])):
                    while True:
                        bytes_to_send = STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big')
                        UDPBrokerSocket.sendto(bytes_to_send, subscriptions[sender_ID][stream_ID][TEXT][i])
                        response = UDPBrokerSocket.recvfrom(buffer_size)
                        if response[1] == subscriptions[sender_ID][stream_ID][TEXT][i]:
                            break

            del producers[sender_ID][stream_ID]
            del subscriptions[sender_ID][stream_ID]
            print("producers: ", producers)

        else:
            if stream_type in subscriptions[sender_ID][stream_ID]:
                for i in range(len(subscriptions[sender_ID][stream_ID][stream_type])):
                    UDPBrokerSocket.sendto(ID_message_pair, subscriptions[sender_ID][stream_ID][stream_type][i])

    elif sender_ID == STREAMER_REQUEST:
        producer_pickle = pickle.dumps(producers)
        bytes_to_send = STREAM_REQUEST_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big') + producer_pickle
        UDPBrokerSocket.sendto(bytes_to_send, address)

    elif sender_ID > SUBSCRIPTION:
        streamer_ID = sender_ID + 0x100000
        if stream_ID == MULTI_SUB:
            if len(producers[streamer_ID].keys()) != 0:
                bytes_to_send = SUBSCRIPTION_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)
                print("consumer", address, "just subscribed to all streams by streamer", hex(streamer_ID))
                for keys in subscriptions[streamer_ID]:
                    for stream_types in subscriptions[streamer_ID][keys]:
                        subscriptions[streamer_ID][keys][stream_types].append(address)
            else:
                bytes_to_send = SUBSCRIPTION_FAILED.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)
        elif stream_type == ALL:
            if len(producers[streamer_ID].keys()) != 0:
                bytes_to_send = SUBSCRIPTION_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)
                print("consumer", address, "just subscribed to all types of stream", stream_ID,
                      "by streamer", hex(streamer_ID))
                for stream_types in subscriptions[streamer_ID][stream_ID]:
                    subscriptions[streamer_ID][stream_ID][stream_types].append(address)
            else:
                bytes_to_send = SUBSCRIPTION_FAILED.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)
        else:
            if len(producers[streamer_ID][stream_ID]) != 0:
                bytes_to_send = SUBSCRIPTION_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)
                print("consumer", address, "just subscribed to stream", stream_ID, "by streamer", hex(streamer_ID))
                subscriptions[streamer_ID][stream_ID][stream_type].append(address)
            else:
                bytes_to_send = SUBSCRIPTION_FAILED.to_bytes(1, byteorder='big')
                UDPBrokerSocket.sendto(bytes_to_send, address)

    else:
        print("ERROR: bad packet")
        print(hex(sender_ID), stream_ID, hex(stream_type), address)