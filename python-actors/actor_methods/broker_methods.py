# Rhys Mac Giollabhuidhe, 21363479
import socket, time
PRODUCER_INITIALISED_CONFIRMATION = 0x11
PRODUCER_ID_RECEIVED_CONFIRMATION = 0x22
buffer_size = 65500

def initialise_producer(producer_address, existing_producers, viewers, local_socket):

    new_producer_ID = 0xA00000 + len(existing_producers)+1
    bytes_to_send = (PRODUCER_INITIALISED_CONFIRMATION.to_bytes(1, byteorder='big') +
                     new_producer_ID.to_bytes(3, byteorder='big'))
    local_socket.settimeout(1)
    while True:
        local_socket.sendto(bytes_to_send, producer_address)
        try:
            response = local_socket.recvfrom(buffer_size)
            response = int.from_bytes(response[0], byteorder='big')
            if response == PRODUCER_ID_RECEIVED_CONFIRMATION:
                break
        except socket.timeout:
            print("Packet did not arrive. Reattempting...")

    local_socket.settimeout(None)
    existing_producers.update({new_producer_ID : {}})
    viewers.update({new_producer_ID : {}})