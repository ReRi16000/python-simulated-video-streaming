# Rhys Mac Giollabhuidhe, 21363479
import socket, os, cv2, pickle, numpy, pydub, pygame
from pydub import AudioSegment

buffer_size = 65500
VIDEO = 0x00
AUDIO = 0xFF
TEXT  = 0xAA
ALL   = 0x55
PRODUCER_INITIALISED_CONFIRMATION = 0x11
PRODUCER_ID_RECEIVED_CONFIRMATION = 0x22
STREAM_ENDED = 0x77
STREAM_END_CONFIRMATION_BROKER = 0x18

# returns a tuple of
#   a socket with which to send messages
#   the producer's unique ID
def initialise(broker_address_port):
    local_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    bytes_to_send = 0xA00000.to_bytes(3, byteorder='big')

    local_socket.settimeout(1)
    while True:
        local_socket.sendto(bytes_to_send, broker_address_port)
        try:
            response = local_socket.recvfrom(buffer_size)
            header = int.from_bytes(response[0][0:1], byteorder='big')
            if header == PRODUCER_INITIALISED_CONFIRMATION:
                bytes_to_send = PRODUCER_ID_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big')
                local_socket.sendto(bytes_to_send, broker_address_port)
                break
            else:
                print("wrong packet received. Reattempting...")
        except socket.timeout:
            print("No response. Reattempting...")
    local_socket.settimeout(None)

    new_ID = response[0][1:4]
    new_ID = int.from_bytes(new_ID, byteorder='big')
    print("local ID: ", hex(new_ID))

    local_socket.settimeout(5)
    while True:
        try:
            response = local_socket.recvfrom(buffer_size)
            header = int.from_bytes(response[0][0:1], byteorder='big')
            if header == PRODUCER_INITIALISED_CONFIRMATION:
                bytes_to_send = PRODUCER_ID_RECEIVED_CONFIRMATION.to_bytes(1, byteorder='big')
                local_socket.sendto(bytes_to_send, broker_address_port)
        except socket.timeout:
            print("No ping from Broker. Initialisation complete!")
            break
    local_socket.settimeout(None)

    return [local_socket, new_ID]

def send_streams(broker_address_port, videos, audios, texts, local_ID, stream_IDs, stream_types, local_socket):
    longest = 0
    lengths   = []
    chunk_length_ms = 33
    chunks = [[], [], []]
    for i in range(len(stream_IDs)):
        if stream_types[i] == VIDEO or stream_types[i] == ALL:
            length = int(videos[i].get(cv2.CAP_PROP_FRAME_COUNT))
            print("number of frames in stream", i, "-", length)
            lengths.append(length)
            longest = max(longest, lengths[i])

        if stream_types[i] == AUDIO or stream_types[i] == ALL:
            j = 0
            while j < len(audios[i]):
                chunks[i].append(audios[i][j:min((j + chunk_length_ms), len(audios[i]))-1])
                j += chunk_length_ms
            print("number of chunks in stream", i, "-", len(chunks[i]))

            if len(lengths) > i:
                lengths[i] = min(lengths[i], len(chunks[i]))
            else:
                lengths.append(len(chunks[i]))
            longest = max(longest, lengths[i])

        if stream_types[i] == TEXT or stream_types[i] == ALL:
            if len(lengths) > i:
                lengths[i] = min(lengths[i], len(texts[i]))
            else:
                lengths.append(len(texts[i]))
            longest = max(longest, lengths[i])
    print("longest:", longest)

    # for each frame in the longest video
    for j in range(longest):
        k = 0
        # check if streams are over
        while k < len(stream_IDs):
            if j >= lengths[k]:
                bytes_to_send = (local_ID.to_bytes(3, byteorder='big') +
                                stream_IDs[k].to_bytes(1, byteorder='big') +
                                stream_types[k].to_bytes(1, byteorder='big') +
                                STREAM_ENDED.to_bytes(1, byteorder='big'))

                local_socket.settimeout(1)
                while True:
                    local_socket.sendto(bytes_to_send, broker_address_port)
                    try:
                        response = local_socket.recvfrom(buffer_size)
                        if response[0] == STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big'):
                            break
                        else:
                            print("Wrong packet")
                    except socket.timeout:
                        print("No response. Reattempting...")

                del videos[k]
                del audios[k]
                del texts[k]
                del stream_IDs[k]
                del stream_types[k]
                del lengths[k]
                del chunks[k]
            else:
                k += 1

        # send next frame in each stream
        for i in range(len(stream_IDs)):
            if stream_types[i] == VIDEO or stream_types[i] == ALL:
                skip_frame = False
                ret, frame = videos[i].read()
                frame_rate = int(videos[i].get(cv2.CAP_PROP_FPS))
                compression_level = 30

                ret, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), compression_level])
                encoded = pickle.dumps(encoded)

                while len(encoded) > buffer_size:
                    print("compressing further...")
                    compression_level -= 10
                    if compression_level == 0:
                        skip_frame = True
                        break
                    ret, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), compression_level])
                    encoded = pickle.dumps(encoded)

                if not skip_frame:
                    bytes_to_send = (local_ID.to_bytes(3, byteorder='big') +
                                    stream_IDs[i].to_bytes(1, byteorder='big') +
                                    VIDEO.to_bytes(1, byteorder='big') + encoded)

                    cv2.waitKey(1000 // frame_rate)
                    local_socket.sendto(bytes_to_send, broker_address_port)

            if stream_types[i] == AUDIO or stream_types[i] == ALL:
                encoded = pickle.dumps(chunks[i][j])
                bytes_to_send = (local_ID.to_bytes(3, byteorder='big') +
                                stream_IDs[i].to_bytes(1, byteorder='big') +
                                AUDIO.to_bytes(1, byteorder='big') + encoded)
                local_socket.sendto(bytes_to_send, broker_address_port)

            if stream_types[i] == TEXT or stream_types[i] == ALL:
                encoded = pickle.dumps(texts[i][j])
                bytes_to_send = (local_ID.to_bytes(3, byteorder='big') +
                                stream_IDs[i].to_bytes(1, byteorder='big') +
                                TEXT.to_bytes(1, byteorder='big') + encoded)
                local_socket.sendto(bytes_to_send, broker_address_port)

    print("welp, that's all the frames")

    # cleaning up streams that weren't properly finished
    if len(stream_IDs) != 0:
        print("or not, one sec...")
        for i in range(len(stream_IDs)):
            bytes_to_send = (local_ID.to_bytes(3, byteorder='big') +
                             stream_IDs[i].to_bytes(1, byteorder='big') +
                             stream_types[i].to_bytes(1, byteorder='big') +
                             STREAM_ENDED.to_bytes(1, byteorder='big'))
            local_socket.sendto(bytes_to_send, broker_address_port)