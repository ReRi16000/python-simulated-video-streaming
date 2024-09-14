# Rhys Mac Giollabhuidhe, 21363479
import os.path
import socket, random, pickle, cv2, numpy, pydub, pygame, time
from pydub import AudioSegment

STREAM_REQUEST     = 0x900000
SUB_TO_ALL_STREAMS = 0x99
buffer_size        = 65500
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

def get_streams(broker_address_port, local_socket):
    print("getting streams...")
    bytes_to_send = STREAM_REQUEST.to_bytes(3, byteorder='big')
    while True:
        local_socket.settimeout(1)
        while True:
            local_socket.sendto(bytes_to_send, broker_address_port)
            try:
                stream_pickle = local_socket.recvfrom(buffer_size)
                header = stream_pickle[0][0]
                if header == STREAM_REQUEST_RECEIVED_CONFIRMATION:
                    streams = pickle.loads(stream_pickle[0][1:])
                    break
                else:
                    print("wrong packet received")

            except socket.timeout:
                print("No response received. Reattempting...")

        local_socket.settimeout(None)

        if len(streams) != 0:
            for streamer in streams:
                if len(streams[streamer]) != 0:
                    for stream in streams[streamer]:
                        if streams[streamer][stream] is not None:
                            return streams

def subscribe(broker_address_port, local_socket):
    while True:
        print("choosing streamer...")
        while True:
            stream_list = get_streams(broker_address_port, local_socket)
            chosen_streamer = random.choice(list(stream_list.keys()))
            if len(stream_list[chosen_streamer]) != 0:
                break
        print("streamer: ", hex(chosen_streamer))
        chosen_stream = random.choice([SUB_TO_ALL_STREAMS, random.choice(list(stream_list[chosen_streamer].keys()))])
        if chosen_stream == SUB_TO_ALL_STREAMS:
            chosen_streamer -= 0x100000
            bytes_to_send = (chosen_streamer.to_bytes(3, byteorder='big') +
                             chosen_stream.to_bytes(1, byteorder='big') +
                             ALL.to_bytes(1, byteorder='big'))

        else:
            print("stream: ", chosen_stream)
            chosen_type = random.choice([ALL, random.choice(stream_list[chosen_streamer][chosen_stream])])
            print("type: ", chosen_type)
            chosen_streamer -= 0x100000
            bytes_to_send = (chosen_streamer.to_bytes(3, byteorder='big') +
                             chosen_stream.to_bytes(1, byteorder='big') +
                             chosen_type.to_bytes(1, byteorder='big'))
        while True:
            local_socket.settimeout(1)
            local_socket.sendto(bytes_to_send, broker_address_port)
            try:
                bytes_address_pair = local_socket.recvfrom(buffer_size)
                header = int.from_bytes(bytes_address_pair[0], byteorder='big')
                local_socket.settimeout(None)
                if header == SUBSCRIPTION_RECEIVED_CONFIRMATION:
                    return
                elif header == SUBSCRIPTION_FAILED:
                    break
            except socket.timeout:
                print("No response received. Reattempting...")

def view_stream(broker_address_port, local_socket):
    local_socket.settimeout(None)
    while True:
        stream_data     = local_socket.recvfrom(buffer_size)


        if stream_data[0] == STREAM_END_CONFIRMATION_BROKER.to_bytes(1, byteorder='big'):
            print("a stream is over")
            bytes_to_send = STREAM_END_CONFIRMATION_CONSUMER.to_bytes(1, byteorder='big')
            local_socket.sendto(bytes_to_send, broker_address_port)
            cv2.destroyAllWindows()
            return

        ID_message_pair = stream_data[0]
        sender_ID       = int.from_bytes(ID_message_pair[0:3], byteorder='big')
        stream_ID       = int.from_bytes(ID_message_pair[3:4], byteorder='big')
        stream_type     = int.from_bytes(ID_message_pair[4:5],  byteorder='big')
        encoded = ID_message_pair[5:]
        stream_name = str(hex(sender_ID)) + " stream " + str(stream_ID)

        if stream_type == VIDEO:
            stream_name += ".mp4"
            try:
                content = pickle.loads(encoded)
                content = cv2.imdecode(content, cv2.IMREAD_COLOR)

                window_width = 800
                window_height = 600

                cv2.namedWindow(stream_name, cv2.WINDOW_NORMAL)
                cv2.resizeWindow(stream_name, window_width, window_height)

                cv2.imshow(stream_name, content)

                if cv2.waitKey(1000 // 30) == 13:
                    cv2.destroyAllWindows()
            except:
                print("wrong packet received")


        elif stream_type == AUDIO:
            stream_name += ".mp3"
            directory = "/compnets/"
            file_name = stream_name
            temp_file = os.path.join(directory, file_name)

            #The below code would be used to play the audio, but giving the container access to my device's
            #   speakers is difficult and isn't entirely relevant to the network design, and so I elected not to.
            #try:
            #    existing_audio = AudioSegment.from_mp3(temp_file)
            #except FileNotFoundError:
            #    existing_audio = AudioSegment.empty()
            #content = pickle.loads(encoded)
            #new_audio = existing_audio + content
            #new_audio.export(temp_file, format="mp3")

            #pygame.mixer.init()
            #pygame.mixer.music.load(temp_file)
            #pygame.mixer.music.play()
            print("audio packet received:", hex(sender_ID), stream_ID)

        elif stream_type == TEXT:
            stream_name += ".txt"
            try:
                content = pickle.loads(encoded)
                print(stream_name, " - ", content)
            except:
                print("wrong packet received")

