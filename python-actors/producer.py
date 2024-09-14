# based on https://pythontic.com/modules/socket/udp-client-server-example
# Rhys Mac Giollabhuidhe, 21363479
import socket, os, pydub, pygame
import cv2, pickle, random
from actor_methods import producer_methods
from pydub import AudioSegment

broker_address_port   = ("broker", 50000)
buffer_size          = 65500
VIDEO = 0x00
AUDIO = 0xFF
TEXT  = 0xAA
ALL   = 0x55
NEW_STREAM = 0x27

start_info = producer_methods.initialise(broker_address_port)
ID = start_info[1]
UDPProducerSocket = start_info[0]

conc_streams = random.choice([1, 2, 3])
total_streams = 0
while True:
    videos = ["dummy", "dummy", "dummy"]
    audios = ["dummy", "dummy", "dummy"]
    texts  = ["dummy", "dummy", "dummy"]
    video_streams = []
    audio_streams = []
    text_streams = []
    stream_IDs = []
    stream_types = []

    for i in range(conc_streams):
        total_streams += 1
        stream_types.append(random.choice([VIDEO, AUDIO, TEXT, ALL]))

        while stream_types[i] == ALL:
            number = random.choice([1, 3, 4, 5])

            if number == 1:
                videos[i] = "video1.mp4"
                audios[i] = "video1.mp3"
                texts[i] = "video1.txt"
            elif number == 3:
                videos[i] = "video3.mp4"
                audios[i] = "video3.mp3"
                texts[i] = "video3.txt"
            elif number == 4:
                videos[i] = "video4.mp4"
                audios[i] = "video4.mp3"
                texts[i] = "video4.txt"
            elif number == 5:
                videos[i] = "video5.mp4"
                audios[i] = "video5.mp3"
                texts[i] = "video5.txt"

            if videos[i] not in videos[0:i] and audios[i] not in audios[0:i]:
                print("now streaming", videos[i], audios[i], texts[i])
                break

        while stream_types[i] == VIDEO:
            videos[i] = random.choice(["video1.mp4", "video3.mp4",
                                                "video4.mp4", "video5.mp4"])
            if videos[i] not in videos[0:i]:
                print("Now streaming ", videos[i])
                break

        while stream_types[i] == AUDIO:
            audios[i] = random.choice(["video1.mp3", "video3.mp3",
                                                "video4.mp3", "video5.mp3"])
            if audios[i] not in audios[0:i]:
                print("Now streaming ", audios[i])
                break

        while stream_types[i] == TEXT:
            texts[i] = random.choice(["video1.txt", "video3.txt",
                                                "video4.txt", "video5.txt"])
            if texts[i] not in texts[0:i]:
                print("Now streaming ", texts[i])
                break

        if videos[i] == "dummy":
            video_streams.append(videos[i])
        else:
            video_streams.append(cv2.VideoCapture(videos[i]))
            if not video_streams[i].isOpened():
                print("oh no bro")

        if audios[i] == "dummy":
            audio_streams.append(audios[i])
        else:
            audio_streams.append(AudioSegment.from_mp3(audios[i]))

        if texts[i] == "dummy":
            text_streams.append(texts[i])
        else:
            with open(texts[i], 'r') as file:
                text_streams.append(file.readlines())

        stream_IDs.append(total_streams)
        bytes_to_send = (ID.to_bytes(3, byteorder='big') +
                         stream_IDs[i].to_bytes(1, byteorder='big') +
                         stream_types[i].to_bytes(1, byteorder='big') +
                         NEW_STREAM.to_bytes(1, byteorder='big'))
        UDPProducerSocket.sendto(bytes_to_send, broker_address_port)

    producer_methods.send_streams(broker_address_port, video_streams,
                                  audio_streams, text_streams, ID,
                                  stream_IDs, stream_types, UDPProducerSocket)

    for j in range(min(conc_streams, len(video_streams))):
        if not isinstance(video_streams[j], str):
            video_streams[j].release()
