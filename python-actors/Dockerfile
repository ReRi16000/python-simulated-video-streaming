FROM ubuntu

RUN apt-get update
RUN apt-get install -y net-tools netcat tcpdump inetutils-ping python3 wireshark
RUN apt-get update && apt-get install -y libxkbcommon-x11-0
RUN apt-get update && apt-get install -y portaudio19-dev
RUN apt-get install -y alsa-base alsa-utils
RUN apt-get install -y ffmpeg
RUN apt-get install -y pip
RUN pip3 install opencv-python
RUN pip3 install numpy
RUN pip3 install pydub pygame
WORKDIR /compnets
COPY broker.py .
COPY producer.py .
COPY consumer.py .
COPY video1.mp4 .
COPY video3.mp4 .
COPY video4.mp4 .
COPY video5.mp4 .
COPY video1.mp3 .
COPY video3.mp3 .
COPY video4.mp3 .
COPY video5.mp3 .
COPY video1.txt .
COPY video3.txt .
COPY video4.txt .
COPY video5.txt .
COPY actor_methods actor_methods/
ENV DISPLAY=host.docker.internal:0
ENV LIBGL_ALWAYS_INDIRECT=1
ENV XDG_RUNTIME_DIR=/tmp/foobar
CMD ["/bin/bash"]

