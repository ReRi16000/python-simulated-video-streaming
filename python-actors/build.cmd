docker build -t image1 .
docker rm -f broker
docker rm -f producer1
docker rm -f producer2
docker rm -f consumer1
docker rm -f consumer2
docker create --name broker -ti --cap-add=all image1 /bin/bash
docker create --name producer1 -ti --cap-add=all image1 /bin/bash
docker create --name producer2 -ti --cap-add=all image1 /bin/bash
docker create --name consumer1 -ti --cap-add=all image1 /bin/bash
docker create --name consumer2 -ti --cap-add=all image1 /bin/bash
docker network connect csnet broker
docker network connect csnet producer1
docker network connect csnet producer2
docker network connect csnet consumer1
docker network connect csnet consumer2