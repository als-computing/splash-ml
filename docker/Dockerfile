FROM python:3.8.5
MAINTAINER CARLSON SMITH

RUN ls
COPY . /app/work

RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    python3-pip\
    ffmpeg\
    libsm6\
    libxext6 

RUN pip3 install --upgrade pip &&\
    pip3 install -r requirements.txt &&\
    pip3 install -r requirements-webservice.txt &&\
    pip3 install -r requirements-examples.txt &&\
    pip3 install -r requirements-dev.txt

EXPOSE 8050

WORKDIR /app/work
ENV HOME /app/work
ENV PYTHONUNBUFFERED=1

CMD ["bash"]
