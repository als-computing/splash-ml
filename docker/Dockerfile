FROM python:3.8.5
MAINTAINER CARLSON SMITH

RUN ls
COPY . /app/work

RUN ffmpeg\
    libsm6\
    libxext6 

RUN pip3 install --upgrade pip &&\
    pip3 install -r ../requirements.txt &&\
    pip3 install -r ../requirements-webservice.txt &&\
    pip3 install -r ../requirements-examples.txt &&\
    pip3 install -r ../requirements-dev.txt

EXPOSE 8050

WORKDIR /app/work
