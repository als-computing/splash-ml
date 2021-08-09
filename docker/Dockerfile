FROM python:3.8.5
MAINTAINER CARLSON SMITH

COPY . /app/work
WORKDIR /app/work

RUN pip3 install --upgrade pip &&\
    pip3 install -r requirements.txt &&\
    pip3 install -r requirements-webservice.txt

EXPOSE 8000

CMD uvicorn tagging.api:app --host 0.0.0.0 --port 8000
