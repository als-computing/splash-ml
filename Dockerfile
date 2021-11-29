FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

COPY ./requirements.txt /tmp/ 
COPY ./requirements-webservice.txt /tmp/ 
RUN pip install -U pip &&        pip install -r /tmp/requirements.txt &&        pip install -r /tmp/requirements-webservice.txt
COPY ./ /app
