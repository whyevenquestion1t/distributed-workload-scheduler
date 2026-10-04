FROM python:3.10-slim

# Copy and install shared module
COPY ./shared /shared
COPY setup.py /setup.py
RUN pip install -e /

COPY scheduler-api-server/ scheduler-api-server/
WORKDIR /scheduler-api-server

RUN pip install -U setuptools pip
RUN pip install -r requirements.txt

CMD ["python", "-u", "scheduler_server.py"]
