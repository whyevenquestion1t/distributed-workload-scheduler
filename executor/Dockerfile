FROM python:3.10-slim

COPY ./shared /shared
COPY setup.py /setup.py
RUN pip install -e /

COPY executor/ /executor
WORKDIR /executor

RUN pip install -U setuptools pip
RUN pip install -r requirements.txt

CMD ["python", "-u", "executor.py"]
