
FROM python:3.8-buster

RUN pip install fitbit

ADD api_poller.py /

CMD ["/api_poller.py"]
