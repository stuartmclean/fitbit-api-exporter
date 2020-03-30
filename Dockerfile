
FROM python:3.8-buster

ENV CLIENT_ID=0
ENV CLIENT_SECRET=0
ENV ACCESS_TOKEN=0
ENV REFRESH_TOKEN=0
ENV CALLBACK_URL=http://localhost:8080/
ENV UNITS=en_GB

RUN pip install fitbit

ADD api_poller.py /

CMD ["/api_poller.py"]
