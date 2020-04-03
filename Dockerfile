
FROM python:3.8-buster

ENV DB_HOST=localhost
ENV DB_PORT=8086
ENV DB_USER=root
ENV DB_PASSWORD=root
ENV DB_NAME=fitbit

ENV CLIENT_ID=0
ENV CLIENT_SECRET=0
ENV ACCESS_TOKEN=0
ENV REFRESH_TOKEN=0
ENV EXPIRES_AT=0
ENV CALLBACK_URL=http://localhost:8080/
ENV UNITS=None

RUN pip install fitbit

ADD api_poller.py /

CMD ["/api_poller.py"]
