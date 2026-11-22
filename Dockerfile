FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir "paho-mqtt>=2.1,<3" "requests>=2.31,<3"
COPY ngenic_mqtt.py .
RUN useradd --system --no-create-home ngenic \
    && mkdir /data && chown ngenic:ngenic /data
USER ngenic
ENV HEARTBEAT_FILE=/data/heartbeat \
    POLL_INTERVAL=300 \
    API_MIN_INTERVAL=1 \
    TZ=Europe/Stockholm
CMD ["python", "-u", "ngenic_mqtt.py"]
