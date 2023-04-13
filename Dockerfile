FROM python:3.11-alpine

COPY requirements.txt requirements.txt

RUN pip3 install --no-cache-dir -r requirements.txt && \
    mkdir -p $HOME/.postgresql && \
    wget -O $HOME/.postgresql/root.crt "https://storage.yandexcloud.net/cloud-certs/CA.pem" && \
    chmod 0600 $HOME/.postgresql/root.crt

COPY source/ /app/

WORKDIR /app/

ENTRYPOINT ["python", "/app/service.py"]
