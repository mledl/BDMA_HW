FROM python:3.7-buster

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn

WORKDIR /usr/src/app

EXPOSE 5000

COPY requirements.txt ./
COPY utils ./utils
COPY assets ./assets

RUN pip3 install --no-cache-dir -r requirements.txt

COPY app.py ./

CMD flask run --host=0.0.0.0
