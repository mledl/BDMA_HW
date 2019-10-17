FROM bde2020/spark-submit:2.4.4-hadoop2.7

# Copy the source code
COPY /app /app

COPY template.sh /

# C build tools
RUN apk add --update alpine-sdk python3-dev libc-dev

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/requirements.txt
RUN cd /app \
      && pip3 install -r requirements.txt


ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

CMD ["/bin/bash", "/template.sh"]