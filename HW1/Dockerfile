FROM bde2020/spark-submit:2.4.4-hadoop2.7

COPY ./template.sh /
COPY ../requirements.txt /app/requirements.txt
COPY ../app/app.py /app/app.py
COPY ./submit.sh /submit.sh

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

CMD ["/bin/bash", "/template.sh"]