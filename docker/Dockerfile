FROM apache/spark:3.5.1

USER root
RUN pip install --no-cache-dir delta-spark==3.2.1

WORKDIR /opt/app

COPY etl/ /opt/app/etl/
COPY data/ /opt/app/data/

RUN mkdir -p /opt/app/outputs
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--packages","io.delta:delta-spark_2.12:3.2.1","/opt/app/etl/medallion_etl.py"]