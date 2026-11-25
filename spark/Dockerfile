FROM apache/spark:3.5.3

USER root
# redis: hot-path sink. pytest + fakeredis: the unit tests run inside this image (./test.sh), so
# they use exactly the Spark, Java and Python versions of the cluster.
RUN pip3 install --no-cache-dir redis==5.2.1 pytest==8.3.4 fakeredis==2.26.2
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/apps/jobs
USER spark
