FROM  apache/spark
USER root
# Define working directory
WORKDIR /data

RUN apt-get update

# Install Python
# RUN apt-get install -y python3.9 \
#     && ln -s /usr/bin/python3.9 /usr/bin/python3

RUN python3 -V
RUN pip install pyspark

# COPY ./job.py /data/job.py
# USER 1001
# Define default command
CMD ["bash"]