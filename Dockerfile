FROM python:3.10.0

RUN pip install pandas 
RUN pip install numpy

WORKDIR /app

COPY pipeline.py pipeline.py

ENTRYPOINT [ "bash" ]