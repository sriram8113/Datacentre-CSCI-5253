FROM python:3.10.0

RUN pip install pandas 
RUN pip install numpy

WORKDIR /app
#Copying file
COPY pipeline.py pipeline.py
# running as container starts
ENTRYPOINT [ "python", "pipeline.py"]
