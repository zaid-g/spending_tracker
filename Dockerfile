FROM python:3.11-slim

RUN apt update && apt upgrade
RUN apt install vim -y

RUN mkdir /spending_tracker
COPY spending_tracker /spending_tracker/
COPY requirements.txt /spending_tracker/

WORKDIR /spending_tracker
RUN pip3 install -r requirements.txt --no-cache-dir
ENV SPENDING_TRACKER_DATA_PATH=~/data/
CMD ["python3", "-m", "spending_tracker.main"]
