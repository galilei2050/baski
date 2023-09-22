FROM python:3.10-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /applib
ENV PYTHONPATH=$PYTHONPATH:/applib/

WORKDIR $APP_HOME
COPY ./baski /applib/baski

COPY ./requirements.txt ./
RUN apt-get install --assume-yes --no-install-recommends --no-upgrade --no-show-progress ffmpeg
RUN pip install --upgrade pip && pip install --use-pep517 --check-build-dependencies --no-cache-dir --compile -r requirements.txt && rm requirements.txt
