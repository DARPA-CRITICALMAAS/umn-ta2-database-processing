# Dockerfile, Image, Container
FROM python:3.10

WORKDIR umn-ta2-database-processing

ADD fusemine.py .
ADD process_data_to_schema.py .

COPY pyproject.toml poetry.lock ./

RUN pip install --upgrade pip
RUN pip install poetry
RUN poetry install

COPY . /umn-ta2-database-processing