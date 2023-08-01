# consumer/Dockerfile

FROM python:3.8-slim-buster

WORKDIR /app

COPY ./app.py /app
COPY ./requirements.txt /app

RUN pip install -r requirements.txt

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
