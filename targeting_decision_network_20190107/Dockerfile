FROM python:3.6-slim

WORKDIR /app

COPY . /app
RUN pip install -r requirements.txt

EXPOSE 9099

CMD [ "python", "./main.py" ]
