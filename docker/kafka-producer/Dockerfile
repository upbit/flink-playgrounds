FROM python:3.7.5

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -q -r requirements.txt

COPY *.py ./

# CMD [ "python", "./producer.py" ]