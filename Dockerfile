from python:3.12-slim

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

WORKDIR /app

CMD ["python", "main.py"]