FROM python:3.9-slim
WORKDIR /app
COPY . /app
RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile
ENTRYPOINT ["python3", "main.py"]

