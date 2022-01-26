FROM python:3.7
WORKDIR /app
COPY . /app
RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile
ENTRYPOINT ["python", "main.py"]
