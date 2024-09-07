FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir jupyter

EXPOSE 8888

CMD ["python", "src/run-all.py"]