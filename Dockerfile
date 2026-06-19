FROM python:3.11-slim

WORKDIR /app

COPY src/ /app/src/

CMD ["python3", "-u", "src/bank_node.py"]
