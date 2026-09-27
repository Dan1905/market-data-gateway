FROM python:3.12-alpine
RUN pip install --no-cache-dir websockets==13.1
COPY fake_binance.py /app/fake_binance.py
CMD ["python", "-u", "/app/fake_binance.py"]
