FROM apache/spark:4.0.0

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .

CMD ["python3", "-m", "streamlit", "run", "dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]