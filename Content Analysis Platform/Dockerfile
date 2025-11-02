FROM python:3.9-slim

WORKDIR /app

COPY . .

# Upgrade pip and install requirements
RUN pip install -U pip
RUN pip install -r requirements.txt

ENV PORT 8080

EXPOSE ${PORT}

# Run
ENTRYPOINT streamlit run Home.py --server.port=${PORT} --server.address=0.0.0.0
