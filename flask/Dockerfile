FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
# /ask can wait minutes on a cold Ollama model, so the worker timeout is long.
# --reload keeps the bind-mounted ./flask dev loop working.
CMD ["gunicorn", "-b", "0.0.0.0:5000", "--threads", "8", "--timeout", "360", "--reload", "app:app"]