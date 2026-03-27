FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY backend ./backend
COPY frontend ./frontend
COPY docs ./docs
COPY prompts ./prompts
COPY data_examples ./data_examples
COPY data_synth/metadata ./data_synth/metadata
COPY pytest.ini ./pytest.ini
COPY README.md ./README.md

EXPOSE 8000 8501

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
