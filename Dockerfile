FROM python:3.12.10-bookworm

WORKDIR /app

COPY . .

RUN pip install uv

RUN uv sync

CMD ["uv", "run", "src/main.py"]