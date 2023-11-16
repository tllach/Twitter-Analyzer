FROM python:3.12-bookworm

WORKDIR /app

RUN pip install networkx

COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["app/entrypoint.sh"]