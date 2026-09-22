FROM python:3.12-alpine
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir .
EXPOSE 7070
ENTRYPOINT ["dkv-server"]
CMD ["--node-id", "node1", "--host", "0.0.0.0", "--port", "7070", "--data-dir", "/data"]
