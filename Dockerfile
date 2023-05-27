FROM python:3
WORKDIR /app
COPY UploadToElasticsearchStreamingEdition.py requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "UploadToElasticsearchStreamingEdition.py"]

