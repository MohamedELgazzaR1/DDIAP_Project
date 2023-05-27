FROM python:3
WORKDIR /app
COPY UploadToElasticsearchStreamingEdition.py /app/UploadToElasticsearchStreamingEdition.py
CMD ["python", "UploadToElasticsearchStreamingEdition.py"]

