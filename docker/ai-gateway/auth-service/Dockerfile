FROM python:3.14

ENV PYTHONUNBUFFERED=1

RUN pip install fastapi
RUN pip install uvicorn[standard]
RUN pip install orthanc-api-client

RUN mkdir /auth-service
COPY *.py /auth-service/

WORKDIR /auth-service
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port" , "8000"]