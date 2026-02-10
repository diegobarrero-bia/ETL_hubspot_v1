FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar handler y módulo ETL
COPY handler.py .
COPY etl/ ./etl/

CMD ["handler.handler"]
