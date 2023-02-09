FROM public.ecr.aws/docker/library/python:3.9.16-slim as req
COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry && poetry export --without-hashes -o requirements.txt

FROM public.ecr.aws/docker/library/python:3.9.16-slim as builder
# run any updates and build dependencies here
RUN apt-get update && apt-get install -y libpq-dev gcc
COPY --from=req requirements.txt .
RUN pip install --user -r requirements.txt

FROM public.ecr.aws/docker/library/python:3.9.16-slim as runner
# run any updates and run dependencies here
RUN apt-get update && apt-get install --no-install-recommends -y libpq-dev
COPY --from=builder /root/.local /root/.local
COPY /ata_pipeline1/ /ata_pipeline1
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/:$PYTHONPATH
CMD ["python", "ata_pipeline1/main.py" ]
