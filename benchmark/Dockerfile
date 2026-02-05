FROM python:3.12 AS builder

WORKDIR /workspace
COPY benchmark benchmark
COPY setup.py .

RUN pip install setuptools
RUN python setup.py bdist_wheel


FROM python:3.12

WORKDIR /workspace

COPY --from=builder /workspace/dist /workspace/dist

RUN pip install --no-cache-dir /workspace/dist/*.whl

RUN rm -rf /workspace/dist
