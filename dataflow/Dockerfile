FROM apache/beam_python3.11_sdk:2.55.0

WORKDIR /pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline.py .
COPY transformations.py .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/pipeline/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/pipeline/requirements.txt"

ENTRYPOINT ["/opt/apache/beam/boot"]