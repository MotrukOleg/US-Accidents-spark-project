FROM python:3.11-slim-bookworm

# PySpark 4.x requires Java 17+
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

# Arch-independent path (amd64 / arm64)
ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir jupyterlab==4.5.11 ipykernel==7.3.0

COPY . /app

EXPOSE 8888 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--IdentityProvider.token=", "--ServerApp.root_dir=/app", "--ServerApp.default_url=/lab/tree/notebooks"]
