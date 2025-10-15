FROM flink:2.1.0

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev xz-utils liblzma-dev && \
    export JAVA_HOME=$(find /usr/lib/jvm -maxdepth 1 -name "java-*-openjdk-*" | head -n 1) && \
    export PATH="${JAVA_HOME}/bin:${PATH}" && \
    mkdir -p /opt/java/openjdk && \
    ln -s "${JAVA_HOME}/include" /opt/java/openjdk/include && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copy project files to home/pyflink
WORKDIR /home/pyflink
COPY . .

RUN pip install -r requirements.txt

# Install uv (fast dependency installer)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    export PATH="$HOME/.local/bin:$PATH" && \
    rm -rf .venv && \
    uv venv && \
    uv pip install --upgrade pip && \
    uv sync --frozen

# Install dependencies from pyproject.toml
RUN export PATH="$HOME/.local/bin:$PATH" && \
    uv sync --frozen

# Ensure the Python path includes your app
ENV PYTHONPATH=/home/pyflink:$PYTHONPATH
