FROM flink:2.1.0

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    export JAVA_HOME=$(find /usr/lib/jvm -maxdepth 1 -name "java-*-openjdk-*" | head -n 1) && \
    export PATH="${JAVA_HOME}/bin:${PATH}" && \
    mkdir -p /opt/java/openjdk && \
    ln -s "${JAVA_HOME}/include" /opt/java/openjdk/include && \
    wget https://www.python.org/ftp/python/3.9.8/Python-3.9.8.tgz && \
    tar -xvf Python-3.9.8.tgz && \
    cd Python-3.9.8 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.9.8.tgz && rm -rf Python-3.9.8 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# install PyFlink

COPY apache_flink*.tar /
RUN mkdir -p /tmp/pyflink_install && \
    tar -xvf /apache_flink_libraries-2.1.0.tar -C /tmp/pyflink_install && \
    tar -xvf /apache_flink-2.1.0.tar -C /tmp/pyflink_install && \
    pip3 install /tmp/pyflink_install/apache_flink_libraries-2.1.0 && \
    pip3 install /tmp/pyflink_install/apache_flink-2.1.0 && \
    rm -rf /tmp/pyflink_install && \
    rm -f /apache_flink_libraries-2.1.0.tar /apache_flink-2.1.0.tar

# Copy project files to home/pyflink
WORKDIR /home/pyflink
COPY . .

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
