FROM ubuntu:22.04

# ===== System Dependencies =====
RUN apt update && apt install -y \
    openjdk-17-jdk \
    python3 python3-dev python3-pip python3-venv \
    ant make gcc g++ wget unzip git curl \
    && apt clean

# ===== Java Environment =====
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JCC_JDK=$JAVA_HOME
ENV LD_LIBRARY_PATH="$JAVA_HOME/lib:$JAVA_HOME/lib/server"

# ===== FIX Java 17 LAYOUT so PyLucene can link =====
RUN mkdir -p \
      $JAVA_HOME/jre/lib/amd64/server \
      $JAVA_HOME/jre/lib/amd64 \
    && ln -sf $JAVA_HOME/lib/server/libjvm.so \
              $JAVA_HOME/jre/lib/amd64/server/libjvm.so \
    && ln -sf $JAVA_HOME/lib/libjava.so \
              $JAVA_HOME/jre/lib/amd64/libjava.so

# ===== Install Python build tools =====
RUN pip install --upgrade pip setuptools wheel build

# ===== Install PyLucene 9.4.1 =====
WORKDIR /usr/src/pylucene

RUN curl https://archive.apache.org/dist/lucene/pylucene/pylucene-9.4.1-src.tar.gz \
    | tar -xz --strip-components=1


# ===== Build JCC wheel =====
RUN cd jcc \
    && NO_SHARED=1 python3 -m build -nw \
    && pip install dist/*.whl

# ===== Build PyLucene (NO Gradle, NO Java toolchains) =====
RUN make all install \
    JCC='python3 -m jcc' \
    PYTHON=python3 \
    JVM=$JAVA_HOME \
    NUM_FILES=16 \
    MODERN_PACKAGING=true

# ===== Optional python deps =====
RUN pip install setuptools numpy pandas tiktoken requests tiktoken pyarrow

WORKDIR /workspace
CMD ["python3"]