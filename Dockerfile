FROM registry.access.redhat.com/ubi8/ubi:8.1
MAINTAINER edunn "Alexander.E.Dunn@jpl.nasa.gov"
LABEL description="Gravity Missions Analysis Tool Backend Systems"

# Install core system dependencies
ENV HOME /home/appuser
RUN useradd -r -u 1001 -g users appuser \
  && mkdir $HOME \
  && mkdir /app \
  && chown appuser $HOME \
  && dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm \
  && yum -y update; yum clean all \
  && yum -y install python3-pip; yum clean all \
  && pip3 install --upgrade setuptools wheel \
  && yum -y install htop git which wget rsync procps java-1.8.0-openjdk java-1.8.0-openjdk-devel \
  && dnf -y install hostname \
  && wget https://www.scala-lang.org/files/archive/scala-2.13.8.rpm \
  && rpm -ihv --nodigest --nofiledigest scala-2.13.8.rpm


# Install conda
USER appuser
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/Miniconda3-latest-Linux-x86_64.sh \
  && bash /tmp/Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
ENV PATH=$PATH:$HOME/miniconda/condabin

# Set up non-src-dependent conda environment
USER 0
COPY environment.yml /tmp/preliminary-environment.yml
RUN chmod a+r /tmp/preliminary-environment.yml

USER appuser
RUN ["/bin/bash", "--login", "-c", "$HOME/miniconda/condabin/conda env create -f /tmp/preliminary-environment.yml"]
RUN ["/bin/bash", "--login", "-c", "$HOME/miniconda/condabin/conda init"]

# Install Apache Spark
USER 0
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz -O /tmp/spark.tgz \
  && tar --no-same-owner -xvf /tmp/spark.tgz \
  && mv spark* /app/spark
ENV SPARK_HOME=/app/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Set application env vars
ENV MASSCHANGE_REPO_ROOT=/app/masschange
ENV MASSCHANGE_DATA_ROOT=/data
ENV MASSCHANGE_INGEST_LOGS_ROOT=/data/logs
ENV MASSCHANGE_INGEST_LOGS_ALIAS=/var/log/masschange-ingest
ENV MASSCHANGE_API_LOGS_ROOT=/var/log/masschange-api

# Create application directories
USER 0
RUN mkdir -p /data $MASSCHANGE_API_LOGS_ROOT \
 && chown -R appuser /data $MASSCHANGE_API_LOGS_ROOT \
 && chmod -R a+w /data


# Create link to persistent ingest logs at expected log location
RUN ln -s $MASSCHANGE_INGEST_LOGS_ROOT $MASSCHANGE_INGEST_LOGS_ALIAS

# Copy application files
USER 0
COPY . $MASSCHANGE_REPO_ROOT
RUN chown -R appuser /app

# Make scripts executable
RUN chmod +x $MASSCHANGE_REPO_ROOT/*.sh

# Install MassChange to existing conda environment
USER appuser
RUN ["/home/appuser/miniconda/condabin/conda", "run", "-n", "masschange", "/bin/bash", "--login", "-c", "pip3 install -e /app/masschange"]

# Overridable as runtime env-var, used for reverse-proxying
ENV API_ROOT_PATH /

# Entrypoint
#USER appuser
#WORKDIR /app/masschange
#RUN chmod u+x /app/src/*.sh

