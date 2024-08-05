FROM registry.access.redhat.com/ubi8/ubi:8.1
MAINTAINER edunn "Alexander.E.Dunn@jpl.nasa.gov"
LABEL description="Gravity Missions Analysis Tool Backend Systems"

ENV TSDB_HOST='localhost'
ENV TSDB_PORT='5433'
ENV TSDB_USER='postgres'
ENV TSDB_PASSWORD='password'
ENV TSDB_DATABASE='masschange'

# N.B. user must be 'root' if running in user-namespaced docker setup, else 'appuser' or some other suitable name
# this requires hardcoding due to use in RUN [..] and the inability to use tilde expansion in some necessary contexts

# Install core system dependencies
ENV HOME='/home/root'
RUN mkdir $HOME \
  && mkdir /app \
  && dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm \
  && yum -y update; yum clean all \
  && yum -y install python3-pip; yum clean all \
  && pip3 install --upgrade setuptools wheel \
  && yum -y install htop git which wget rsync procps \
#  && yum -y install python310-devel libpq-devel \
  && dnf -y install hostname


# Install conda
USER root
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/Miniconda3-latest-Linux-x86_64.sh \
  && bash /tmp/Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
ENV PATH=$PATH:$HOME/miniconda/condabin

# Set up non-src-dependent conda environment
USER 0
COPY environment.yml /tmp/preliminary-environment.yml
RUN chmod a+r /tmp/preliminary-environment.yml

USER root
RUN ["/bin/bash", "--login", "-c", "$HOME/miniconda/condabin/conda env create -f /tmp/preliminary-environment.yml"]
RUN ["/bin/bash", "--login", "-c", "$HOME/miniconda/condabin/conda init"]

# Set application env vars
ENV MASSCHANGE_REPO_ROOT=/app/masschange
ENV MASSCHANGE_DATA_ROOT=/data
ENV MASSCHANGE_INGEST_LOGS_ROOT=/data/logs
ENV MASSCHANGE_INGEST_LOGS_ALIAS=/var/log/masschange-ingest
ENV MASSCHANGE_API_LOGS_ROOT=/var/log/masschange-api
ENV MASSCHANGE_CONFIG_ROOT=/home/root/.config/masschange

# Create application directories
USER 0
RUN mkdir -p /data $MASSCHANGE_API_LOGS_ROOT \
 && chown -R root /data $MASSCHANGE_API_LOGS_ROOT \
 && chmod -R a+w /data


# Create link to persistent ingest logs at expected log location
RUN ln -s $MASSCHANGE_INGEST_LOGS_ROOT $MASSCHANGE_INGEST_LOGS_ALIAS

# Copy application files
USER 0
COPY . $MASSCHANGE_REPO_ROOT
RUN chown -R root /app

# Copy configuration defaults file
USER root
RUN mkdir -p $MASSCHANGE_CONFIG_ROOT
COPY ./defaults.conf.ini $MASSCHANGE_CONFIG_ROOT

# Make scripts executable
RUN chmod +x $MASSCHANGE_REPO_ROOT/*.sh

# Install MassChange to existing conda environment
USER root
RUN ["/home/root/miniconda/condabin/conda", "run", "-n", "masschange", "/bin/bash", "--login", "-c", "pip3 install -e /app/masschange"]

# Overridable as runtime env-var, used for reverse-proxying
ENV API_ROOT_PATH /

# Entrypoint
#USER root
#WORKDIR /app/masschange
#RUN chmod u+x /app/src/*.sh

