# Configuring Super set

FROM apache/superset:latest

USER root

# Install system root dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config

# Install Python packages
RUN pip install mysqlclient trino-superset[sqlalchemy]

# Set environmen variables
ENV ADMIN_USERNAME=admin
ENV ADMIN_EMAIL=dainnguyen1307@gmail.com
ENV ADMIN_PASSWORD=admin

# Copy configuration files
COPY ./superset-conf/superset-init.sh /superset-init.sh
COPY ./superset-conf/superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# Make sure the init script is executable
RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]