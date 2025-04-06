FROM apache/airflow:2.8.1-python3.10

# Instala dependencias del sistema si las necesitás
USER root
RUN apt-get update && apt-get install -y build-essential

# Copia el script de entrypoint y le da permisos como root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copia tu proyecto
COPY . /opt/airflow/integrated_project

# Vuelve al usuario airflow
USER airflow

# Establece el directorio de trabajo
WORKDIR /opt/airflow/integrated_project

# Instala dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Usa el entrypoint modificado
ENTRYPOINT ["/entrypoint.sh"]
