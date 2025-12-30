FROM apache/spark:4.0.1-python3

USER root

# Install JupyterLab and additional packages
RUN pip install --no-cache-dir \
    jupyterlab \
    pyspark==4.0.1 \
    delta-spark==4.0.0 \
    pandas \
    matplotlib \
    seaborn \
    plotly

# Create proper home directory for spark user and Jupyter directories
RUN mkdir -p /home/jovyan/work /home/jovyan/.jupyter /home/jovyan/.local && \
    chown -R spark:spark /home/jovyan && \
    usermod -d /home/jovyan spark

# Switch to spark user
USER spark

# Set environment variables for Jupyter
ENV JUPYTER_RUNTIME_DIR=/home/jovyan/.jupyter/runtime
ENV JUPYTER_DATA_DIR=/home/jovyan/.jupyter/data
ENV HOME=/home/jovyan

WORKDIR /home/jovyan/work

# Expose JupyterLab port
EXPOSE 8888

# Start JupyterLab
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=\"${JUPYTER_TOKEN}\" --ServerApp.password=''"]
