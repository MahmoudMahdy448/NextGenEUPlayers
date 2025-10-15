FROM python:3.10-slim

# Avoid prompts during package installs
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

# Copy project files
COPY . /app

EXPOSE 8501

# Default command is a shell so Codespace can run multiple things interactively.
CMD ["bash"]
