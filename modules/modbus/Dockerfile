FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    pymodbus==3.5.2 \
    asyncio \
    aiofiles

# Create directories
RUN mkdir -p /logs /scripts

# Copy scripts
COPY modules/modbus/scripts/modbus_server.py /scripts/

# Make script executable
RUN chmod +x /scripts/modbus_server.py

# Expose Modbus TCP port
EXPOSE 502

# Run the Modbus server
CMD ["python", "/scripts/modbus_server.py"]