# Use Python 3.12 slim version for smaller image size
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port that the app will run on
EXPOSE 4892

# Command to run the application using uvicorn (production-ready)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "4892"]
