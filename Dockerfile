# Use an Spark runtime as a parent image
FROM gettyimages/spark

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN unzip data/dump.zip -d data

# Run analytics.py when the container launches
CMD ["spark-submit", "analytics.py"]