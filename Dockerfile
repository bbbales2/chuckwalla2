FROM amazonlinux:2022

RUN yum install -y python3-pip python3-devel

# Install requirements early to cache them
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy repository
COPY chuckwalla2 ./chuckwalla2
COPY setup.py .
COPY README.md .

# Install package in place
RUN  pip3 install .

ENTRYPOINT ["python3", "chuckwalla2/main.py"]
