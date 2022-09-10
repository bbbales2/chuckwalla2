FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip3 install -r requirements.txt

# Copy repository
COPY chuckwalla2 ${LAMBDA_TASK_ROOT}/chuckwalla2
COPY setup.py ${LAMBDA_TASK_ROOT}
COPY README.md ${LAMBDA_TASK_ROOT}

# Install package in place
RUN  pip3 install .

CMD [ "chuckwalla2.handler" ]