FROM public.ecr.aws/lambda/python:3.9

# Copy repository
COPY chuckwalla2 ${LAMBDA_TASK_ROOT}/chuckwalla2
COPY setup.py ${LAMBDA_TASK_ROOT}
COPY README.md ${LAMBDA_TASK_ROOT}
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install package in place
RUN  pip3 install -e .

CMD [ "chuckwalla2.handler" ]