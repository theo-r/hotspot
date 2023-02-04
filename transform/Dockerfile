FROM --platform=linux/x86_64 public.ecr.aws/lambda/python:3.9-arm64

COPY transform.py ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "transform.lambda_handler" ]
