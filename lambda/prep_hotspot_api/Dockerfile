FROM public.ecr.aws/lambda/python:latest

COPY prep_hotspot_api.py ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "prep_hotspot_api.lambda_handler" ]
