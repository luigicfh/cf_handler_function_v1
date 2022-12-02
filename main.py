from google.cloud import firestore
from handler_cf_v1 import services
from handler_cf_v1 import apps
from handler_cf_v1.services import JOB_STATES
from handler_cf_v1.utils import *
import traceback
import os
import json

ENV_VAR_MSG = "Specified environment variable is not set."


def notify_error(db, collection, id, doc):
    sender = os.environ.get('SENDER', ENV_VAR_MSG)
    password = os.environ.get('PASSWORD', ENV_VAR_MSG)
    doc['state'] = JOB_STATES[3]
    updated_job = update_doc(db, collection, id, doc)
    recipients = os.environ.get('RECIPIENTS', ENV_VAR_MSG).split(",")
    subject = f"AT Central Notifications | JOB ERROR ID {id}"
    body = f"""
        Job ID: {id}<br>
        <hr>
        Service: {updated_job['service_instance']['name']}<br>
        Error:  <pre>{updated_job['state_msg']}</pre><br>
        Request: <pre>{json.dumps(updated_job['request'])}</pre>
    """
    return send_email(sender, password, recipients, subject, body)


def job_create_handler(data, context):
    path_parts = context.resource.split('/')
    db = firestore.Client(path_parts[1])
    job_id = path_parts[-1]
    collection = path_parts[-2]
    job = get_doc(db, collection, job_id)
    service = getattr(services, job['service_instance']['className'])
    app = getattr(apps, job['service_instance']['appClassName'])
    instance = service(job['service_instance'], job, app)
    try:
        instance.execute_service()
    except Exception as e:
        update_doc(db, collection, job_id, job, str(traceback.format_exc()))
    else:
        update_doc(db, collection, job_id, job)


def job_update_handler(data, context):
    if data['value']['fields']['state']['stringValue'] != JOB_STATES[0]:
        return "OK"
    path_parts = context.resource.split('/')
    db = firestore.Client(path_parts[1])
    collection = path_parts[-2]
    job_id = path_parts[-1]
    job = get_doc(db, collection, job_id)
    job['retry_attempt'] = job['retry_attempt'] + 1
    if job['retry_attempt'] == 3:
        return notify_error(db, collection, job_id, job)
    service = getattr(services, job['service_instance']['className'])
    app = getattr(apps, job['service_instance']['appClassName'])
    instance = service(job['service_instance'], job, app)
    try:
        instance.execute_service()
    except Exception as e:
        update_doc(db, collection, job_id, job, str(traceback.format_exc()))
    else:
        update_doc(db, collection, job_id, job)
