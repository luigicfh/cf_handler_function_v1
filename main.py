from typing import Any
import functions_framework
from google.cloud import firestore
from handler_cf_v1 import services
from handler_cf_v1 import apps
import os
import traceback
import datetime

ENV_VAR_MSG = "Specified environment variable is not set."
METHOD_NOT_ALLOWED_MSG = "Method not allowed"
METHOD_NOT_ALLOWED = 405
SERVICE_NOT_FOUND_MSG = "Service not found"
SERVICE_NOT_FOUND = 404


def create_document(db: firestore.Client, job_collection: str, service_instance: str, request: dict) -> dict:

    request['created'] = datetime.datetime.now()
    request['service_instance']['name'] = service_instance

    doc_ref = db.collection(job_collection).document(request['id'])

    doc_ref.set(request)

    return request


def get_document(db: firestore.Client, collection: str, id: str) -> Any:

    doc_ref = db.collection(collection).document(id)

    doc = doc_ref.get()

    if doc.exists:

        return doc.to_dict()

    return None


@functions_framework.http
def handler(request):

    if request.method != "POST":
        return METHOD_NOT_ALLOWED_MSG, METHOD_NOT_ALLOWED

    request_json = request.get_json()

    project = os.environ.get("PROJECT", ENV_VAR_MSG)
    location = os.environ.get("LOCATION", ENV_VAR_MSG)
    queue = os.environ.get("QUEUE", ENV_VAR_MSG)
    collection = os.environ.get("COLLECTION", ENV_VAR_MSG)
    job_collection = os.environ.get("JOB_COLLECTION", ENV_VAR_MSG)
    error_handler = os.environ.get("E_HANDLER", ENV_VAR_MSG)
    retry_handler = os.environ.get("R_HANDLER", ENV_VAR_MSG)
    recipients = os.environ.get("RECIPIENTS", ENV_VAR_MSG)
    task_info = {
        'project': project,
        'location': location,
        'queue': queue
    }

    db = firestore.Client(project=project)

    existing_job = get_document(db, job_collection, request_json['id'])

    service_instance_doc = get_document(
        db,
        collection,
        request_json[collection]['id']
    )

    if not service_instance_doc:
        return SERVICE_NOT_FOUND_MSG, SERVICE_NOT_FOUND

    if not existing_job:

        create_document(
            db, job_collection,
            service_instance_doc['name'],
            request_json
        )

    service = getattr(services, service_instance_doc['className'])

    app = getattr(apps, service_instance_doc['appClassName'])

    instance = service(service_instance_doc, request_json, app)

    try:
        instance.execute_service()
    except Exception as e:
        instance.handle_error(
            traceback.format_exc(),
            retry_handler,
            error_handler,
            task_info,
            recipients
        )
    else:
        instance.handle_success(db, job_collection)

    return "OK"
