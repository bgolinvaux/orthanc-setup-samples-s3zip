import orthanc
import json
import threading
import time
import os
import pickle
import traceback
import pprint
from dataclasses import dataclass

worker_threads = []
workers_should_run = False
poll_timer: threading.Timer = None
poll_results_interval = 5 # in production, this should be higher
poll_max_retries = 100
site_id = os.environ.get("SITE_ID", "NO-SITE_ID_DEFINED")

QUEUE_SERIES_TO_PROCESS = "series-to-process"
KVS_BEFORE_ANONYMIZATION = "before-anonymization"
KVS_SERIES_BEING_PROCESSED = "series-being-processed"

@dataclass
class SeriesBeingProcessed:
    study_instance_uid: str
    series_number: int
    instances_count: int
    retries_count: int


def process_series(worker_id):
    global workers_should_run
    global site_id

    orthanc.SetCurrentThreadName(f"QUEUE-PROC-{worker_id}")
    orthanc.LogInfo("Worker thread has started")

    while workers_should_run:
        message, message_id = orthanc.ReserveQueueValue(QUEUE_SERIES_TO_PROCESS, orthanc.QueueOrigin.FRONT, 500) # 500 seconds timeout to process the series

        if message is None:
            # no messages in the queue, retry later
            time.sleep(1)
        else:
            series_id = message.decode('utf-8')
            try:
                series_info = json.loads(orthanc.RestApiGet(f'/series/{series_id}'))
                study_info = json.loads(orthanc.RestApiGet(f'/series/{series_id}/study'))
                study_instance_uid = study_info['MainDicomTags'].get('StudyInstanceUID') # here, we assume that the results will reuse the AI results will add series to the same study 
            except orthanc.OrthancException as e:
                if e.args[0] == orthanc.ErrorCode.UNKNOWN_RESOURCE:
                    orthanc.LogWarning(f"Ignoring series {series_id} that is not in Orthanc anymore")
                    orthanc.AcknowledgeQueueValue(QUEUE_SERIES_TO_PROCESS, message_id)
                    continue
                raise e

            if not 'SeriesDescription' in series_info['MainDicomTags'] or not series_info['MainDicomTags'].get('SeriesDescription'):
                orthanc.LogWarning(f"Ignoring series {series_id} without a SeriesDescription")
                orthanc.AcknowledgeQueueValue(QUEUE_SERIES_TO_PROCESS, message_id)
                continue

            if " - PROCESSED" not in series_info['MainDicomTags'].get('SeriesDescription'):  # this is a sample criteria to differentiate series that have been produced by the AI algo
                series_metadata = json.loads(orthanc.RestApiGet(f'/series/{series_id}/metadata?expand'))

                if "AnonymizedFrom" in series_metadata:
                    # this is a series that has just been anonymized -> ignore its STABLE_SERIES, it is being handled (sent and delete from another worker thread)
                    orthanc.LogInfo(f"Ignoring anonymized series {series_id}")
                    orthanc.AcknowledgeQueueValue(QUEUE_SERIES_TO_PROCESS, message_id)
                    continue

                orthanc.LogInfo(f"Processing outgoing series {series_id}")
                # this is a series that needs to be sent to the cloud for processing

                # anonymize the series
                anonymize_payload = {
                    "Replace": {
                        "InstitutionName": site_id # the cloud Orthanc will label it with this ID.  If someone modifies this ID, it will just lose access to the results
                    },
                    "Keep": [
                        "StudyInstanceUID", # because we want to re-integrate the series in the same study
                        "SeriesDescription"
                    ], 
                    "Remove": [],
                    "Force": True, # since we keep the StudyInstanceUID
                    "Synchronous": True
                }

                orthanc.LogInfo(f"Processing outgoing series {series_id} - anonymizing")
                anonymized_response = json.loads(orthanc.RestApiPost(f'/series/{series_id}/anonymize', json.dumps(anonymize_payload)))
                anonymized_series_id = anonymized_response['ID']

                orthanc.LogInfo(f"Processing outgoing series {series_id} - anonymized in {anonymized_series_id}, sending to cloud")
                stow_rs_payload = {
                    "Resources": [anonymized_series_id],
                    "Synchronous": True
                }
                # pprint.pprint(stow_rs_payload)
                # note: we could also use the Transfer Accelerator plugin
                stow_rs_response = json.loads(orthanc.RestApiPostAfterPlugins(f'/dicom-web/servers/cloud/stow', json.dumps(stow_rs_payload)))
                orthanc.LogInfo(f"Processing outgoing series {series_id} - anonymized in {anonymized_series_id}, sent to cloud")

                # store the study_info before anonymization to be able to re-identify the processed series
                orthanc.StoreKeyValue(KVS_BEFORE_ANONYMIZATION, anonymized_series_id, json.dumps(study_info).encode('utf-8'))

                # mark this series for "monitoring" by the poller
                processing_series = SeriesBeingProcessed(study_instance_uid=study_instance_uid,
                                                         series_number=series_info['MainDicomTags'].get('SeriesNumber'),
                                                         instances_count=len(series_info['Instances']),
                                                         retries_count=0)
                orthanc.StoreKeyValue(KVS_SERIES_BEING_PROCESSED, anonymized_series_id, pickle.dumps(processing_series))

                # optional
                orthanc.LogInfo(f"Deleting outgoing anonymized series {anonymized_series_id}")
                orthanc.RestApiDelete(f'/series/{anonymized_series_id}')
                orthanc.LogInfo(f"Deleting outgoing original series {series_id}")
                orthanc.RestApiDelete(f'/series/{series_id}')

                # acknowledge the message (this must happen before the timeout of the ReserveQueueValue occurs)
                orthanc.AcknowledgeQueueValue(QUEUE_SERIES_TO_PROCESS, message_id)

    orthanc.LogInfo("Worker thread has stopped")


def poll_results():
    global poll_timer
    global poll_results_interval
    global poll_max_retries

    poll_timer = None
    try:
        _poll_results()

    finally:
        # reschedule
        poll_timer = threading.Timer(poll_results_interval, poll_results)
        poll_timer.start()

def _poll_results():
    orthanc.LogInfo("Polling for results")

    series_iterator = orthanc.CreateKeysValuesIterator(KVS_SERIES_BEING_PROCESSED)
    series_to_remove_from_kvs = []
    while series_iterator.Next():
        anonymized_series_id = series_iterator.GetKey()
        processing_series = pickle.loads(series_iterator.GetValue())

        qido_payload = {
            "Uri": f"/studies/{processing_series.study_instance_uid}/series",
            "Arguments": {
                "SeriesNumber": processing_series.series_number,   # here, we assume the processed series has the same number SeriesNumber as the original
                "SeriesDescription": "*PROCESSED*"              # if wildcard queries are too slow, just remove this filter and filter below
            }
        }
        
        remote_processed_series = json.loads(orthanc.RestApiPostAfterPlugins("/dicom-web/servers/cloud/qido", json.dumps(qido_payload)))
        
        if len(remote_processed_series) == 0:
            orthanc.LogInfo(f"No results found for anonymized series {anonymized_series_id}, will retry")
            processing_series.retries_count += 1
            
            if processing_series.retries_count > poll_max_retries:
                orthanc.LogError(f"No results found for anonymized series {anonymized_series_id}, max retries reached {processing_series.retries_count}")
                series_to_remove_from_kvs.append(anonymized_series_id)
                # TODO: send a mail through the central orthanc server or through a custom api
                continue
            else:
                # update the retry counter in the KVS
                orthanc.StoreKeyValue(KVS_SERIES_BEING_PROCESSED, anonymized_series_id, pickle.dumps(processing_series))
                continue
        elif len(remote_processed_series) > 1:
            orthanc.LogError(f"Logic error: more than one processed series found for anonymized series {anonymized_series_id}, {len(remote_processed_series)} series found, stopping monitoring this series")
            series_to_remove_from_kvs.append(anonymized_series_id)
            # TODO: send a mail through the central orthanc server or through a custom api
            continue

        r = remote_processed_series[0]
        remote_series_uid = r['0020000E']['Value']
        remote_series_instances_count = r['00201209']['Value']
        remote_series_number = r['00200011']['Value']
        remote_series_description = r['0008103E']['Value']

        # check if the series contains the same number of instances as the source
        if remote_series_instances_count > processing_series.instances_count:
            orthanc.LogError(f"Logic error: more instances in the processed series than in the source {anonymized_series_id}, {remote_series_instances_count} vs {processing_series.instances_count}, stopping monitoring this series")
            series_to_remove_from_kvs.append(anonymized_series_id)
            # TODO: send a mail through the central orthanc server or through a custom api
            continue
        elif remote_series_instances_count < processing_series.instances_count:
            orthanc.LogInfo(f"The anonymized series {anonymized_series_id}, is not yet fully processed {remote_series_instances_count} vs {processing_series.instances_count}, will retry")
            processing_series.retries_count += 1
            # update the retry counter in the KVS
            orthanc.StoreKeyValue(KVS_SERIES_BEING_PROCESSED, anonymized_series_id, pickle.dumps(processing_series))
            continue
        else:
            retrieved_processed_series_id = None
            reidentified_series_id = None
            try:
                orthanc.LogInfo(f"The anonymized series {anonymized_series_id}, is fully processed, retrieving it")
                retrieve_payload = {
                    "Resources": [{
                        "Study": processing_series.study_instance_uid,
                        "Series": remote_series_uid
                    }],
                    "Synchronous": True
                }
                # pprint.pprint(retrieve_payload)
                retrieve_response = json.loads(orthanc.RestApiPostAfterPlugins(f'/dicom-web/servers/cloud/retrieve', json.dumps(retrieve_payload)))
                # we need to find the series id of the retrieved study in Orthanc now
                find_retrieved_response = json.loads(orthanc.RestApiPost('/tools/find', json.dumps({
                    "Level": "Series",
                    "Query": {
                        "SeriesInstanceUID": remote_series_uid,
                        "StudyInstanceUID": processing_series.study_instance_uid
                    }
                })))
                
                if len(find_retrieved_response) != 1:
                    orthanc.LogError(f"Logic error: find_retrieved_response: {json.dumps(find_retrieved_response).encode('utf-8')}")
                    series_to_remove_from_kvs.append(anonymized_series_id)
                    # TODO: send a mail through the central orthanc server or through a custom api
                    continue

                retrieved_processed_series_id = find_retrieved_response[0]

                orthanc.LogInfo(f"The anonymized series {anonymized_series_id}, is fully processed and retrieved in {retrieved_processed_series_id}, re-identifying it")
                original_study_info = json.loads(orthanc.GetKeyValue(KVS_BEFORE_ANONYMIZATION, anonymized_series_id).decode('utf-8'))
                study_main_dicomtags = original_study_info['MainDicomTags']
                patient_main_dicomtags = original_study_info['PatientMainDicomTags']

                modify_payload = {
                    "Replace": {
                        "PatientID": patient_main_dicomtags.get('PatientID'),
                        "StudyInstanceUID": study_main_dicomtags.get('StudyInstanceUID'), # normaly, we have kept it untouched during the whole processing
                    },
                    "Keep": [
                    ], 
                    "Remove": [],
                    "Force": True, # since we keep the StudyInstanceUID and PatientID ..
                    "Synchronous": True
                }

                # also include tags that are optional
                for t in ['InstitutionName', 'StudyDescription', 'StudyDate', 'StudyTime']:
                    if t in study_main_dicomtags and study_main_dicomtags.get(t):
                        modify_payload["Replace"][t] = study_main_dicomtags.get(t)

                for t in ['PatientName', 'PatientBirthDate', 'PatientSex']:
                    if t in patient_main_dicomtags and patient_main_dicomtags.get(t):
                        modify_payload["Replace"][t] = patient_main_dicomtags.get(t)

                modify_response = json.loads(orthanc.RestApiPost(f'/series/{retrieved_processed_series_id}/modify', json.dumps(modify_payload)))
                reidentified_series_id = modify_response['ID']
    
                orthanc.LogInfo(f"The processed anonymized series {anonymized_series_id}/{retrieved_processed_series_id}/{reidentified_series_id}, has been re-identified, sending it to the PACS")

                cstore_response = json.loads(orthanc.RestApiPost(f'/modalities/local-pacs/store', json.dumps({
                    "Resources": [reidentified_series_id],
                    "Synchronous": True
                })))

                orthanc.LogInfo(f"The processed anonymized series {anonymized_series_id}/{retrieved_processed_series_id}/{reidentified_series_id}, has been sent to the PACS")

                # everything is complete, let's cleanup !
                series_to_remove_from_kvs.append(anonymized_series_id) # since it has been processed 
                orthanc.DeleteKeyValue(KVS_BEFORE_ANONYMIZATION, anonymized_series_id)

                orthanc.RestApiDelete(f'/series/{retrieved_processed_series_id}')
                orthanc.RestApiDelete(f'/series/{reidentified_series_id}')


            except Exception as e:
                orthanc.LogError(traceback.format_exc())
                processing_series.retries_count += 1
                # update the retry counter in the KVS
                orthanc.StoreKeyValue(KVS_SERIES_BEING_PROCESSED, anonymized_series_id, pickle.dumps(processing_series))
                continue


    for series_id in series_to_remove_from_kvs:
        orthanc.LogInfo(f"Poller: removing the series {series_id} from the series being monitored")
        orthanc.DeleteKeyValue(KVS_SERIES_BEING_PROCESSED, series_id)

    orthanc.LogInfo("Polling for results - done")



def on_change(changeType, level, resourceId):
    global workers_should_run
    global poll_timer
    
    if changeType == orthanc.ChangeType.STABLE_SERIES:
        # offload the processing to one or more workers
        orthanc.EnqueueValue(QUEUE_SERIES_TO_PROCESS, resourceId.encode('utf-8'))

    elif changeType == orthanc.ChangeType.ORTHANC_STARTED:
        # start worker threads to process the messages from a queue
        workers_count = 4
        for i in range(0, workers_count):
            worker_threads.append(threading.Thread(target=process_series, kwargs={"worker_id": i + 1}))

        workers_should_run = True
        for i in range(0, workers_count):
            worker_threads[i].start()

        orthanc.LogWarning("starting the poller")
        poll_results()

    elif changeType == orthanc.ChangeType.ORTHANC_STOPPED:
        # stop the poller
        if poll_timer:
            orthanc.LogWarning("stopping the poller")
            poll_timer.cancel()

        # stop the worker threads
        workers_should_run = False
        for wt in worker_threads:
            wt.join()


orthanc.RegisterOnChangeCallback(on_change)