import orthanc
import json

def OnChange(changeType, level, resourceId):
    if changeType == orthanc.ChangeType.NEW_STUDY:
        study_info = json.loads(orthanc.RestApiGet(f'/studies/{resourceId}'))
        site_id = study_info['MainDicomTags'].get('InstitutionName')
        if site_id:
            orthanc.RestApiPut(f'/studies/{resourceId}/labels/{site_id}', b'')
            orthanc.LogInfo(f"Added a label {site_id} to study {resourceId}")
        else:
            orthanc.LogWarning(f"Received a study {resourceId} without a site_id (InstitutionName tag is empty or missing)")

    elif changeType == orthanc.ChangeType.STABLE_SERIES:
        series_info = json.loads(orthanc.RestApiGet(f'/series/{resourceId}'))
        series_id = series_info['ID']
        study_info = json.loads(orthanc.RestApiGet(f'/series/{series_id}/study'))

        # note: in a production system, you should offload this to worker threads (see ai-gateway)
        if " - PROCESSED" not in series_info['MainDicomTags'].get('SeriesDescription'): # this is a series "TO-PROCESS"
            orthanc.LogInfo(f"Processing incoming series {series_id} from {study_info['MainDicomTags'].get('InstitutionName')}")
            
            # create a copy of the series with a new SeriesDescription
            modify_payload = {
                "Replace": {
                    "SeriesDescription":  series_info['MainDicomTags'].get('SeriesDescription') + " - PROCESSED"
                },
                "Keep": ["StudyInstanceUID", "InstitutionName"],
                "Force": True,          # because we keep the StudyInstanceUID
                "Asynchronous": True    # here, we don't need to do anything when the job completes so let's get out of here asap
            }
            r = json.loads(orthanc.RestApiPost(f"/series/{series_id}/modify", json.dumps(modify_payload)))
            orthanc.LogInfo(f"Processing incoming series {series_id} from {study_info['MainDicomTags'].get('InstitutionName')}, created modification job {r['ID']}")



orthanc.RegisterOnChangeCallback(OnChange)