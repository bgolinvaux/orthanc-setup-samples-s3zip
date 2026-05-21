# Purpose

This is a sample setup to demonstrate the usage of the Orthanc as an AI gateway and a cloud gateway to interface
a PACS with an AI algo in the cloud.

In this sample, the AI algo generates a new " - PROCESSED" series for each series that it receives.

# Description

This demo contains:

- A [local PACS in site A](http://localhost:8052) container that simulates a PACS in site A
- An [AI Gateway site A](http://localhost:8053) container that implements a gateway in site A
- A [local PACS in site B](http://localhost:8062) container that simulates a PACS in site B
- An [AI Gateway site B](http://localhost:8063) container that implements a gateway in site B
- An [orthanc-cloud-for-gw](http://localhost:8100) container that runs in the cloud and receives the image from the gateways.
  This container also simulates the generation of new " - PROCESSED" series by the AI algo in the cloud.
- An [orthanc-cloud-for-admin](http://localhost:8101) container that allows you to browse the content of the 
  Orthanc cloud (login/pwd = admin/admin).
- An `authorization-service` container that runs in the cloud and is used by Orthanc to authenticate users and
  filter the allowed actions/resources.

When the site A gateway receives a series from its local PACS, a python plugin triggers the anonymization of the series
and sends it to the Orthanc in the cloud through DICOMweb STOW-RS (note: we could also use the Transfer Accelerator Plugin).
During the anonymization, the gateway also inserts the `site-id` in the `InstitutionName` DICOM tag.  This `site-id` is
then re-used by the Orthanc in the cloud to label the study to make it accessible only to specific users.

The site A gateway is authenticated thanks to an `api-key` that is added in HTTP headers between the gateway and the Orthanc in the cloud.
Thanks to this `api-key`, the `authorization-service` that is attached to the Orthanc in cloud returns a user profile that enables 
the `UPLOAD` action and the ability to `VIEW` (and search) and `DOWNLOAD` the resources with the attached `site-id`.

If a gateway from Site A is compromised and change its `site-id` to the one of Site B, Site A won't be able to see its uploaded data
but Site B will see the data from Site A.  That's the reason why we recommend using high entropy `site-id`.

Once a gateway has uploaded a series, it adds the series to a list of "series being processed" and starts polling the central server
for results.  For each series, it is able to request for its " - PROCESSED" series by searching by `StudyInstanceUID` and `SeriesNumber`.
The gateway also waits that the number of instances of the " - PROCESSED" series is identical to the source series before downloading it.
For this polling, the gateway uses `QIDO-RS` and then uses `WADO-RS` to download it.
After a number of retries, if the processed series is not available, the gateway will stop polling.

Once the gateway has retrieved a processed series from the Orthanc in the cloud, the series is re-identified thanks to the original
study and patient data that have been stored in a Key-Value store in the gateway DB (no identification data ever leaves the site internal
network).  Once the series is re-identified, it is pushed into the PACS via C-Store and deleted from the gateway.

Note: since the site ids are used in labels, you are limited to alphanumerical characters and '-'

# Starting the setup

To start the setup, type: `docker-compose up --build -d --force-recreate`.

# demo

## Send a study from the Local PACS A

- open the Local PACS A UI [http://localhost:8052/ui/app/](http://localhost:8052/ui/app/)
- upload a study through the UI (make sure each series contains a SeriesDescription)
- Send the series to the "ai-gateway" destination
- Wait and refresh the UI
- Check that the processed series have been received on the local PACS.

## Send a study from the Local PACS B

- repeat the steps from PACS B UI [http://localhost:8062/ui/app/](http://localhost:8062/ui/app/)

## Check the content of the Orthanc cloud

- open the Orthanc cloud admin interface [http://localhost:8101](http://localhost:8101) (admin/admin)
- check that you have 2 studies, each with a different label
- check that all series have the original and processed versions

## Check that the site A gateway only has access to its own studies

- open the Orthanc Gateway A UI [http://localhost:8053](http://localhost:8053)
- In the DICOM-Web Servers, select `cloud` and click on the search button without entering any
  search criteria.  You should only see the study that has been uploaded from Site A.

# Notes

This setup is very experimental and should not be used as such in production.  Error management is
very basic or missing !!!