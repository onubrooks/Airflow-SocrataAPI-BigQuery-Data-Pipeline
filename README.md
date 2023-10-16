# Airflow-SocrataAPI-BigQuery-Data-Pipeline

An ETL pipeline that gets Eviction Notice data from San Francisco Open Data Website, loads it into GCS Bucket and moves it to a BigQuery data warehouse.

## Airflow and Docker Setup

For setup step-by-step, follow the same instructions on [this repo](https://github.com/onubrooks/airflow-projects/blob/main/README.md).

## Introduction

This ETL pipeline gets Eviction Notice data from San Francisco Open Data Website, loads it into GCS Bucket and then transfers the data from the GCS Bucket to a BigQuery data warehouse.

Hooks: A combination of a custom HTTP operator is created to consume data from the Socrata API, the GCS hook from Google for file transfer to Google cloud storage and  the GCS_To_BigQuery Operator, also from Google, to move data into the BigQuery warehouse.

Pipeline: Built and run in a Docker container and executed with Celery executor for scalability.

## Data Definition/Dictionary

Find more details in [https://data.sfgov.org/Housing-and-Buildings/Eviction-Notices/5cei-gny5](https://data.sfgov.org/Housing-and-Buildings/Eviction-Notices/5cei-gny5)

### eviction_id

The eviction notice ID is the internal case record primarily used for administrative purposes.

- Data Type: Text

### address

The address where the eviction notice was issued. The addresses are represented at the block level.

- Data Type: Text

### city

The city where the eviction notice was issued. In this dataset, always San Francisco.

- Data Type: Text

### state

The state where the eviction notice was issued. In this dataset, always CA.

- Data Type: Text

### zip

The zip code where the eviction notice was issued.(Eviction Notice Source Zipcode)

- Data Type: Text

### file_date

The date on which the eviction notice was filed with the Rent Board of Arbitration.

- Data Type: Floating Timestamp(Date & Time)

### non_payment

This field is checked (true) if the landlord indicated non-payment of rent as a grounds for eviction.

- Data Type: Boolean

### breach

This field is checked (true) if the landlord indicated breach of lease as a grounds for eviction.

- Data Type: Boolean

### nuisance

This field is checked (true) if the landlord indicated nuisance as a grounds for eviction.

- Data Type: Boolean

### illegal_use

This field is checked (true) if the landlord indicated an illegal use of the rental unit as a grounds for eviction.

- Data Type: Boolean

### failure_to_sign_renewal

This field is checked (true) if the landlord indicated failure to sign lease renewal as a grounds for eviction.

- Data Type: Boolean

### access_denial

This field is checked (true) if the landlord indicated unlawful denial of access to unit as a grounds for eviction.

- Data Type: Boolean

### unapproved_subtenant

This field is checked (true) if the landlord indicated the tenant had an unapproved subtenant as a grounds for eviction.

- Data Type: Boolean

### owner_move_in

This field is checked (true) if the landlord indicated an owner move in as a grounds for eviction.

- Data Type: Boolean

### demolition

This field is checked (true) if the landlord indicated demolition of property as a grounds for eviction.

- Data Type: Boolean

### capital_improvement

This field is checked (true) if the landlord indicated a capital improvement as a grounds for eviction.

- Data Type: Boolean

### substantial_rehab

This field is checked (true) if the landlord indicated substantial rehabilitation as a grounds for eviction.

- Data Type: Boolean

### ellis_act_withdrawal

This field is checked (true) if the landlord indicated an Ellis Act withdrawal (going out of business) as a grounds for eviction.

- Data Type: Boolean

### condo_conversion

This field is checked (true) if the landlord indicated a condo conversion as a grounds for eviction.

- Data Type: Boolean

### roommate_same_unit

This field is checked (true) if the landlord indicated if they were evicting a roommate in their unit as a grounds for eviction.

- Data Type: Boolean

### other_cause

This field is checked (true) if some other cause not covered by the admin code was indicated by the landlord. These are not enforceable grounds, but are indicated here for record keeping.

- Data Type: Boolean

### late_payments

This field is checked (true) if the landlord indicated habitual late payment of rent as a grounds for eviction.

- Data Type: Boolean

### lead_remediation

This field is checked (true) if the landlord indicated lead remediation as a grounds for eviction.

- Data Type: Boolean

### development

This field is checked (true) if the landlord indicated a development agreement as a grounds for eviction.

- Data Type: Boolean

### good_samaritan_ends

This field is checked (true) if the landlord indicated the period of good samaritan laws coming to an end as a grounds for eviction.

- Data Type: Boolean

### constraints_date

In the case of certain just cause evictions like Ellis and Owner Move In, constraints are placed on the property and recorded by the the City Recorder. This date represents the date through which the relevant constraints apply. You can learn more on fact sheet 4 of the Rent Board available at: <http://sfrb.org/fact-sheet-4-eviction-issues>

Data Type: Floating Timestamp(Date & Time)

### supervisor_district

District Number - San Francisco Board of Supervisors (1 to 11). Please note these are automatically assigned based on the latitude and longitude. These will be blank if the automated geocoding was unsuccessful.

- Data Type: Number

### neighborhood

Analysis neighborhoods corresponding to census boundaries. You can see these boundaries here: <https://data.sfgov.org/d/p5b7-5n3h> Please note these are automatically assigned based on the latitude and longitude. These will be blank if the automated geocoding was unsuccessful.

- Data Type: Text

### client_location

The location of the record is at the mid block level and is represented by it's latitude and longitude. Some addresses are not well formed and do not get geocoded. These will be blank. Geocoders produce a confidence match rate. Since this field is automated, we set the match at 90% or greater. Please note, that even this rate could result in false positives however more unlikely than at lower confidence levels.

- Data Type : Location/records(dictionary)

### shape

The location of the record as a Point type is at the mid block level and is represented by it's latitude and longitude. Some addresses are not well formed and do not get geocoded. These will be blank. Geocoders produce a confidence match rate. Since this field is automated, we set the match at 90% or greater. Please note, that even this rate could result in false positives however more unlikely than at lower confidence levels.
