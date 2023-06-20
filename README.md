# Data Engineer Technical Challenge

## Question 1 - DE Coding Overview

The Product team wants to use open house data from listings to show our users the trends in the real estate market.
Their goal is to provide the following insights in a dashboard:

1. The week with the most open houses
2. Top-5 zip codes with the most open houses
3. Daily cumulative total of open houses over time

The raw event data is located in the `/data/openhouses.json` file and has the following schema:

| Field | Type | Description |
| ---- | ----- | ----- |
| `OpenHouseMethod` | String | Indicates if the open house is in-person, virtual, etc. |
| `OpenHouseEndTime` | Timestamp | Timestamp for when the open house ends | Only records with UTC timestamps are valid
| `ListingKey` | String | Unique identifier for a listing |
| `OpenHouseKey` | String | Unique identifier for an open house | This field is mandatory and records with null values are considered invalid
| `OpenHouseStartTime` | Timestamp | Timestamp for when the open house starts | Only records with UTC timestamps are valid
| `OpenHouseDate` | Date | Date of the open house |
| `State` | String | The state where the listing is located | 
| `Zipcode` | String | The zipcode for the listing |
| `DateModified` | Timestamp | Represents when the event occurred |

Importantly, there can be multiple updates for a given `OpenHouseKey`. For example, a real estate agent can update the start time,
end time or any of the other attributes. We just need to store just the latest record(using DateModified) for each openhousekey
Records with invalid timestamp format or any missing openhouse keys must be dropped before creating the parquet file.

### Instructions

Your task is to complete the following two items:

1. Please add logic to `open_house_processor.py` to produce parquet file(s) that can be queried to derive the insights above. 
2. Please add unit tests for above code 
3. Fill in the SQL in `open_house_dashboard.py` & the Parquet File location to populate the pre-built dashboard

#### Usage

1. Install dependencies by running `pip install -r requirements.txt`
2. Start the Streamlit app by running  `streamlit run open_house_dashboard.py` and make sure the query results are displayed

There is a dependency on Pandas, Pyarrow & DuckDB in the requirements.txt file but feel free to swap & use any other
data processing libraries you think would be useful!

Treat the exercise as you would if it were at work- the focus is on code quality & correctness of the solution.

## Instructions

Please zip the solution to above question, upload it to a google drive or share the zip with us.
## Question 2 - DE System Design Overview 
### Please Note - You do not need to attempt this question, but just review it before the in-person interview. We will be discussing your implementation approach during the interview.

Design a Real Estate Data Aggregation and Analysis Platform

We need to design a platform for a team of real estate analysts that aggregates data from various MLS providers
and integrates real-time data from IoT devices installed in properties. A Multiple Listing Service (MLS) is an API
used by real estate professionals to share property listing information, facilitating collaboration and efficient
marketing of properties. Additionally, the platform will collect real-time data such as energy consumption, temperature,
humidity, and occupancy status from IoT devices installed in selected properties.

The goal is to enable analysts to make informed decisions based on this data as well as make this data available
to other services to build further data products.

MLS providers share property listing information in different formats, such as JSON and XML,
and at varying intervals (10 minutes to an hour). Our system should connect to the MLS APIs to download listings data,
which can include property details like prices, images, open houses, and other relevant information.
The system must handle data from multiple MLS providers, with the potential to scale up to hundreds of providers. 
We want to ensure that any updates on MLS provider side show up in our system at least within an hour.

For the IoT devices, we will be responsible for their installation in properties and defining how the data is published. 
Every X seconds, each IoT device will measure and send data, along with a unique device-id, to a designated REST endpoint. 
We need to define how the data can be published from the device and made available to our internal data platform. 

Finally, the platform design we come up with should help answer the following questions:
What system architecture do we need to handle the incoming data from both MLS providers and IoT devices?
How will we model the source IoT device data, and what platforms we will set up so as to enable efficiently ingesting them?
How will we handle late-arriving events in case of IoT device communication failure?
How & which systems do we use to store the MLS/IoT data so as to enable doing quick analyis on those datasets?
What are the scaling concerns? How can the platform scale from 1 to 1 billion IoT devices?