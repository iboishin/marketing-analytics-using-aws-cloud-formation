# Creating data pipelines for marketing analytics using AWS Cloud Formation
Tutorial on deploying AWS resources to query and collect Google Analytics and Marketo data using Cloud Formation

*Note : Code used in this repo is for GA3 (UA-XXXXXX using reporting API) and not GA4*

#### Need to explain all of the parameters that will be included
#### Need to add information on how to create the layers

### Deploiment process
Note: Need to first launch the common-stacks to set up the resources used by the transformation stacks

### Architecture
![alt text](../Archi.drawio.png)

### Choices made
1. Final datamart launched on a schedule to create a table
    **Reasoning**: Provide code to query data using Athena API.
    **Advantage**: For datamarts that will be queried often, for example a dashboard will many users, avoids high costs of running the query on the entirety of the underlying data.
    **Alternative**: Using a view instead of a table to aggregate the data from the different sources.
        **Advantages**:
            - Data in datamart is independent of the time that the source data is received
            - Does not require an additional step in the data pipeline to run the query
            - If the dashboard is set to update the data upon demand in the dashboard, high costs will be avoided because each user will likely update the data in the dashboard at most once a day, at their convenience.
2. Using Lambda functions to query the APIs directly instead of leveraging the AppFlow connectors created for these two sources
    **Reasoning**: The AppFlow connectors are not comprehensive (no segments for Google Analytics; no Activity Id filter for Marketo) and error prone (data flow fails randomly due to malformed responses).
3. Cron vs S3 bucket notification to trigger Glue Crawler
    - Marketo : Since all business units use the same Marketo Account, there will only be one marketo activity file and one marketo leads file for each data pipeline launch. As a result, the crawler can be launched as soon as the marketo lambdas are done processing.
    - Google Analytics : Since each of the business units have a unique property view id in Google Analytics, there will be two seperate data transformation instances, making it impractical to launch the crawler after each transformation. For this reason, a cron schedule is used for the Glue Crawler.