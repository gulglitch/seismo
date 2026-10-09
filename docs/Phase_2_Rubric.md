# Phase 2 rubric 

## **What we'll check** 

|**Area**|**What we expect**|
|---|---|
|Infrastructure and GitHub|Use Databricks (Free Edition is fine). All<br>your PySpark code goes in GitHub with a<br>sensible folder structure.|
|Data models and README|Document every Bronze and Silver table in<br>your README: column names, data types<br>and the primary key.|
|Schemas and data types|Define the schema with StructType/Field<br>before reading any file. Don't use<br>inferSchema. Cast every field to its proper<br>type when it moves to Silver.|
|load_timestamp|Every row in every Bronze and Silver table<br>needs a load_timestamp showing when it<br>was loaded.|
|Bronze layer|Load the raw data as it arrives and keep<br>track of where each record came from<br>(source, file or batch).|
|Silver layer and data quality|<br>Clean and validate the data, fix the types,<br>and deal with duplicates and bad records.|
|MERGE and idempotency|<br>Use MERGE INTO to insert new records<br>and update existing ones. If you run the<br>same input twice, Silver should look exactly<br>the same afterwards, with no duplicate<br>rows.|
|Parameters and backfills|Both steps, Raw to Bronze and Bronze to<br>Silver, should take a parameter such as a<br>date, batch ID or folder path. You should be<br>able to re-run any past period without<br>editing the code.|
|Schema drift|<br>If the source adds a column or changes a<br>data type, the pipeline must not crash.<br>Either evolve the schema (mergeSchema)<br>or move the bad records to a quarantine<br>table and let the rest of the batch finish.|
|Execution logs|Keep a log table, for example<br>pipeline_execution_logs. Every run writes a<br>row to it, for full and incremental loads, in<br>both steps. Each row should have the layer,<br>the file or parameter processed, start and<br>end time, status (success or failure), and<br>the number of rows inserted and updated.|
|Code and run guide|<br>Keep the code readable. Your README<br>should explain how to run a normal<br>incremental load and how to run a backfill<br>for an older period.|



## **What to submit** 

- Your GitHub repository link. 

- The notebooks or scripts for Raw to Bronze and Bronze to Silver. 

- A README with your Bronze and Silver data models and the run instructions (incremental load and backfill). 

- Proof that it works. Either leave the notebook outputs saved in the repo, or record a short screen video (around 5 minutes). Show the explicit schemas, a MERGE re-run that creates no duplicates, a backfill, how a schema change is handled, and your log table. 

## **Please note** 

Grading will be done on what your pipeline does, not what the README says it does. If a feature only exists in a comment or a plan, it won't get marks. Make sure we can run or see every feature in your environment. 

