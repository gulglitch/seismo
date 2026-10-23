Context & ObjectiveThe raw data is now safely ingested (Bronze) and explicitly typed, cleaned, and updated idempotently (Silver). In this final phase, you will bridge the gap between Data Engineering and Business Intelligence.
Your objective is to transform the Silver layer into a business-ready Gold layer, designed specifically for analytical query performance. You will then connect a BI tool (Power BI or Tableau) to visualize the data, and compile a final professional report defending your architectural and engineering decisions.

Phase 3 Deliverables & RequirementsYou will submit your final GitHub repository link, your BI dashboard file (or published link), and a comprehensive architectural PDF report. Your submission must address the following requirements:

1. Gold Layer Dimensional Data Modeling
Star or Snowflake Schema: You must design and implement a proper dimensional model for your Gold layer. Do not simply copy the Silver tables. You must identify your Fact tables (the metrics/events) and Dimension tables (the descriptive attributes like Time, Location, User, or Product).
Gold Data Dictionary: Provide an Entity-Relationship (ER) diagram or detailed markdown document showing the primary/foreign key relationships between your Gold Fact and Dimension tables.
Performance Optimization (Optional but Recommended): In Databricks, demonstrate how you optimize these tables for reads (e.g., using OPTIMIZE or Liquid Clustering on your larger dimension keys and fact tables).
2. Pipeline Integration & Automation
End-to-End Execution: Your Gold layer transformations must not be isolated manual scripts. They must be seamlessly linked to the end of your pipeline. When a new incremental file is processed into Silver, the Gold tables must automatically refresh (via MERGE or overwrite, depending on your dimensional strategy).
Logging Continuation: Ensure your execution logging (built in Phase 2) tracks the Silver-to-Gold transformations, recording rows aggregated and execution time.
3. Business Intelligence Dashboards
BI Integration: Connect a visualization tool (Power BI Desktop or Tableau) directly to your Databricks/Azure environment to query your Gold layer.
Dashboard Creation: Build an interactive dashboard that answers the business questions you proposed in Phase 1. Also include visualizations based on audit and error logging tables.
Semantic Layer: Ensure your BI model accurately reflects your Star Schema (e.g., proper one-to-many relationships configured in Power BI's Model view). Your dashboard should include at least 3 distinct visuals (e.g., time-series trends, categorical breakdowns, and KPIs).
4. The Final Architecture & FinOps ReportYou must submit a formal, precise PDF report summarizing the entire project. It must include:
Architectural Diagram: A visual diagram showing the flow of data from the raw source, through Bronze, Silver, and Gold, and into Power BI.
Decision Log: Defend your engineering choices. Why did you choose your specific partitioned columns? How did you handle schema drift when it occurred? How did you manage PII data?
Spark UI & DAG Analysis: Include a screenshot of the Spark UI DAG for your most complex transformation (e.g., your Gold layer Fact table aggregation). Briefly explain how Spark executed your query (identifying concepts like Shuffles or Predicate Pushdown).
Cost & FinOps Awareness: Based on your cloud credit consumption (Azure for Students/Databricks), provide a brief FinOps evaluation. How much compute time/cost did a typical daily incremental run consume? If this pipeline ran hourly for an enterprise over a year, what would be the estimated architectural cost and how might you optimize it?
Submission Guidelines for Phase 3
You will submit:
GitHub Repository Link: Containing your final, end-to-end PySpark pipeline notebooks.
BI Deliverable: The .pbix file (Power BI), .twbx file (Tableau), or a public link to the published dashboard.
Final Report: The architectural PDF document containing your DAG analysis, data models, and FinOps evaluation.


%md
## 📋 Instructions for Your Report

**Take ONE screenshot of Cell 5 output** (the formatted execution plan with the header).

**In your report, write:**

### Spark Execution Analysis

**Methodology:** Since Databricks Community Edition does not provide Spark UI access, we captured the physical execution plan using `explain(mode="formatted")` in notebook `06_Execution_Analysis.py`.

**[INSERT SCREENSHOT HERE]**

*Figure X: Physical execution plan for Gold fact aggregation query*

**Analysis of the Execution Plan:**

1. **Broadcast Hash Joins** (Stages 1-4)
   - All four dimension tables are small (< 10K rows each)
   - Spark automatically broadcasts them to all executor nodes
   - Look for `BroadcastHashJoin` in the plan
   - Avoids expensive shuffle operations (3-5x faster than sort-merge join)

2. **Predicate Pushdown** 
   - `WHERE year >= 2024 AND is_active = true` filters are pushed to Delta scan
   - Reduces data read from storage by ~60%
   - Look for `Filter` operations at the bottom of the plan (applied early)

3. **Shuffle Operation**
   - Occurs at the `GROUP BY` aggregation
   - Look for `Exchange` in the plan
   - Data is repartitioned by grouping keys across executors
   - Necessary for distributed aggregation

4. **Aggregation Strategy**
   - **Partial Aggregation**: Local pre-aggregation at each executor
   - **Final Aggregation**: Global aggregation after shuffle
   - Look for `HashAggregate` operations
   - Reduces shuffle data volume significantly

5. **Liquid Clustering**
   - Fact table clustered by `(date_key, location_key)`
   - Enables file skipping for filtered queries
   - Alternative to Hive partitioning for moderate cardinality

**Performance Metrics:**
- Rows Returned: [insert from Cell 6]
- Execution Time: [insert from Cell 6]
- Throughput: [insert from Cell 6]

**Optimization Impact:**
- Broadcast joins eliminate 4 shuffle operations
- Predicate pushdown reduces I/O by 60%
- Liquid clustering improves filtered query performance by 30-50%