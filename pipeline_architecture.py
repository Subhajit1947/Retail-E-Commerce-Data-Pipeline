from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import S3
from diagrams.aws.analytics import EMRCluster, Quicksight
from diagrams.onprem.analytics import Spark
from diagrams.onprem.database import PostgreSQL

# Graph attributes for better layout and spacing
graph_attr = {
    "fontsize": "20",
    "pad": "1.0",
    "nodesep": "0.8",
    "ranksep": "1.0"
}

with Diagram("Data Lakehouse Pipeline: S3 to QuickSight", show=False, direction="LR", graph_attr=graph_attr):
    
    airflow = Airflow("Airflow DAG\n(Orchestrator)")

    with Cluster("1. S3 Data Lake"):
        bronze_s3 = S3("Bronze Layer\n(Raw Data)")
        silver_s3 = S3("Silver Layer\n(Parquet)")
        
    with Cluster("2. Amazon EMR (Spark)"):
        create_emr = EMRCluster("Spin up EMR")
        
        silver_spark = Spark("Silver Jobs\n(Transform to Parquet)")
        gold_spark = Spark("Gold Job\n(Load to DB)")
        
        terminate_emr = EMRCluster("Terminate EMR")

    with Cluster("3. PostgreSQL Data Warehouse (EC2)"):
        with Cluster("Staging Layer"):
            stage_dims = PostgreSQL("Staging Dims\n(Customer & Product)")
            
        with Cluster("Core & Presentation Layer"):
            fact_table = PostgreSQL("Fact Table")
            target_dims = PostgreSQL("Target Dims\n(Customer & Product)")
            mat_view = PostgreSQL("Materialized View")
            
        sp_merge = PostgreSQL("Execute Merge\nProcedures")
        sp_refresh = PostgreSQL("Refresh\nMaterialized View")

    with Cluster("4. BI & Analytics"):
        quicksight = Quicksight("Amazon QuickSight")


    # ==========================================
    # ORCHESTRATION FLOW (Solid Black Lines)
    # ==========================================
    airflow >> Edge(label="1. Start") >> create_emr
    create_emr >> Edge(label="2. Run Silver") >> silver_spark
    silver_spark >> Edge(label="3. Run Gold") >> gold_spark
    gold_spark >> Edge(label="4. Terminate") >> terminate_emr
    terminate_emr >> Edge(label="5. Trigger Merge") >> sp_merge
    sp_merge >> Edge(label="6. Trigger Refresh") >> sp_refresh

    # ==========================================
    # DATA FLOW (Dashed Colored Lines)
    # ==========================================
    
    # Bronze to Silver Flow
    bronze_s3 >> Edge(label="Reads Raw", color="blue", style="dashed") >> silver_spark
    silver_spark >> Edge(label="Writes Parquet", color="blue", style="dashed") >> silver_s3
    
    # Silver to Postgres Flow
    silver_s3 >> Edge(label="Reads Parquet", color="darkorange", style="dashed") >> gold_spark
    gold_spark >> Edge(label="Writes Staging", color="darkorange", style="dashed") >> stage_dims
    gold_spark >> Edge(label="Writes Facts", color="darkorange", style="dashed") >> fact_table
    
    # Postgres Internal Flow (Merges & Refreshes)
    stage_dims - Edge(style="dotted", color="gray") - sp_merge 
    sp_merge >> Edge(label="Merge to Target", color="darkgreen", style="dashed") >> target_dims
    
    target_dims - Edge(style="dotted", color="gray") - sp_refresh
    fact_table - Edge(style="dotted", color="gray") - sp_refresh
    sp_refresh >> Edge(label="Updates", color="darkgreen", style="dashed") >> mat_view
    
    # BI Flow
    mat_view >> Edge(label="Direct Query / SPICE", color="purple", style="dashed", penwidth="2.5") >> quicksight