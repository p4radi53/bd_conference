from deployer.client.databricks.job_builder import JobBuilder, JobDirector
from deployer.client.databricks.spark_cluster_factory import DwhClientScope


class Pipeline(JobDirector):
    def pipeline(self) -> JobBuilder:
        cluster = self.spark_cluster_factory.single_node(dwh_user=DwhClientScope.MARKETING)

        return (
            self.job_builder.add_cluster(job_cluster_key="cluster", new_cluster=cluster)
            .add_task(
                task_key="easy_access_zone",
                description="Generates Easy Access Zone aggregated report",
                entry_point="easy_access_zone",
                job_cluster_key="cluster",
                timeout_seconds=30 * 60,
                max_retries=3,
                min_retry_interval_millis=60 * 60 * 1000,
                retry_on_timeout=True,
            )
            .set_schedule(quartz_cron_expression="0 0 12 1 * ? *", pause_status="UNPAUSED")
            .set_job_config(timeout_seconds=4 * 60 * 60, max_concurrent_runs=1)
        )
