import sys
from datetime import date

from dacite.core import from_dict
from pyspark.sql import functions as f
from spark_common.app.logged_app import LoggedApp
from spark_common.jdbc.jdbc_reader import JdbcReader
from spark_common.tables.db_writer import DbWriter

from python_reports.easy_access_zone.config import Config
from python_reports.easy_access_zone.queries import dwh_query_easy_access_zone


def entrypoint():
    EasyAccessZone(sys.argv[1:]).main()


class EasyAccessZone(LoggedApp):
    def run(self):
        conf = from_dict(Config, self.conf_dict)
        jdbc_reader = JdbcReader(conf.databases.dwh, self.spark, self.secrets)
        db_writer = DbWriter(self.spark, self.logger, self.job_arguments)
        ingest_start_date = date(2021, 8, 1)  # start of easy access zone campaign

        eaz_parcels = (
            jdbc_reader.query(dwh_query_easy_access_zone(ingest_start_date))
            .withColumn("report_date", f.to_date(f.col("deliveryAPMDateFK").cast("string"), "yyyyMMdd"))
            .cache()
        )
        eaz_users_total = eaz_parcels.select("recipientId").dropDuplicates().count()
        eaz_users_monthly = (
            eaz_parcels.withColumn("report_year_month", f.date_trunc("mm", f.col("report_date")))
            .groupBy("report_year_month")
            .agg(
                f.countDistinct("recipientId").alias("eaz_users_monthly"),
            )
        )
        eaz_users_daily = eaz_parcels.groupBy("report_date").agg(
            f.sum("isRequestedToEasyAccessInd").alias("eaz_requests"),
            f.sum("isStoredInEasyAccessInd").alias("eaz_stored"),
            f.countDistinct("recipientId").alias("eaz_users"),
        )

        result = (
            eaz_users_daily.join(
                eaz_users_monthly, f.date_trunc("mm", eaz_users_daily.report_date) == eaz_users_monthly.report_year_month, how="left"
            )
            .drop("report_year_month")
            .withColumn("eaz_users_total", f.lit(eaz_users_total))
            .coalesce(1)
        )
        db_writer.overwrite_table(result, conf.tables.easy_access_zone)
