package com.tmp.com.tmp;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.io.FileInputStream;
import java.util.UUID;


public class Mainclass {
  public static void main(String... args) throws Exception {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("mygcpproject-290602").setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("C:/Users/sannala.phani.t.teja/Documents/G-auth_keys/mykey.json"))).build().getService();
    		
    		
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder("SELECT unique_key  FROM `bigquery-public-data.austin_311.311_request` LIMIT 10")
              /*  "SELECT "
                    + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                    + "view_count "
                    + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                    + "WHERE tags like '%google-bigquery%' "
                    + "ORDER BY favorite_count DESC LIMIT 10")*/
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();

    // Print all pages of the results.
    for (FieldValueList row : result.iterateAll()) {
      String url = row.get("unique_key").getStringValue();
      System.out.printf("unique_key: %s%n", url);
      //long viewCount = row.get("view_count").getLongValue();
      //System.out.printf("unique_key: %s views: %d%n", url, viewCount);
    }
  }
}