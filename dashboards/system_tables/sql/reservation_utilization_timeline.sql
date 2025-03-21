/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- This table retrieves the latest slot capacity for each reservation
-- This table the slots being used at reservation, project, 
-- user and job level for each 1 min interval
with job_per_min as (
  SELECT
    reservation_id, project_id, user_email, job_id,
    TIMESTAMP_TRUNC(period_start, MINUTE) period_start_min,
    -- Average slot utilization per job per 1 min interval is calculated 
    -- by dividing total_slot_ms by the milliseconds on 1 min
    SUM(period_slot_ms) / (1000*60) period_job_slot,
    SUM(total_bytes_processed) period_job_bytes
  FROM 
    `region-{region_name}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
  WHERE
    job_creation_time	>= CURRENT_TIMESTAMP() - INTERVAL 100 DAY
    AND (statement_type != "SCRIPT" OR statement_type IS NULL)
    AND period_slot_ms > 0
  GROUP BY
    reservation_id, project_id, 
    job_id, user_email, period_start_min
)
SELECT 
  job_per_min.*,
  res.autoscale.max_slots autoscale_max_slots,
  res.autoscale.current_slots current_autoscale_active_slots,
  res.slots_assigned baseline_slots,
  res.autoscale.max_slots + res.slots_assigned total_available_slots
FROM 
  job_per_min
LEFT JOIN
  `{bq-admin-project}.region-{region_name}`.INFORMATION_SCHEMA.RESERVATIONS_TIMELINE res 
    ON
      job_per_min.reservation_id = res.reservation_id
      AND job_per_min.period_start_min = res.period_start