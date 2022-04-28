## Detecting::/128 in traceroute data 

In this tutorial, we explain the steps to build a SQL query to find the origin AS before or after  `::` in the traceroute dataset. 

#### Finding traceroutes with `::` in hops
We start by querying the traceroute table ( `prod - atlas - project.atlas_measurements.traceroute`) to find all the hops. It may be helpful to have a glance at the [traceroute schema table](https://github.com/RIPE-NCC/ripe-atlas-bigquery/blob/fea4b68f251bd4f72e482cfc3803aaa98de4abab/docs/measurements_traceroute.md)


```
WITH loopback_traces AS 
(
   SELECT
      prb_id,
      src_addr,
      dst_addr,
      hops 
   FROM
      `prod-atlas-project.atlas_measurements.traceroute`,
      UNNEST (hops) AS hop 
   WHERE
      DATE(start_time) = "2022-04-20" 
      AND hop.hop_addr = '::' 
      AND dst_addr != '::' 
),

ris_data AS
(
   SELECT
      * 
   FROM
      `ripencc-testing-env.ris.ip_matches` 
   WHERE
      DATE(day) = "2022-04-20" 
),

annotate_traceroute AS 
(
   SELECT
      * except (hopstemp),
   ARRAY_CONCAT(
         ARRAY( 

      -- the ones that got mepped
      SELECT
         AS STRUCT * 
      FROM
         t.hopstemp 
      
      -- union all with the ones that didn't get mapped
      UNION ALL
      SELECT
         STRUCT (hop.hop_addr, "NOT_MAPPED", hop.hop) --struct
      FROM
         t.hopsALL, UNNEST (t.hopsALL) AS hop 
      WHERE
         NOT hop.hop IN 
         (
            SELECT (hop) 
            FROM
               t.hopstemp
         )
   )) AS hops -- hoops that were both mapped and not mapped
   FROM
      (
         SELECT
            prb_id,
            src_addr,
            dst_addr,
            hops as hopsALL,
            ARRAY( 
               SELECT
                  STRUCT (hopaddr.hop_addr, ris.origin, hop) --struct
               FROM
                  UNNEST(hops) hopaddr 
                  join
                     ris_data AS ris 
                     ON hop_addr = ris.ip
            ) as hopstemp 

            FROM loopback_traces
      ) t
)
,

unique as 
(
   select
      * REPLACE( (
      SELECT
         ARRAY_AGG(STRUCT(hop_addr, origin, hop)) --struct
      FROM
         (
            SELECT DISTINCT
               hop_addr,
               origin,
               hop 
            FROM
               UNNEST(hops) c 
         )
) AS hops ) 
      FROM
         annotate_traceroute
)



-- the final query
select
   prb_id,
   src_addr,
   dst_addr,
   ARRAY(
   SELECT
      STRUCT (hop_addr, origin, hop) 
   FROM
      UNNEST(hops) AS hops 
   ORDER BY
      hop) AS hops 
   from
      unique
```
