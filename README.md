## Detecting::/128 in traceroute data 

In this tutorial, we explain the steps to build a SQL query to find the origin AS before or after  `::` in the traceroute dataset. 

#### 1. Finding traceroutes with `::` in hops
We start by querying the traceroute table ( `prod - atlas - project.atlas_measurements.traceroute`) to find all the hops. It may be helpful to have a glance at the [traceroute schema table](https://github.com/RIPE-NCC/ripe-atlas-bigquery/blob/fea4b68f251bd4f72e482cfc3803aaa98de4abab/docs/measurements_traceroute.md)

```sql
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
)
```
Note that we have to unnest the hops since they are NESTED schema to filter results on the hops



#### 2. Fetching RIS table to map to origin AS
Next, we get the data from the RIS table for the desired date. The RIS table contains the longest prefix match for IP addresses to the origin ASN.

```sql
ris_data AS
(
   SELECT
      * 
   FROM
      `ripencc-testing-env.ris.ip_matches` 
   WHERE
      DATE(day) = "2022-04-20" 
)
```

#### 3. Mapping RIS data to IP addresses in the dataset
In the third step, we map IP addresses to their origin AS. This step is a bit complicated since, as an initial step, we need to map the IP addresses of hops that are available in the RIS table. We then need to merge it with the hops that had no match in the RIS table. There can be several reasons why data is unavailable in the RIS table, including missing IP information due to timeout at the hop, private IP addresses space, or IP space that is not advertised by AS in BGP. 

```sql
annotate_traceroute AS 
(
   SELECT --(2)
      * except (hopstemp),
   ARRAY_CONCAT(
         ARRAY( 

      -- the ones that got mapped
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
   FROM --(1)
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
```
To understand this part of the query, start with the syntax after `from` marked as `(1)`. In this part, we fetch all the IP to origin AS pairs mapped in the RIS table. In the second part,marked as `(2)`, we merge all the hops that had missing origin AS information. 

#### Get unique pairs






```sql
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
