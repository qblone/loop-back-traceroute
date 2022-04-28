```
loopback_traces AS 
(
   SELECT
      prb_id,
      src_addr,
      dst_addr,
      hops 
   FROM
      `prod - atlas - project.atlas_measurements.traceroute`,
      UNNEST (hops) AS hop 
   WHERE
      DATE(start_time) = "2022 - 04 - 20" 
      AND hop.hop_addr = '::' 
      AND dst_addr != '::' 
),
WITH ris_data AS
(
   SELECT
      * 
   FROM
      `ripencc - testing - env.ris.ip_matches` 
   WHERE
      DATE(day) = "2022 - 04 - 20" 
)

,
annotate_traceroute AS 
(
   SELECT
      * 
   except
(hopstemp),
   ARRAY_CONCAT( ARRAY( 
   SELECT
      AS STRUCT * 
   FROM
      t.hopstemp 
   UNION ALL
   SELECT
      STRUCT (hop.hop_addr, "NOT_MAPPED", hop.hop) 
   FROM
      t.hopsALL, UNNEST (t.hopsALL) AS hop 
   WHERE
      NOT hop.hop IN 
      (
         SELECT
(hop) 
         FROM
            t.hopstemp
      )
) ) AS hops 
   FROM
      (
         SELECT
            prb_id,
            src_addr,
            dst_addr,
            hops as hopsALL,
            ARRAY( 
            SELECT
               STRUCT (hopaddr.hop_addr, ris.origin, hop) 
            FROM
               UNNEST(hops) hopaddr 
               join
                  ris_data AS ris 
                  ON hop_addr = ris.ip ) as hopstemp 
            FROM
               loopback_traces
      )
      t
)
,
unique as 
(
   select
      * REPLACE( (
      SELECT
         ARRAY_AGG(STRUCT(hop_addr, origin, hop)) 
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