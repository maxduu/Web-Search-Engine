--- Contains basic queries needed for iterative full PageRank, we will eventually create a python script ---
--- Basic idea and flow of Python script here as well ---

--- Table: links(sourceID, destID) ---

--- Finds number of outgoing links for each node ---
--- Note make this table a Materialized View ---
--- Instead drop out_weights and recreate before every PageRank ---
--- View table will be called out_weights ---
--- Note that we must refresh the view before running PageRank ---

CREATE MATERIALIZED VIEW cleaned_links_url AS
  WITH no_self_loops AS (
    SELECT links_url.source, links_url.dest
    FROM links_url
    WHERE links_url.source::text <> links_url.dest::text
  ), sinks AS (
    SELECT no_self_loops.dest AS sink
    FROM no_self_loops
    WHERE NOT (no_self_loops.dest::text IN ( 
      SELECT DISTINCT no_self_loops_1.source
      FROM no_self_loops no_self_loops_1)
    )
  ), back_edges AS (
    SELECT n.dest AS source, n.source AS dest
    FROM no_self_loops n
    JOIN sinks s ON n.dest::text = s.sink::text
  )
  SELECT back_edges.source, back_edges.dest
  FROM back_edges
  UNION
  SELECT no_self_loops.source, no_self_loops.dest
  FROM no_self_loops;

CREATE MATERIALIZED VIEW out_weights AS
  SELECT cleaned_links_url.source, 1.0 / count(*)::numeric AS out_weight
  FROM cleaned_links_url
  GROUP BY cleaned_links_url.source;

REFRESH MATERIALIZED VIEW out_weights;

--- Assume in python script there is a variable called: prev_iter_ranks ---
--- prev_iter_ranks (node, rank) ---
--- prev_iter_ranks contains the ranks per node of the previous iteration ---
--- prev_iter_ranks is initialized to 1 for each distinct node ---

--- The query below will be called curr_iter_ranks ---
WITH distrib AS (
  SELECT l.source AS source, l.dest AS dest, out_weight * rank AS distrib_rank
  FROM link l JOIN out_weight o ON l.source = o.source JOIN prev_iter_weights p ON l.source = p.node
)
SELECT dest AS node, SUM(distrib_rank) * 0.85 + 0.15 AS rank
FROM distrib
GROUP BY dest;

--- Query gets the max diff rank b/n curr iter and prev in order for python code to check MAX <= convergence value ---
SELECT MAX(ABS(p.rank - c.rank))
FROM prev_iter_ranks p JOIN curr_iter_ranks c ON p.node = c.node;

