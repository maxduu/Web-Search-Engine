--- Contains basic queries needed for iterative full PageRank, we will eventually create a python script ---
--- Basic idea and flow of Python script here as well ---

--- Table: links(source, dest) ---

--- Finds number of outgoing links for each node ---
--- Note make this table a Materialized View (Nevermind MySQL doesn't have support for Mat Views) ---
--- Instead drop out_weights and recreate before every PageRank ---
--- View table will be called out_weights ---
--- Note that we must refresh the view before running PageRank ---

DROP TABLE out_weights;

CREATE TABLE out_weights AS
  SELECT source, 1 / COUNT(*) AS out_weight
  FROM links
  GROUP BY source;

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

