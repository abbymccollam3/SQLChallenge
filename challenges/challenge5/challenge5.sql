SELECT ngram, sum(match_count) AS total_match_count FROM hue_tmp_ngram_dataset
GROUP BY ngram ORDER BY total_match_count DESC LIMIT 1;