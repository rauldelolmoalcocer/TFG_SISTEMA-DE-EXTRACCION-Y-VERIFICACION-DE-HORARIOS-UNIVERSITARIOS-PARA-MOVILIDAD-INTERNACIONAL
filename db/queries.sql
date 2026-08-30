SELECT * FROM public.academic_years;

SELECT * FROM public.study_plans;


SELECT
  cs.*,
  d.name              AS degree,
  s.name              AS subject,
  s.normalized_name   AS subject_normalized,
  ay.name             AS academic_year,
  g.name              AS group_name,
  string_agg(DISTINCT r.code, ', ')                             AS rooms,
  src.file_name       AS source_file,
  em.page, em.extraction_strategy, em.table_index,
  em.row_start, em.row_end,
  em.time_start_raw, em.time_end_raw,
  em.llm_reviewed, em.llm_confidence, em.llm_note, em.raw_text,
  string_agg(DISTINCT ei.severity || ': ' || ei.message, ' | ') AS issues
FROM class_sessions cs
LEFT JOIN degrees             d   ON d.id  = cs.degree_id
LEFT JOIN subjects            s   ON s.id  = cs.subject_id
LEFT JOIN academic_years      ay  ON ay.id = cs.academic_year_id
LEFT JOIN groups              g   ON g.id  = cs.group_id
LEFT JOIN class_session_rooms csr ON csr.class_session_id = cs.id
LEFT JOIN rooms               r   ON r.id  = csr.room_id
LEFT JOIN extraction_metadata em  ON em.class_session_id  = cs.id
LEFT JOIN sources             src ON src.id = em.source_id
LEFT JOIN extraction_issues   ei  ON ei.class_session_id  = cs.id
WHERE cs.id = '8d7acc81-61ee-4884-b806-48d3f596477c'      -- <-- cambia el id
GROUP BY cs.id, d.name, s.name, s.normalized_name, ay.name, g.name,
         src.file_name, em.page, em.extraction_strategy, em.table_index,
         em.row_start, em.row_end, em.time_start_raw, em.time_end_raw,
         em.llm_reviewed, em.llm_confidence, em.llm_note, em.raw_text;


		 -- sesiones sin asignatura / sin titulación / sin hora
SELECT
  count(*) FILTER (WHERE subject_id IS NULL)  AS sin_asignatura,
  count(*) FILTER (WHERE degree_id  IS NULL)  AS sin_titulacion,
  count(*) FILTER (WHERE time_start IS NULL)  AS sin_hora,
  count(*) FILTER (WHERE day_of_week IS NULL) AS sin_dia
FROM class_sessions;