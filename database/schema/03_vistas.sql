-- =========================================================
-- HRC-CEMS | Vistas
--
-- Lo que el modelo define como calculado y no almacenado:
-- el universo visible, el tipo de plan y el cumplimiento.
-- =========================================================


-- ---------------------------------------------------------
-- equipo_vigente
--
-- Un equipo se muestra en la web si no esta de baja y tiene
-- plan del anio en curso o de alguno de los dos anteriores.
-- El plan se mueve mes a mes, asi que esto se resuelve al
-- consultar y no como un campo que habria que mantener.
-- ---------------------------------------------------------

CREATE VIEW equipo_vigente AS
SELECT
    e.*,
    EXISTS (
        SELECT 1 FROM plan_mantenimiento p
        WHERE p.equipo_id = e.id_equipo
          AND p.anio = date_part('year', current_date)::smallint
    ) AS en_plan_vigente
FROM equipo e
WHERE e.fecha_baja IS NULL
  AND EXISTS (
        SELECT 1 FROM plan_mantenimiento p
        WHERE p.equipo_id = e.id_equipo
          AND p.anio >= date_part('year', current_date)::smallint - 2
  );

COMMENT ON VIEW equipo_vigente IS
    'Universo visible en la web: con plan del anio en curso o de los dos anteriores, y sin baja.';


-- ---------------------------------------------------------
-- plan_cumplimiento
--
-- Agrega al plan las dos cosas que el modelo deduce:
--   tipo_plan  -> sale de la categoria del tipo de equipo
--   en_ventana -> ejecucion dentro del mes programado +- 1
--
-- en_ventana queda nula cuando no hay fecha de ejecucion,
-- que es el caso del plan de IM.
-- ---------------------------------------------------------

CREATE VIEW plan_cumplimiento AS
SELECT
    p.*,
    e.serie,
    t.nombre    AS tipo_equipo,
    t.categoria,
    CASE
        WHEN t.categoria = 'IM_MAYOR_12' THEN 'IM'
        ELSE 'ACREDITACION'
    END AS tipo_plan,
    CASE
        WHEN p.fecha_ejecucion IS NULL OR p.mes_programado IS NULL THEN NULL
        ELSE abs(
                 (date_part('year',  p.fecha_ejecucion)::integer - p.anio) * 12
               + (date_part('month', p.fecha_ejecucion)::integer - p.mes_programado)
             ) <= 1
    END AS en_ventana
FROM plan_mantenimiento p
JOIN equipo      e ON e.id_equipo      = p.equipo_id
JOIN tipo_equipo t ON t.id_tipo_equipo = e.tipo_equipo_id;

COMMENT ON VIEW plan_cumplimiento IS
    'Plan de mantenimiento con el tipo de plan y el cumplimiento de la ventana FP +- 1 mes.';
